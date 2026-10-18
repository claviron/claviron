use std::process::ExitStatus;
use std::time::Duration;

use claviron_runtime::AppState;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use self::child::{Action, Completion};
use self::spawn::start;
pub use self::spec::RestartPolicy;
pub(crate) use self::spec::{
    CommandSpec, ExitAction, ProcessSignal, ReloadAction,
};
use crate::environment::ExecutionEnvironment;
use crate::runner::{StopCause, StopContext, TaskRuntime};
use crate::{Error, Reason, TaskResult};

mod child;
mod instance;
pub(crate) mod presence;
pub use presence::{
    ProcessOwner, TaskProcessControl, TaskProcessIssue, TaskProcessLiveness,
    TaskProcessPhase, TaskProcessSnapshot, TaskProcessStatus,
};
mod logger;
mod spawn;
mod spec;

#[cfg(test)]
#[path = "../tests/process/tests.rs"]
mod tests;

/// Spawn an external process under task supervision.
///
/// Used by tasks defined with a `command:` block in `tasks.yaml` (PHP
/// `artisan schedule:run`, queue workers, sidecar daemons, …). The
/// runner treats this exactly like a Rust task: same retry/backoff,
/// same cron/interval scheduling, same graceful-shutdown contract.
///
/// **Environment hygiene** — `env_clear()` is called first so the
/// child does **not** inherit the supervisor's full environment.
/// Only an explicit allowlist plus the task-context variables below
/// are passed in. Adds `LANG=C.UTF-8` for predictable text handling,
/// and a `PATH` when the operator hasn't set one — the service-level
/// `command.path` from `tasks.yaml`, whose default covers the system
/// locations plus `$HOME/.local/bin` (and Homebrew's prefix on macOS).
/// `$VAR` in it is expanded against the supervisor's environment as the
/// child is spawned, and a segment naming an unset variable is dropped
/// rather than left half-resolved. `command.env_file` is retained separately
/// from the inline allowlist; configuration loading does not read its contents.
///
/// **Reserved env vars** injected per attempt (operator config can't
/// override these — they're removed and re-set in order):
/// `CLAVIRON_TASK_NAME`, `CLAVIRON_TASK_SLOT`,
/// `CLAVIRON_TASK_ATTEMPT`, `CLAVIRON_TASK_SCOPE`, `CLAVIRON_TASK_DISPATCH`,
/// `CLAVIRON_RUN_MODE`, `CLAVIRON_PROCESS_ID`, plus
/// `CLAVIRON_WORKER_NUMBER` (worker only) or `CLAVIRON_WORKER_COUNT`
/// (master only). Children can self-identify without any handshake.
///
/// **Cancellation contract:** shutdown selects `exit_action`. An explicit
/// reload `restart` uses the same SIGTERM, `graceful_timeout`, SIGKILL, and reap
/// ladder. An unchanged daemon's reload signal (except KILL) is delivered in
/// place, keeping supervision and pool ownership. KILL stops and reaps before
/// replacement. Omitted `reload_action` means that the incremental reload
/// planner leaves an unchanged command alone. Persistent daemon instances carry
/// an inherited kernel lock so the next Claviron incarnation attaches instead
/// of spawning a duplicate. Execution timeout is a hard boundary: it sends
/// SIGKILL and reaps the child regardless of `exit_action`.
///
/// **Exit semantics:**
///   - Owner cancellation applies the action for its stop cause and returns
///     terminal `Reason::Shutdown`; it never enters restart policy.
///   - `restart: never` makes every natural completion terminal.
///   - `restart: on-failure` retries start failures, non-zero exits, and
///     external signals; exit code zero completes successfully.
///   - `restart: always` retries every natural completion and is valid with
///     daemon or manual timing.
pub(crate) async fn run_command(
    state: AppState,
    token: CancellationToken,
    stop: StopContext,
    spec: &CommandSpec,
    runtime: TaskRuntime,
    graceful_timeout: Option<Duration>,
    persistent_instance: bool,
) -> TaskResult<()> {
    let mut environment = ExecutionEnvironment::prepare(
        &state,
        spec.env_file.as_deref(),
        &spec.env,
        persistent_instance,
    )?;
    let mut spec = spec.clone();
    spec.env = environment.values().clone();
    // The command's inherited lock excludes duplicate processes; this separate
    // tenure excludes duplicate controllers (including two attach callers).
    let controller = if persistent_instance {
        Some(
            presence::Controller::acquire(&state, runtime)
                .await
                .map_err(Error::Io)?,
        )
    } else {
        None
    };
    let instance = if persistent_instance {
        Some(
            instance::resolve(&state, runtime, &spec)
                .await
                .map_err(Error::Io)?,
        )
    } else {
        None
    };

    let mut running = start(&spec, runtime, instance).await?;

    loop {
        publish_presence(
            &controller,
            &running,
            TaskProcessPhase::Running,
            runtime,
        );
        let mut reload_signals = stop.subscribe_reload_signals();
        // Register the live PID so the RSS sampler can inspect both newly
        // spawned and reattached command processes.
        #[cfg(feature = "metrics")]
        let _pid_guard = {
            crate::metrics::register_command_pid(
                runtime.task_name,
                runtime.task_worker_number,
                running.pid(),
            );
            PidRegistrationGuard {
                task: runtime.task_name,
                slot: runtime.task_worker_number,
            }
        };

        let completion = loop {
            tokio::select! {
                biased;
                _ = token.cancelled() => {
                    let action = match stop.cause() {
                        StopCause::Shutdown => Action::from(spec.exit_action),
                        // Structural transitions provide a restart override;
                        // in-place signals never cancel this generation.
                        StopCause::Reload => {
                            Action::from(stop.reload_action().or(spec.reload_action))
                        },
                        StopCause::ExecutionTimeout => Action::Signal(ProcessSignal::Kill)
                    };
                    if matches!(action, Action::Terminate | Action::Signal(ProcessSignal::Kill)) {
                        publish_presence(&controller, &running, TaskProcessPhase::Stopping, runtime);
                    }
                    running.apply(action, graceful_timeout).await?;
                    let reason = match stop.cause() {
                        StopCause::ExecutionTimeout => Reason::ExecutionTimeout,
                        StopCause::Shutdown | StopCause::Reload => Reason::Shutdown
                    };
                    return Err(Error::stopped(reason));
                }
                result = running.wait() => break Some(result.map_err(Error::Io)?),
                result = environment.notified() => match result {
                    Ok(()) => {
                        publish_presence(&controller, &running, TaskProcessPhase::Stopping, runtime);
                        // Env cannot be changed in a live process. Never detach
                        // here, even when exit_action permits it on shutdown.
                        running.apply(Action::Terminate, graceful_timeout).await?;
                        break None;
                    }
                    Err(error) => warn!(task = runtime.task_name, slot = runtime.slot(), %error,
                        "task environment replacement rejected; keeping command supervision"),
                },
                Ok(()) = reload_signals.changed() => {
                    let signal = *reload_signals.borrow_and_update();
                    let Some(signal) = signal else { continue };
                    match running.apply(Action::Signal(signal), graceful_timeout).await {
                        Ok(()) => debug!(
                            task = runtime.task_name,
                            slot = runtime.slot(),
                            ?signal,
                            "sent reload signal; keeping command supervision"
                        ),
                        // A failed notification must not detach a live child
                        // or relinquish its pool slot.
                        Err(error) => warn!(
                            task = runtime.task_name,
                            slot = runtime.slot(),
                            ?signal,
                            %error,
                            "failed to send reload signal; keeping command supervision"
                        )
                    }
                }
            }
        };

        #[cfg(feature = "metrics")]
        drop(_pid_guard);

        match completion {
            Some(Completion::Spawned(status)) => {
                return classify_exit(&spec, runtime, status);
            }
            None => {
                if token.is_cancelled() {
                    return Err(Error::stopped(Reason::Shutdown));
                }
                spec.env = environment.values().clone();
                let next = instance::resolve(&state, runtime, &spec)
                    .await
                    .map_err(Error::Io)?;
                if token.is_cancelled() {
                    return Err(Error::stopped(Reason::Shutdown));
                }
                debug!(
                    task = runtime.task_name,
                    slot = runtime.slot(),
                    "task environment changed; replacing drained command in the same slot"
                );
                running = start(&spec, runtime, Some(next)).await?;
            }
            Some(Completion::Attached) => {
                if spec.restart == RestartPolicy::Never {
                    return Err(Error::Exit(Reason::Custom(
                        "attached process exited",
                    )));
                }
                // An attached daemon belongs to the current supervision
                // session. If that inherited process exits, fill the same
                // logical instance immediately rather than spending a task
                // retry merely crossing from Attached to Spawned.
                if token.is_cancelled() {
                    return Err(Error::stopped(Reason::Shutdown));
                }
                let next = instance::resolve(&state, runtime, &spec)
                    .await
                    .map_err(Error::Io)?;
                if token.is_cancelled() {
                    return Err(Error::stopped(Reason::Shutdown));
                }
                running = start(&spec, runtime, Some(next)).await?;
            }
        }
    }
}

fn publish_presence(
    controller: &Option<presence::Controller>,
    running: &child::Running,
    phase: TaskProcessPhase,
    runtime: TaskRuntime,
) {
    if let Some(controller) = controller {
        let result = running.identity().and_then(|process| {
            controller.publish(
                process,
                matches!(running, child::Running::Attached(_)),
                phase,
            )
        });
        // Observational I/O failure must never drop supervision of a live child.
        if let Err(error) = result {
            warn!(task = runtime.task_name, slot = runtime.slot(), %error,
                "command presence publication failed; retaining process supervision");
        }
    }
}

fn classify_exit(
    spec: &CommandSpec,
    runtime: TaskRuntime,
    status: ExitStatus,
) -> TaskResult<()> {
    let failed = !status.success();
    match spec.restart {
        RestartPolicy::Never if failed => {
            error!(
                task = runtime.task_name,
                status = %status,
                "process exited; restart policy is never"
            );
            Err(Error::Exit(Reason::Custom("process exited")))
        }
        RestartPolicy::Never | RestartPolicy::OnFailure if !failed => Ok(()),
        RestartPolicy::OnFailure | RestartPolicy::Always => {
            Err(Error::other(format!(
                "process '{}' exited with status: {}",
                runtime.task_name, status
            )))
        }
        RestartPolicy::Never => {
            unreachable!("failed restart-never exit handled above")
        }
    }
}

/// RAII guard ensuring the (task, slot) entry is removed from the
/// shared PID registry on every exit path of `run_command`. Without
/// this, an early `?`-return on I/O error would leave a stale PID
/// in the map, which the RSS sampler would still try to read from
/// after `/proc/<pid>` is gone.
#[cfg(feature = "metrics")]
struct PidRegistrationGuard {
    task: &'static str,
    slot: u8,
}

#[cfg(feature = "metrics")]
impl Drop for PidRegistrationGuard {
    fn drop(&mut self) {
        crate::metrics::unregister_command_pid(self.task, self.slot);
    }
}
