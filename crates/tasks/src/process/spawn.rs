use std::path::PathBuf;

use tokio::process::Command;
use tracing::{debug, error};

use super::child::Running;
use super::instance::{self, CommandInstance, SpawnReservation};
use super::{CommandSpec, RestartPolicy, logger};
use crate::{Error, Reason, TaskResult, TaskRuntime};

pub(super) async fn start(
    spec: &CommandSpec,
    runtime: TaskRuntime,
    instance: Option<CommandInstance>,
) -> TaskResult<Running> {
    match instance {
        Some(CommandInstance::Attach(attached)) => {
            debug!(
                task = runtime.task_name,
                slot = runtime.task_worker_number,
                pid = attached.pid(),
                "attached to existing command instance"
            );
            Ok(Running::Attached(attached))
        }
        Some(CommandInstance::Spawn(reservation)) => {
            spawn(spec, runtime, Some(reservation)).await
        }
        None => spawn(spec, runtime, None).await,
    }
}

/// Resolve a configured `PATH` against the supervisor's own environment.
///
/// Segments are expanded independently and a segment that names a variable
/// which is not set is **dropped**, not half-resolved: `$HOME/.local/bin`
/// with no `HOME` would otherwise become `/.local/bin`, an absolute path to
/// somewhere nobody meant. Empty segments go with it, so a stray `::` cannot
/// smuggle the current directory onto the search path of a supervised
/// process.
///
/// A value written in `tasks.yaml` has already been expanded once by the
/// config loader; running it through again is harmless, because what came
/// back no longer contains a reference. The built-in default has not been
/// expanded at all — a serde default never passes through the loader — which
/// is the reason this happens here rather than at load time.
fn expand_path(raw: &str) -> String {
    raw.split(':')
        .filter_map(expand_segment)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join(":")
}

/// One `PATH` segment with `$VAR` / `${VAR}` replaced, or `None` when it
/// names a variable this process does not have.
fn expand_segment(segment: &str) -> Option<String> {
    let mut out = String::with_capacity(segment.len());
    let mut rest = segment;

    while let Some(at) = rest.find('$') {
        out.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        let (name, tail) = match after.strip_prefix('{') {
            Some(body) => match body.find('}') {
                Some(end) => (&body[..end], &body[end + 1..]),
                // An unclosed `${` is not a reference; keep it verbatim.
                None => {
                    out.push_str(&rest[at..]);
                    return Some(out);
                }
            },
            None => {
                let end = after
                    .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                    .unwrap_or(after.len());
                (&after[..end], &after[end..])
            }
        };

        if name.is_empty() {
            // A bare `$` is a literal dollar, not an empty variable name.
            out.push('$');
        } else {
            out.push_str(&std::env::var(name).ok()?);
        }
        rest = tail;
    }

    out.push_str(rest);
    Some(out)
}

async fn spawn(
    spec: &CommandSpec,
    runtime: TaskRuntime,
    reservation: Option<SpawnReservation>,
) -> TaskResult<Running> {
    let mut command = Command::new(&spec.program);
    if !spec.args.is_empty() {
        command.args(&spec.args);
    }
    if let Some(cwd) = &spec.cwd {
        command.current_dir(PathBuf::from(cwd));
    }

    // A command receives only the explicit allowlist and Claviron task
    // context. Never leak the supervisor's complete environment.
    command.env_clear();

    if !spec.env.contains_key("PATH") {
        command.env("PATH", expand_path(&spec.default_path));
    }
    if let Some(cwd) = &spec.cwd {
        command.env("PWD", cwd);
    }
    if !spec.env.contains_key("LANG") && !spec.env.contains_key("LC_ALL") {
        command.env("LANG", "C.UTF-8");
    }
    if !spec.env.is_empty() {
        command.envs(&spec.env);
    }

    for key in [
        "CLAVIRON_TASK_NAME",
        "CLAVIRON_TASK_WORKER_NUMBER",
        "CLAVIRON_TASK_SLOT",
        "CLAVIRON_TASK_ATTEMPT",
        "CLAVIRON_TASK_SCOPE",
        "CLAVIRON_TASK_DISPATCH",
        "CLAVIRON_RUN_MODE",
        "CLAVIRON_WORKER_NUMBER",
        "CLAVIRON_WORKER_COUNT",
        "CLAVIRON_PROCESS_ID",
    ] {
        command.env_remove(key);
    }

    command.envs(runtime.identity_env());

    command.kill_on_drop(false);
    logger::configure(&mut command, spec);

    let instance_lock_fd = reservation.as_ref().map(SpawnReservation::raw_fd);
    #[cfg(target_os = "linux")]
    let terminate_with_owner = spec.exit_action.is_terminating();

    // The instance lock survives exec. Managed children additionally use
    // Linux's parent-death signal so an abrupt owner death cannot orphan them.
    unsafe {
        #[cfg(target_os = "linux")]
        let expected_parent = nix::libc::getpid();
        command.pre_exec(move || {
            if let Some(raw_fd) = instance_lock_fd {
                instance::inherit_lock(raw_fd)?;
            }

            #[cfg(target_os = "linux")]
            if terminate_with_owner {
                if nix::libc::prctl(
                    nix::libc::PR_SET_PDEATHSIG,
                    nix::libc::SIGKILL,
                ) == -1
                {
                    return Err(std::io::Error::last_os_error());
                }
                if nix::libc::getppid() != expected_parent {
                    nix::libc::_exit(1);
                }
            }
            Ok(())
        });
    }

    let mut child = command
        .spawn()
        .map_err(|error| classify_start_failure(spec, runtime, error))?;
    logger::attach(&mut child, spec, runtime.task_name);
    let pid = child.id().ok_or_else(|| {
        classify_start_failure(
            spec,
            runtime,
            std::io::Error::other("spawned command has no PID"),
        )
    })?;
    if let Some(reservation) = reservation
        && let Err(error) = reservation.publish(pid)
    {
        let _ = child.start_kill();
        let _ = child.wait().await;
        return Err(Error::Io(error));
    }
    Ok(Running::Spawned(child))
}

pub(super) fn classify_start_failure(
    spec: &CommandSpec,
    runtime: TaskRuntime,
    error: std::io::Error,
) -> Error {
    match spec.restart {
        RestartPolicy::Never => {
            error!(
                task = runtime.task_name,
                error = %error,
                "process failed to start; restart policy is never"
            );
            Error::Exit(Reason::Custom("process failed to start"))
        }
        RestartPolicy::OnFailure | RestartPolicy::Always => {
            Error::other(format!(
                "process '{}' failed to start: {}",
                runtime.task_name, error
            ))
        }
    }
}

#[cfg(test)]
#[path = "../../tests/process/spawn.rs"]
mod path_tests;
