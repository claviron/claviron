//! Configuration and execution for the task kinds an engine provides.
//!
//! Deliberately shaped as `CommandSpec` minus the two fields that need a
//! separate process — `exit_action` (a thread is not a process to leave
//! running) and `reload_action` (a thread takes no signal). Console output is
//! executor-independent policy: an embedded engine routes it through its own
//! task-local writer instead of child-process `Stdio`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use claviron_helpers::sync::Unpoisoned;
use claviron_runtime::AppState;
use futures_util::future::BoxFuture;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::TaskResult;
use crate::config::{TaskExecutor, TaskExecutorKind};
use crate::environment::{ExecutionEnvironment, drain_for_environment};
use crate::output::LogConfig;
use crate::process::RestartPolicy;
use crate::runner::{StopContext, TaskRunner, TaskRuntime};
use crate::service::{effective_dispatch, effective_scope};

#[derive(Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScriptSpec {
    /// The script to run, resolved against `cwd`.
    ///
    /// Never an interpreter. `command` needs `program` because it spawns
    /// something; this task *is* the interpreter, so there is no field in which
    /// a second PHP could be named — and therefore no way for a scheduled task
    /// to drift onto a different build, ini set or extension list from the one
    /// serving requests. `artisan`, not `php artisan`.
    pub script: String,

    #[serde(default)]
    pub args: Vec<String>,

    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Environment source declaration, retained without reading the file.
    /// The execution owner resolves it with inline `env:` values taking
    /// precedence; the task config snapshot never stores the merged result.
    pub env_file: Option<PathBuf>,

    /// Where this task runs, and what a relative `script` is resolved against
    /// — the same meaning as a command task's `cwd`.
    ///
    /// A PHP thread can have one of its own. Under ZTS, PHP's `chdir()` writes
    /// the thread-local virtual CWD rather than the process's
    /// (`zend_virtual_cwd.h`: ZTS defines `VIRTUAL_DIR`, so `VCWD_CHDIR` is
    /// `virtual_chdir` and `CWDG` reaches TSRM), so two tasks can sit in two
    /// directories and neither moves the server.
    #[serde(default, alias = "dir")]
    pub cwd: Option<String>,

    /// Task stdout/stderr routing. Relative paths resolve against `cwd`.
    #[serde(default)]
    pub log: LogConfig,

    /// Which natural completions enter the task runner's retry loop — the same
    /// policy, and the same three values, as a command task's.
    #[serde(default)]
    pub restart: RestartPolicy,
}

/// One instance of a hosted-language task, handed to its engine owner.
///
/// This crate decides *when* and *where* a PHP task runs — trigger, scope,
/// dispatch, attempts, backoff — and never learns *how*. The how is supplied
/// by `claviron-php-embed` through [`Executor`], which is the whole of the
/// coupling between the two crates.
pub struct TaskExecution {
    pub task: String,
    /// Execution-time environment is already evaluated. The raw recipe stays
    /// in TaskService; the engine does not reopen `env_file`.
    pub spec: Arc<ScriptSpec>,
    pub state: AppState,
    pub cancellation: CancellationToken,
    pub stop: StopContext,
    pub runtime: TaskRuntime,
    /// The task's `graceful_timeout`. `None` waits indefinitely and never
    /// interrupts — `GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION`.
    pub graceful_timeout: Option<Duration>,
    /// Daemon timing on `each` or `pool` dispatch: one long-lived program that
    /// owns its slot, rather than a bounded occurrence that ends.
    pub persistent: bool,
}

/// Runs one [`TaskExecution`]. Installed once by the crate that owns the
/// embedded engine; see [`TaskService::set_php_executor`].
///
/// A return after cancellation must mean owned execution has drained/joined.
/// Environment renewal cancels only this instance and awaits that return
/// before calling the executor again in the same slot/attempt. `Ok(())` or
/// `Exit(Cancelled | Shutdown)` acknowledges the stop; other errors refuse
/// replacement and enter the runner's existing failure policy.
///
/// [`TaskService::set_php_executor`]: crate::service::TaskService::set_php_executor
pub type Executor = Arc<
    dyn Fn(TaskExecution) -> BoxFuture<'static, TaskResult<()>> + Send + Sync,
>;

impl crate::service::TaskService {
    /// Install the engine that runs one executor kind.
    ///
    /// Called once by the crate that owns that engine — `claviron-php-embed`
    /// for `php:`, `claviron-js` for `js:`. This crate keeps the schedule, the
    /// scope, the dispatch, the attempt budget and the drain; the executor is
    /// handed one occurrence at a time and knows nothing about any of them.
    ///
    /// Keyed rather than named per engine, because the second one is where a
    /// `set_js_executor` beside a `set_php_executor` would have become the
    /// third implementation of the same idea. A fourth runtime installs itself
    /// with the same call.
    ///
    /// Without an engine for its kind a task is still configured, validated,
    /// listed and refused by name — it simply has no runner, which is what a
    /// build without that engine should say.
    ///
    /// Installing also registers, so neither side has to boot first: whichever
    /// of the two arrives last does the work. A table loaded before the engine
    /// is registered here; an engine installed before the table is registered
    /// again when `boot` or `reload` loads it.
    pub fn set_executor(
        &self,
        kind: TaskExecutorKind,
        executor: Executor,
    ) -> &Self {
        self.inner.executors.write_unpoisoned().insert(kind, executor);
        self.register_engine_tasks()
    }

    /// Materialize a runner for every task whose kind has an engine, the way
    /// [`Self::register_commands`] does for `command:` tasks. Replaces existing
    /// entries, so a reload re-materializes against the new configuration.
    pub fn register_engine_tasks(&self) -> &Self {
        let executors = self.inner.executors.read_unpoisoned().clone();
        let tasks = self.task_snapshot();
        let mut runners = self.inner.engine_runners.write_unpoisoned();
        runners.clear();

        for (name, cfg) in tasks.iter() {
            // No engine for this kind: register nothing and say nothing here.
            // The task then has no runner, which the existing paths already
            // report — `MissingRunner` in the snapshot, and `Unregistered Task
            // '…' — skipping` when the task set is spawned.
            let (spec, executor) = match cfg.executor() {
                TaskExecutor::Php(spec) => {
                    (spec, executors.get(&TaskExecutorKind::Php))
                }
                TaskExecutor::Js(spec) => {
                    (spec, executors.get(&TaskExecutorKind::Js))
                }
                TaskExecutor::Lua(spec) => {
                    (spec, executors.get(&TaskExecutorKind::Lua))
                }
                TaskExecutor::Builtin | TaskExecutor::Command(_) => continue,
            };
            let Some(executor) = executor.cloned() else {
                continue;
            };
            let spec = Arc::new(spec.clone());
            let graceful_timeout = cfg.graceful_timeout;
            let dispatch = effective_dispatch(cfg, None);
            // A daemon-timed program owns its slot for as long as it runs; a
            // bounded occurrence does not. Same distinction the command kind
            // draws, and for the same reason.
            let persistent = cfg.timing.is_daemon();
            let static_name: &'static str =
                Box::leak(name.clone().into_boxed_str());
            let task = name.clone();

            let mut runner: TaskRunner = TaskRunner::from_with_stop_context(
                move |state, cancellation, runtime, stop| {
                    Box::pin(run_with_environment(
                        TaskExecution {
                            task: task.clone(),
                            spec: Arc::clone(&spec),
                            state,
                            cancellation,
                            stop,
                            runtime,
                            graceful_timeout,
                            persistent,
                        },
                        Arc::clone(&executor),
                    ))
                },
            );
            runner.set_name(static_name);
            runner.set_task_scope(effective_scope(cfg, None));
            runner.set_task_dispatch(dispatch);
            runner.set_runtime_state(self.runtime_state(name));

            runners.insert(name.clone(), Arc::new(runner));
        }

        if !runners.is_empty() {
            debug!(
                tasks = runners.len(),
                "TaskService registered engine tasks"
            );
        }
        self
    }
}

/// Keep one logical attempt/slot while renewing a daemon's engine instance.
/// The raw recipe remains in `execution.spec`; only the per-instance copy is
/// evaluated, so inline overrides never accumulate across notifications.
async fn run_with_environment(
    execution: TaskExecution,
    executor: Executor,
) -> TaskResult<()> {
    let mut environment = ExecutionEnvironment::prepare(
        &execution.state,
        execution.spec.env_file.as_deref(),
        &execution.spec.env,
        execution.persistent,
    )?;
    loop {
        if execution.cancellation.is_cancelled() {
            return Err(crate::Error::stopped(crate::Reason::Shutdown));
        }
        let mut spec = (*execution.spec).clone();
        spec.env = environment.values().clone();
        // The engine receives evaluated values, not another file to reopen.
        spec.env_file = None;
        let cancellation = execution.cancellation.child_token();
        let mut running = executor(TaskExecution {
            task: execution.task.clone(),
            spec: Arc::new(spec),
            state: execution.state.clone(),
            cancellation: cancellation.clone(),
            stop: execution.stop.clone(),
            runtime: execution.runtime,
            graceful_timeout: execution.graceful_timeout,
            persistent: execution.persistent,
        });
        loop {
            tokio::select! {
                biased;
                // Await rather than drop: the adapter joins its thread/work.
                _ = execution.cancellation.cancelled() => return running.await,
                result = &mut running => return result,
                result = environment.notified() => match result {
                    Ok(()) => {
                        drain_for_environment(running.as_mut(), &cancellation).await?;
                        debug!(task = %execution.task, slot = execution.runtime.slot(),
                            "task environment changed; engine execution drained");
                        break;
                    }
                    Err(error) => warn!(task = %execution.task, %error,
                        "task environment replacement rejected; keeping engine execution"),
                }
            }
        }
    }
}
