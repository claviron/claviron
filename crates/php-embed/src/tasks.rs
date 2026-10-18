//! Running a `php:` task on the embedded engine.
//!
//! `claviron-tasks` owns everything about *when* and *where*: the trigger, the
//! scope, the dispatch, the attempt budget, the backoff and the drain deadline.
//! This module owns exactly one thing — how a single occurrence reaches PHP —
//! and the whole coupling between the two crates is one installed closure.
//!
//! That split is not new work, it is the work that was already being done
//! twice. `TaskProgram` carries only the execution values that reach the
//! engine: `run_task_program` builds `PhpProgram::new(name, script, args,
//! environment)` and passes nothing else through. Scheduling and retry remain
//! in `TaskService`, as they do for its other executors.

use std::sync::Arc;

use claviron_runtime::AppState;
use claviron_tasks::service::TaskService;
use claviron_tasks::{
    Error as TaskError, Executor, TaskExecution, TaskExecutorKind, TaskResult,
};

use crate::config::TaskProgram;
use crate::engine::task::TaskExecutions;
use crate::service::PhpEmbedRuntime;

/// Install the PHP executor on the task service, if one is present.
///
/// Registration is idempotent and order-independent: installing also registers
/// every `php:` task already in the table, and `TaskService`'s own boot and
/// reload register again once the table is loaded. Whichever arrives last does
/// the work.
pub(crate) fn register(
    state: &AppState,
    runtime: Arc<PhpEmbedRuntime>,
) -> Result<(), String> {
    let Some(tasks) = state.registry_ref().resolve::<TaskService>() else {
        // A build that carries the engine but not the task service is a valid
        // composition; there is simply nothing to install into.
        return Ok(());
    };
    tasks.set_executor(TaskExecutorKind::Php, executor(runtime));
    Ok(())
}

/// Give the task service the invocation bridge's own task entry, on every
/// fleet member.
///
/// The entry is `scope: workers`, and scope is what keeps the master from
/// running it: a member the scope rules out spawns nothing for the task. The
/// master still has to know the task exists, because a management command for
/// it reaches every member and each one looks the name up in its own table.
pub(crate) fn register_bridge(
    state: &AppState,
    runtime: Arc<PhpEmbedRuntime>,
) -> Result<(), String> {
    let Some(tasks) = state.registry_ref().resolve::<TaskService>() else {
        return Ok(());
    };
    crate::service::invoke::register(&tasks, runtime)
}

fn executor(runtime: Arc<PhpEmbedRuntime>) -> Executor {
    Arc::new(move |execution: TaskExecution| {
        let runtime = Arc::clone(&runtime);
        Box::pin(run(execution, runtime))
    })
}

/// One occurrence: start the program on its own PHP thread, let it run until it
/// finishes or the task is cancelled, then drain it.
///
/// The drain deadline belongs to the task, not to this module. `None` waits
/// indefinitely and never interrupts — see
/// [GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION][graceful-ownership].
/// A value is the operator authorising the interrupt, which is the only thing
/// that ends a loop that never asks
/// `claviron_is_cancelled()`.
///
/// [graceful-ownership]: https://github.com/claviron/claviron/blob/main/docs/design/GRACEFUL_OWNERSHIP.md
async fn run(
    execution: TaskExecution,
    runtime: Arc<PhpEmbedRuntime>,
) -> TaskResult<()> {
    runtime
        .ensure_booted()
        .map_err(|error| TaskError::other(error.to_string()))?;

    let TaskExecution {
        task,
        spec,
        cancellation,
        runtime: task_runtime,
        graceful_timeout,
        ..
    } = execution;

    // `task_worker_number` is 1-indexed; PHP instances are 0-indexed, and the
    // number is what reaches the script as CLAVIRON_PHP_INSTANCE.
    let instance = task_runtime.task_worker_number.saturating_sub(1);

    let mut program = TaskProgram::new(task.clone(), spec.script.clone());
    program = program.args(spec.args.iter().cloned());
    program = program.log(spec.log.clone());
    if let Some(cwd) = spec.cwd.as_deref() {
        program = program.cwd(cwd);
    }
    // The same identity a command task receives, in the same spelling, so a
    // program converted between the two kinds reads the same values.
    for (key, value) in task_runtime.identity_env() {
        program = program.environment(key, value);
    }
    // Declared values win over identity, as they do for a command task.
    for (key, value) in &spec.env {
        program = program.environment(key.clone(), value.clone());
    }
    let mut executions = TaskExecutions::default();
    executions
        .start_instance(&program, instance)
        .await
        .map_err(TaskError::other)?;

    tokio::select! {
        () = cancellation.cancelled() => {}
        () = executions.wait() => {}
    }

    let failed = executions.failed();
    // The deadline starts here, where the drain does — not when the task
    // started. `drain_until` is the production form because the deadline
    // belongs to the caller; this caller has exactly one drain to spend it on.
    let deadline =
        graceful_timeout.map(|grace| tokio::time::Instant::now() + grace);
    executions.drain_until(deadline).await.map_err(TaskError::other)?;
    if failed {
        return Err(TaskError::other(format!(
            "php task '{task}' ended in failure"
        )));
    }
    Ok(())
}
