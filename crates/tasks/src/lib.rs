use claviron_runtime::AppState;

mod config;
mod environment;

pub mod executor;
mod manager;
mod output;
mod pool;
mod process;
pub mod registry;
pub mod runner;
mod scheduler;
pub mod service;
mod snapshot;

mod error;
#[cfg(feature = "metrics")]
mod metrics;

pub use claviron_macros::task;
pub(crate) use config::*;
pub use config::{
    SchedulerConfig, TaskConfig, TaskExecutor, TaskExecutorKind, TaskOverlap,
};
pub use error::{Error, Reason, TaskResult};
pub use executor::{Executor, ScriptSpec, TaskExecution};
pub use manager::{
    TaskManager, TaskManagerError, TaskManagerResult, TaskRunId, TaskRunOutcome,
};
pub use output::{LogConfig, LogTarget, TaskOutput};
pub use process::{
    ProcessOwner, RestartPolicy, TaskProcessControl, TaskProcessIssue,
    TaskProcessLiveness, TaskProcessPhase, TaskProcessSnapshot,
    TaskProcessStatus,
};
pub use registry::{TaskDispatch, TaskRegistration, TaskScope};
pub use runner::{StopCause, StopContext, TaskRunner, TaskRuntime};
pub use snapshot::{
    TaskExecutionOutcome, TaskExecutionResult, TaskKind, TaskLifecycle,
    TaskListSnapshot, TaskMember, TaskMemberRole, TaskPlacementStatus,
    TaskStatus, TaskStatusSnapshot, TaskTimingSummary,
};

/// Register the task service and every inventory-discovered Rust task.
///
/// `tasks.yaml` is read asynchronously by the service's `boot()` hook; this
/// function only performs synchronous registry wiring.
pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    claviron_orbit::register_fleet(state)?;
    claviron_orbit::register_lock(state)?;
    if !state.is_standalone() {
        claviron_orbit::shm::register_reusable_ring::<
            snapshot::TaskSnapshotRecord,
        >(state, "TaskSnapshotShm", "task-snapshot-ring")?;
    }
    let mut builder =
        service::TaskServiceBuilder::new(state.config_dir().join("tasks.yaml"));
    builder
        .register_discovered_tasks()
        .map_err(claviron_runtime::registry::Error::msg)?;
    scheduler::register(&mut builder)
        .map_err(claviron_runtime::registry::Error::msg)?;

    let service = builder.build();
    service.attach_state(state);
    state.bind_config_environment::<service::TaskService>()?;
    state.registry_ref().register(service.clone())?;
    manager::register(state, service.clone())?;

    // Registration stays unconditional when metrics are compiled in. The
    // sampler itself is dormant while no command task is running and reads
    // its cadence from the reloadable TaskService config.
    #[cfg(feature = "metrics")]
    metrics::register(state, service)?;

    Ok(())
}

/// Implementation details used by the `#[task]` proc-macro expansion.
///
/// Task provider crates should import the macro from `claviron_tasks::task`
/// instead of depending on `claviron-macros` directly. They must not
/// depend on `inventory` directly. The macro expands through
/// `::claviron_tasks::_macros::inventory` so the registration mechanism stays
/// an internal concern of this crate.
#[doc(hidden)]
pub mod _macros {
    pub use ::inventory;
}
