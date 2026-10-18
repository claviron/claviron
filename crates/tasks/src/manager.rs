//! Fleet-facing management commands for configured tasks.
//!
//! `TaskManager` is a thin domain handle. It is neither a provider nor a
//! runnable: the registered `TaskService` owns local execution, while the
//! runtime's typed event bridge carries commands to peer fleet members.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use claviron_orbit::{
    EVENT_PAYLOAD_MAX, FleetEventCodec, FleetEventRegistry, OrbitStateExt,
};
use claviron_runtime::AppState;
use claviron_runtime::events::Event;

use crate::TaskResult;
use crate::service::TaskService;
use crate::snapshot::{TaskListSnapshot, TaskStatusSnapshot};

const TASK_COMMAND_TOPIC: &str = "claviron.tasks.command.v1";
const ORBIT_EVENT_HEADER_BYTES: usize = 12;
const TASK_COMMAND_HEADER_BYTES: usize = 1;
const MAX_TASK_NAME_BYTES: usize = EVENT_PAYLOAD_MAX
    - ORBIT_EVENT_HEADER_BYTES
    - TASK_COMMAND_TOPIC.len()
    - TASK_COMMAND_HEADER_BYTES;
static NEXT_LOCAL_RUN_ID: AtomicU64 = AtomicU64::new(1);

/// Process-local identity for one explicit task execution request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskRunId(u64);

impl TaskRunId {
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Failure to submit a task-management command.
#[derive(Debug, thiserror::Error)]
pub enum TaskManagerError {
    #[error("TaskService is not registered")]
    ServiceUnavailable,

    #[error("task name cannot be empty")]
    EmptyTaskName,

    #[error(
        "task name is too long for the Orbit command frame: {actual} bytes, max {max}"
    )]
    TaskNameTooLong { actual: usize, max: usize },

    #[error("task '{0}' is not configured")]
    TaskNotFound(String),

    #[error(
        "task '{0}' uses manual timing and has no schedule to start or stop"
    )]
    ManualTaskHasNoSchedule(String),

    #[error("task '{0}' is stopped; start it before applying this command")]
    TaskStopped(String),

    #[error("task '{0}' is not a daemon; only daemon tasks can be restarted")]
    RestartRequiresDaemon(String),

    #[error("task '{0}' is not eligible in this runtime")]
    TaskIneligible(String),

    #[error("task '{0}' has no registered runner in this runtime")]
    TaskRunnerUnavailable(String),

    #[error("task '{0}' uses daemon timing; use start instead of run")]
    DaemonRequiresStart(String),

    #[error(
        "execution timeout must be greater than zero and representable by the runtime"
    )]
    InvalidExecutionTimeout,

    #[error("task management is unavailable during shutdown")]
    ShuttingDown,

    #[error("Orbit fleet is unavailable for task snapshots")]
    FleetUnavailable,

    #[error("task snapshot failed: {0}")]
    Snapshot(String),
}

pub type TaskManagerResult<T> = Result<T, TaskManagerError>;

/// Completed local execution requested through [`TaskManager::run`].
#[derive(Debug)]
pub struct TaskRunOutcome {
    pub run_id: TaskRunId,
    pub result: TaskResult<()>,
}

/// Submit logical task commands to every member of the current fleet.
///
/// Fleet lifecycle commands name a task, never a worker. Each member applies
/// its own scope eligibility and configured dispatch topology. `run` is the
/// exception: it executes one occurrence only in the runtime that receives the
/// call and never enters the Orbit event path.
///
/// Fleet lifecycle submission is asynchronous and reports acceptance only.
/// `run` instead waits for the local execution result. Enable and disable
/// mutate the live trigger state held in the task table on every receiving
/// fleet member. A later config reload replaces that state from YAML.
#[derive(Clone)]
pub struct TaskManager {
    state: AppState,
    service: Arc<TaskService>,
}

impl TaskManager {
    /// Inspect durable command records across all former and current worker
    /// slots, including tasks no longer configured. This never adopts, signals
    /// or starts anything; legacy records explicitly lack owner metadata.
    /// Filesystem work runs outside the runtime executor's async workers.
    pub async fn processes(
        &self
    ) -> TaskManagerResult<crate::TaskProcessSnapshot> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            crate::process::presence::inspect(&state)
        })
        .await
        .map_err(|error| TaskManagerError::Snapshot(error.to_string()))?
        .map_err(|error| TaskManagerError::Snapshot(error.to_string()))
    }

    /// Resolve the process-local task service and create an owned manager.
    pub fn new(state: &AppState) -> TaskManagerResult<Self> {
        let service = state
            .registry_ref()
            .resolve::<TaskService>()
            .ok_or(TaskManagerError::ServiceUnavailable)?;
        Ok(Self { state: state.clone(), service })
    }

    /// Arm this task's configured schedule where eligible and let it run.
    ///
    /// The name is the operator's: this is live control, not a change to the
    /// declaration. `tasks.yaml`'s `enabled` is the value the schedule is given
    /// whenever that declaration is applied — at boot, and again at every
    /// reload — so a start lives until the next time it is.
    pub fn start(
        &self,
        name: &str,
    ) -> TaskManagerResult<()> {
        self.submit(name, TaskCommandAction::Start)
    }

    /// Disarm the schedule and drain active placements.
    ///
    /// One verb, because for a schedule these are the same act: no further
    /// occurrences, and the ones in flight are drained rather than abandoned.
    pub fn stop(
        &self,
        name: &str,
    ) -> TaskManagerResult<()> {
        self.submit(name, TaskCommandAction::Stop)
    }

    /// Apply the configured reload action, defaulting to a generation restart.
    pub fn reload(
        &self,
        name: &str,
    ) -> TaskManagerResult<()> {
        self.submit(name, TaskCommandAction::Reload)
    }

    /// Drain and replace this daemon's automatic runtime generation.
    ///
    /// Unlike stop followed by start, restart does not mutate the live
    /// schedule policy and is applied as one serialized fleet command.
    pub fn restart(
        &self,
        name: &str,
    ) -> TaskManagerResult<()> {
        self.submit(name, TaskCommandAction::Restart)
    }

    /// Execute one occurrence in this runtime without changing trigger policy.
    pub async fn run(
        &self,
        name: &str,
        execution_timeout: Option<Duration>,
    ) -> TaskManagerResult<TaskRunOutcome> {
        validate_local_task_name(name)?;
        if self.state.is_shutting_down() {
            return Err(TaskManagerError::ShuttingDown);
        }
        if execution_timeout.is_some_and(|timeout| {
            timeout.is_zero()
                || tokio::time::Instant::now().checked_add(timeout).is_none()
        }) {
            return Err(TaskManagerError::InvalidExecutionTimeout);
        }
        let run_id = NEXT_LOCAL_RUN_ID.fetch_add(1, Ordering::Relaxed);
        let result = self
            .service
            .run_explicit(&self.state, name, run_id, execution_timeout)
            .await?;
        Ok(TaskRunOutcome { run_id: TaskRunId(run_id), result })
    }

    /// Return the freshest complete task snapshot published by each live fleet
    /// member. Standalone mode reads the process-local service directly.
    pub fn list(&self) -> TaskManagerResult<TaskListSnapshot> {
        if self.state.is_standalone() {
            return Ok(self.service.local_snapshot(&self.state));
        }
        let fleet =
            self.state.fleet().ok_or(TaskManagerError::FleetUnavailable)?;
        let mut snapshot = self
            .service
            .publish_local_snapshot(&self.state)
            .map_err(TaskManagerError::Snapshot)?;
        let policy = self.service.snapshot_config();
        let members = claviron_orbit::service::current_members(&self.state)
            .map_err(TaskManagerError::Snapshot)?;
        let mut collected = crate::snapshot::collect(
            &fleet,
            snapshot.captured_at,
            policy.max_age,
            &members,
        );

        snapshot.members_expected = members.len() as u16;
        snapshot.members_reporting = collected.members_reporting;
        for task in &mut snapshot.tasks {
            task.placements =
                collected.by_task.remove(&task.name).unwrap_or_default();
        }
        Ok(snapshot)
    }

    /// Return the current fleet snapshot for one configured logical task.
    pub fn status(
        &self,
        name: &str,
    ) -> TaskManagerResult<TaskStatusSnapshot> {
        validate_local_task_name(name)?;
        let snapshot = self.list()?;
        let task = snapshot
            .tasks
            .into_iter()
            .find(|task| task.name == name)
            .ok_or_else(|| TaskManagerError::TaskNotFound(name.to_owned()))?;
        Ok(TaskStatusSnapshot {
            captured_at: snapshot.captured_at,
            members_expected: snapshot.members_expected,
            members_reporting: snapshot.members_reporting,
            task,
        })
    }

    fn submit(
        &self,
        name: &str,
        action: TaskCommandAction,
    ) -> TaskManagerResult<()> {
        validate_task_name(name)?;
        if self.state.is_shutting_down() {
            return Err(TaskManagerError::ShuttingDown);
        }
        self.service.validate_manager_command(name, action)?;
        self.state.events().emit(TaskCommand { action, name: name.to_owned() });
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TaskCommandAction {
    Start,
    Stop,
    Reload,
    Restart,
}

impl TaskCommandAction {
    const fn code(self) -> u8 {
        match self {
            Self::Start => 1,
            Self::Stop => 2,
            Self::Reload => 3,
            Self::Restart => 4,
        }
    }

    fn from_code(code: u8) -> Result<Self, String> {
        match code {
            1 => Ok(Self::Start),
            2 => Ok(Self::Stop),
            3 => Ok(Self::Reload),
            4 => Ok(Self::Restart),
            _ => Err(format!("unknown task command action {code}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TaskCommand {
    pub(crate) action: TaskCommandAction,
    pub(crate) name: String,
}

impl Event for TaskCommand {}

impl FleetEventCodec for TaskCommand {
    const TOPIC: &'static str = TASK_COMMAND_TOPIC;

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload =
            Vec::with_capacity(TASK_COMMAND_HEADER_BYTES + self.name.len());
        payload.push(self.action.code());
        payload.extend_from_slice(self.name.as_bytes());
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        let Some((&action, name)) = bytes.split_first() else {
            return Err("task command payload is empty".to_string());
        };
        if name.is_empty() {
            return Err("task command name is empty".to_string());
        }
        if name.len() > MAX_TASK_NAME_BYTES {
            return Err(format!(
                "task command name is too long: {} bytes, max {MAX_TASK_NAME_BYTES}",
                name.len()
            ));
        }
        Ok(Self {
            action: TaskCommandAction::from_code(action)?,
            name: std::str::from_utf8(name)
                .map_err(|error| {
                    format!("task command name is not UTF-8: {error}")
                })?
                .to_owned(),
        })
    }
}

fn validate_task_name(name: &str) -> TaskManagerResult<()> {
    validate_local_task_name(name)?;
    if name.len() > MAX_TASK_NAME_BYTES {
        return Err(TaskManagerError::TaskNameTooLong {
            actual: name.len(),
            max: MAX_TASK_NAME_BYTES,
        });
    }
    Ok(())
}

fn validate_local_task_name(name: &str) -> TaskManagerResult<()> {
    if name.is_empty() {
        return Err(TaskManagerError::EmptyTaskName);
    }
    Ok(())
}

pub(crate) fn register(
    state: &AppState,
    service: Arc<TaskService>,
) -> claviron_runtime::registry::Result<()> {
    let listener_service = service;
    state.events().listen::<TaskCommand, _>(move |state, command| {
        let service = Arc::clone(&listener_service);
        async move { service.apply_manager_command(&state, command).await }
    })?;

    // Idempotent, and the point of calling it here: `FleetEventRegistry` only
    // exists when this process is part of a fleet, and it is registered by
    // `claviron_orbit::register`. Resolving without this made "absent" mean
    // either "standalone" or "orbit has not registered yet" — the second is an
    // ordering accident, and the symptom is a bridge that is silently never
    // installed. After this call, absent means standalone and nothing else.
    claviron_orbit::register(state)?;
    if let Some(events) = state.registry_ref().resolve::<FleetEventRegistry>() {
        events.register_bridge::<TaskCommand>(state)?;
    }

    Ok(())
}

#[cfg(test)]
#[path = "../tests/manager.rs"]
mod tests;
