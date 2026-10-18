//! Fleet-facing management commands for configured worker slots.

use std::sync::Arc;

use claviron_orbit::{FleetEventCodec, FleetEventRegistry, OrbitStateExt};
use claviron_runtime::events::Event;
use claviron_runtime::{AppState, WorkerId};

use crate::control::WorkerControl;
use crate::lifecycle::CommandKey;
use crate::snapshot::{WorkerListSnapshot, WorkerStatusSnapshot};

const WORKER_COMMAND_TOPIC: &str = "claviron.supervisor.worker.command.v4";
const WORKER_COMMAND_BYTES: usize = 5 + 8;

#[derive(Debug, thiserror::Error)]
pub enum WorkerManagerError {
    #[error("worker management requires a supervised fleet")]
    Unavailable,
    #[error(
        "worker #{worker_id} does not exist; configured worker ids are 1..={worker_count}"
    )]
    WorkerNotFound { worker_id: u8, worker_count: u8 },
    #[error("worker management is unavailable during shutdown")]
    ShuttingDown,
    #[error(
        "worker #{worker_id} is the last running worker; stopping it would make worker-hosted management unavailable"
    )]
    LastRunningWorker { worker_id: u8 },
    #[error("worker #{worker_id} is {lifecycle}, not running")]
    WorkerNotRunning { worker_id: u8, lifecycle: &'static str },
    #[error(
        "rolling restart requires at least two running workers; found {running}"
    )]
    InsufficientRunningWorkers { running: usize },
    #[error(
        "rolling restart requires a stable fleet; worker #{worker_id} is {lifecycle}"
    )]
    FleetNotStable { worker_id: u8, lifecycle: &'static str },
    #[error(
        "worker snapshot has {reported} configured slot(s), but its fleet size is {worker_count}"
    )]
    FleetSizeMismatch { worker_count: u8, reported: usize },
    #[error(
        "fleet worker capacity {worker_capacity} is already fully assigned; configured workers: {worker_count}"
    )]
    CapacityReached { worker_count: u8, worker_capacity: u8 },
    #[error("the final configured worker cannot be removed")]
    LastConfiguredWorker,
    #[error(
        "fleet resize requires at least two running workers; found {running}"
    )]
    ResizeNeedsRunningWorkers { running: usize },
    #[error("worker snapshot failed: {0}")]
    Snapshot(String),
}

pub type WorkerManagerResult<T> = Result<T, WorkerManagerError>;

/// Accepted logical fleet-size transition. The master applies it
/// asynchronously while preserving the immutable physical fleet capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerResizePlan {
    previous_worker_count: u8,
    target_worker_count: u8,
    command: CommandKey,
}

impl WorkerResizePlan {
    pub const fn previous_worker_count(self) -> u8 {
        self.previous_worker_count
    }

    pub const fn target_worker_count(self) -> u8 {
        self.target_worker_count
    }

    /// The key the master records on the operation it accepts for this plan;
    /// the snapshot's `operation.origin` carries it back.
    pub const fn command(self) -> CommandKey {
        self.command
    }
}

/// Owned domain handle for worker process management.
///
/// Commands are accepted asynchronously and applied only by the master-side
/// [`WorkerControl`]. Every submission returns the [`CommandKey`] it was
/// published with; acceptance means the command is on its way to the master,
/// and the client finds the operation the master opened for it by that key in
/// the snapshot's `operation.origin`. `list` reads the latest read-only
/// snapshot published from that same authoritative process table.
#[derive(Clone)]
pub struct WorkerManager {
    state: AppState,
}

impl WorkerManager {
    pub fn new(state: &AppState) -> WorkerManagerResult<Self> {
        if state.is_standalone()
            || state.worker_count() == 0
            || state.fleet().is_none()
        {
            return Err(WorkerManagerError::Unavailable);
        }
        Ok(Self { state: state.clone() })
    }

    pub fn start(
        &self,
        worker_id: u8,
    ) -> WorkerManagerResult<CommandKey> {
        self.submit(worker_id, WorkerCommandAction::Start)
    }

    pub fn stop(
        &self,
        worker_id: u8,
    ) -> WorkerManagerResult<CommandKey> {
        self.validate_stop(worker_id)?;
        self.submit(worker_id, WorkerCommandAction::Stop)
    }

    pub fn reload(
        &self,
        worker_id: u8,
    ) -> WorkerManagerResult<CommandKey> {
        self.submit(worker_id, WorkerCommandAction::Reload)
    }

    /// Gracefully drain one running worker and start a new incarnation of the
    /// same configured slot. The master awaits the old process exit before it
    /// submits the replacement; readiness remains observable through `list`.
    pub fn restart(
        &self,
        worker_id: u8,
    ) -> WorkerManagerResult<CommandKey> {
        self.validate_restart(worker_id)?;
        self.submit(worker_id, WorkerCommandAction::Restart)
    }

    /// Reload every worker that is currently running. Stopped slots remain stopped.
    pub fn reload_workers(&self) -> WorkerManagerResult<CommandKey> {
        if self.state.is_shutting_down() {
            return Err(WorkerManagerError::ShuttingDown);
        }
        Ok(self.emit_fleet_command(WorkerCommandAction::ReloadAll))
    }

    /// Replace every currently running worker process one at a time.
    /// Stopped slots remain stopped.
    pub fn restart_workers(&self) -> WorkerManagerResult<CommandKey> {
        if self.state.is_shutting_down() {
            return Err(WorkerManagerError::ShuttingDown);
        }
        self.validate_restart_workers()?;
        Ok(self.emit_fleet_command(WorkerCommandAction::RestartAll))
    }

    /// Increase the logical fleet size by one worker without changing its
    /// physical Orbit capacity.
    pub fn add_worker(&self) -> WorkerManagerResult<WorkerResizePlan> {
        self.resize(ResizeDirection::Grow)
    }

    /// Decrease the logical fleet size by removing its highest worker id.
    /// The final configured worker is never removable.
    pub fn remove_worker(&self) -> WorkerManagerResult<WorkerResizePlan> {
        self.resize(ResizeDirection::Shrink)
    }

    pub fn list(&self) -> WorkerManagerResult<WorkerListSnapshot> {
        let fleet =
            self.state.fleet().ok_or(WorkerManagerError::Unavailable)?;
        crate::snapshot::collect(&fleet, std::time::SystemTime::now())
            .map_err(WorkerManagerError::Snapshot)
    }

    /// Return the current authoritative snapshot for one configured worker slot.
    pub fn status(
        &self,
        worker_id: u8,
    ) -> WorkerManagerResult<WorkerStatusSnapshot> {
        let snapshot = self.list()?;
        let worker = snapshot
            .workers
            .into_iter()
            .find(|worker| worker.worker_id == worker_id)
            .ok_or(WorkerManagerError::WorkerNotFound {
                worker_id,
                worker_count: snapshot.workers_expected,
            })?;
        Ok(WorkerStatusSnapshot {
            captured_at: snapshot.captured_at,
            workers_expected: snapshot.workers_expected,
            build: snapshot.build,
            master: snapshot.master,
            master_runtime: snapshot.master_runtime,
            fleet_mode: snapshot.fleet_mode,
            worker,
        })
    }

    fn submit(
        &self,
        worker_id: u8,
        action: WorkerCommandAction,
    ) -> WorkerManagerResult<CommandKey> {
        if self.state.is_shutting_down() {
            return Err(WorkerManagerError::ShuttingDown);
        }
        // Retained workers keep their birth topology after a resize. Only the
        // current master snapshot can establish whether this target exists.
        self.status(worker_id)?;
        let command = self.next_command();
        self.state.events().emit(WorkerCommand {
            action,
            worker_id,
            origin_worker_id: self.state.worker_id().unwrap_or(0),
            expected_worker_count: 0,
            target_worker_count: 0,
            command,
        });
        Ok(command)
    }

    fn emit_fleet_command(
        &self,
        action: WorkerCommandAction,
    ) -> CommandKey {
        let command = self.next_command();
        self.state.events().emit(WorkerCommand {
            action,
            worker_id: 0,
            origin_worker_id: self.state.worker_id().unwrap_or(0),
            expected_worker_count: 0,
            target_worker_count: 0,
            command,
        });
        command
    }

    fn next_command(&self) -> CommandKey {
        CommandKey::next(self.state.worker_id().map(WorkerId::new))
    }

    fn resize(
        &self,
        direction: ResizeDirection,
    ) -> WorkerManagerResult<WorkerResizePlan> {
        if self.state.is_shutting_down() {
            return Err(WorkerManagerError::ShuttingDown);
        }
        let snapshot = self.list()?;
        validate_stable_fleet(&snapshot)?;
        let previous_worker_count = snapshot.workers_expected;
        let target_worker_count = match direction {
            ResizeDirection::Grow => {
                let worker_capacity = self.state.worker_capacity();
                if previous_worker_count >= worker_capacity {
                    return Err(WorkerManagerError::CapacityReached {
                        worker_count: previous_worker_count,
                        worker_capacity,
                    });
                }
                previous_worker_count + 1
            }
            ResizeDirection::Shrink => {
                if previous_worker_count <= 1 {
                    return Err(WorkerManagerError::LastConfiguredWorker);
                }
                let running = snapshot
                    .workers
                    .iter()
                    .filter(|worker| is_running(worker.lifecycle()))
                    .count();
                if running < 2 {
                    return Err(
                        WorkerManagerError::ResizeNeedsRunningWorkers {
                            running,
                        },
                    );
                }
                previous_worker_count - 1
            }
        };
        let command = self.next_command();
        self.state.events().emit(WorkerCommand {
            action: WorkerCommandAction::Resize,
            worker_id: 0,
            origin_worker_id: self.state.worker_id().unwrap_or(0),
            expected_worker_count: previous_worker_count,
            target_worker_count,
            command,
        });
        Ok(WorkerResizePlan {
            previous_worker_count,
            target_worker_count,
            command,
        })
    }

    fn validate_restart_workers(&self) -> WorkerManagerResult<()> {
        let snapshot = self.list()?;
        for worker in &snapshot.workers {
            match worker.lifecycle() {
                crate::snapshot::WorkerLifecycle::Running
                | crate::snapshot::WorkerLifecycle::Stopped => {}
                lifecycle => {
                    return Err(WorkerManagerError::FleetNotStable {
                        worker_id: worker.worker_id,
                        lifecycle: lifecycle.as_str(),
                    });
                }
            }
        }
        let running = snapshot
            .workers
            .iter()
            .filter(|worker| {
                worker.lifecycle() == crate::snapshot::WorkerLifecycle::Running
            })
            .count();
        if running < 2 {
            return Err(WorkerManagerError::InsufficientRunningWorkers {
                running,
            });
        }
        Ok(())
    }

    fn validate_restart(
        &self,
        worker_id: u8,
    ) -> WorkerManagerResult<()> {
        let snapshot = self.list()?;
        let Some(target) = snapshot
            .workers
            .iter()
            .find(|worker| worker.worker_id == worker_id)
        else {
            return Err(WorkerManagerError::WorkerNotFound {
                worker_id,
                worker_count: snapshot.workers_expected,
            });
        };
        if target.lifecycle() != crate::snapshot::WorkerLifecycle::Running {
            return Err(WorkerManagerError::WorkerNotRunning {
                worker_id,
                lifecycle: target.lifecycle().as_str(),
            });
        }
        if snapshot
            .workers
            .iter()
            .filter(|worker| {
                worker.lifecycle() == crate::snapshot::WorkerLifecycle::Running
            })
            .count()
            <= 1
        {
            return Err(WorkerManagerError::LastRunningWorker { worker_id });
        }
        Ok(())
    }

    fn validate_stop(
        &self,
        worker_id: u8,
    ) -> WorkerManagerResult<()> {
        let snapshot = self.list()?;
        let Some(target) = snapshot
            .workers
            .iter()
            .find(|worker| worker.worker_id == worker_id)
        else {
            return Err(WorkerManagerError::WorkerNotFound {
                worker_id,
                worker_count: snapshot.workers_expected,
            });
        };
        if is_running(target.lifecycle())
            && snapshot
                .workers
                .iter()
                .filter(|worker| is_running(worker.lifecycle()))
                .count()
                <= 1
        {
            return Err(WorkerManagerError::LastRunningWorker { worker_id });
        }
        Ok(())
    }
}

fn validate_stable_fleet(
    snapshot: &WorkerListSnapshot
) -> WorkerManagerResult<()> {
    if snapshot.workers.len() != usize::from(snapshot.workers_expected) {
        return Err(WorkerManagerError::FleetSizeMismatch {
            worker_count: snapshot.workers_expected,
            reported: snapshot.workers.len(),
        });
    }
    for (index, worker) in snapshot.workers.iter().enumerate() {
        let expected_id =
            u8::try_from(index + 1).expect("worker snapshot exceeds uint8");
        if worker.worker_id != expected_id {
            return Err(WorkerManagerError::FleetSizeMismatch {
                worker_count: snapshot.workers_expected,
                reported: snapshot.workers.len(),
            });
        }
        match worker.lifecycle() {
            crate::snapshot::WorkerLifecycle::Running
            | crate::snapshot::WorkerLifecycle::Stopped => {}
            lifecycle => {
                return Err(WorkerManagerError::FleetNotStable {
                    worker_id: worker.worker_id,
                    lifecycle: lifecycle.as_str(),
                });
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ResizeDirection {
    Grow,
    Shrink,
}

const fn is_running(lifecycle: crate::snapshot::WorkerLifecycle) -> bool {
    matches!(
        lifecycle,
        crate::snapshot::WorkerLifecycle::Running
            | crate::snapshot::WorkerLifecycle::Reloading
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkerCommandAction {
    Start,
    Stop,
    Reload,
    Restart,
    ReloadAll,
    RestartAll,
    Resize,
}

impl WorkerCommandAction {
    const fn code(self) -> u8 {
        match self {
            Self::Start => 1,
            Self::Stop => 2,
            Self::Reload => 3,
            Self::ReloadAll => 4,
            Self::RestartAll => 5,
            Self::Resize => 6,
            Self::Restart => 7,
        }
    }

    fn from_code(code: u8) -> Result<Self, String> {
        match code {
            1 => Ok(Self::Start),
            2 => Ok(Self::Stop),
            3 => Ok(Self::Reload),
            4 => Ok(Self::ReloadAll),
            5 => Ok(Self::RestartAll),
            6 => Ok(Self::Resize),
            7 => Ok(Self::Restart),
            _ => Err(format!("unknown worker command action {code}")),
        }
    }
}

/// Typed management request relayed through Orbit to the master.
/// Names the action, target, submitting worker and topology preconditions so
/// the master can validate intent and acquire operation ownership. Publication
/// is submission, not proof that the worker action has completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerCommand {
    pub(crate) action: WorkerCommandAction,
    pub(crate) worker_id: u8,
    pub(crate) origin_worker_id: u8,
    pub(crate) expected_worker_count: u8,
    pub(crate) target_worker_count: u8,
    /// The submitter's key, recorded on the operation the master accepts.
    pub(crate) command: CommandKey,
}

impl Event for WorkerCommand {}

impl FleetEventCodec for WorkerCommand {
    const TOPIC: &'static str = WORKER_COMMAND_TOPIC;

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(WORKER_COMMAND_BYTES);
        payload.extend_from_slice(&[
            self.action.code(),
            self.worker_id,
            self.origin_worker_id,
            self.expected_worker_count,
            self.target_worker_count,
        ]);
        payload.extend_from_slice(&self.command.raw().to_be_bytes());
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != WORKER_COMMAND_BYTES {
            return Err(format!(
                "invalid worker command payload length: expected {WORKER_COMMAND_BYTES}, got {}",
                bytes.len()
            ));
        }
        let worker_id = bytes[1];
        let origin_worker_id = bytes[2];
        let expected_worker_count = bytes[3];
        let target_worker_count = bytes[4];
        let command = CommandKey::from_raw(u64::from_be_bytes(
            bytes[5..13].try_into().expect("8 bytes"),
        ))
        .ok_or_else(|| {
            "worker command requires a non-zero command key".to_string()
        })?;
        let action = WorkerCommandAction::from_code(bytes[0])?;
        let fleet_action = matches!(
            action,
            WorkerCommandAction::ReloadAll
                | WorkerCommandAction::RestartAll
                | WorkerCommandAction::Resize
        );
        if worker_id == 0 && !fleet_action {
            return Err("worker command id must be non-zero".to_string());
        }
        if worker_id != 0 && fleet_action {
            return Err(
                "fleet worker command must use worker id zero".to_string()
            );
        }
        if action == WorkerCommandAction::Resize {
            if expected_worker_count == 0
                || target_worker_count == 0
                || expected_worker_count.abs_diff(target_worker_count) != 1
            {
                return Err(
                    "fleet resize must change a non-zero worker count by exactly one".to_string()
                );
            }
        } else if expected_worker_count != 0 || target_worker_count != 0 {
            return Err("non-resize worker command must not carry fleet sizes"
                .to_string());
        }
        Ok(Self {
            action,
            worker_id,
            origin_worker_id,
            expected_worker_count,
            target_worker_count,
            command,
        })
    }
}

pub(crate) fn register(
    state: &AppState,
    supervisor: Option<Arc<WorkerControl>>,
) -> claviron_runtime::registry::Result<()> {
    let events = state
        .registry_ref()
        .resolve::<FleetEventRegistry>()
        .ok_or_else(|| {
            claviron_runtime::registry::Error::msg(
                "FleetEventRegistry is unavailable for worker management events"
            )
        })?;
    events.register_bridge::<WorkerCommand>(state)?;

    if let Some(supervisor) = supervisor {
        // Submit only. The operation runs in its own task under its own
        // ownership, so a command arriving while another drains is answered
        // now instead of queueing on this callback behind the drain.
        state.events().listen::<WorkerCommand, _>(move |state, command| {
            let supervisor = Arc::clone(&supervisor);
            async move {
                if !state.is_master() {
                    return Ok(());
                }
                crate::management::submit::submit_manager_command(
                    &supervisor,
                    &state,
                    command,
                );
                Ok(())
            }
        })?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/management/manager.rs"]
mod tests;
