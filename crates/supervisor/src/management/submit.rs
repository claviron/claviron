use std::sync::Arc;
use std::time::Duration;

use claviron_runtime::{AppState, WorkerId};
use tracing::{debug, error, info, warn};

use crate::control::WorkerControl;
use crate::lifecycle::{
    OperationGuard, OperationKind, OperationOrigin, OperationScope, Refusal,
    SubmitResult,
};
use crate::manager::{WorkerCommand, WorkerCommandAction};
use crate::{Error, Result};

/// Hand an accepted future to the registered runner, retaining its scope
/// ownership until completion. No task is spawned here.
pub(crate) fn enqueue_operation(
    control: &WorkerControl,
    guard: OperationGuard,
    work: impl Future<Output = Result<()>> + Send + 'static,
) -> std::result::Result<(), crate::lifecycle::RejectionReason> {
    let operation = guard.operation();
    control.operation_queue.submit(Box::pin(async move {
        let _guard = guard;
        match work.await {
            Ok(()) => debug!(%operation, "lifecycle operation completed"),
            Err(error) => {
                error!(%operation, %error, "lifecycle operation failed")
            }
        }
    }))
}

/// Take ownership of `scope` for `kind`, or log why not.
pub(crate) fn acquire(
    control: &WorkerControl,
    scope: OperationScope,
    kind: OperationKind,
    origin: OperationOrigin,
) -> std::result::Result<OperationGuard, Refusal> {
    if control.is_shutting_down() {
        warn!(
            "{kind} on {scope} by {origin} refused: the fleet is shutting down"
        );
        return Err(Refusal::ShuttingDown);
    }
    control.operations.try_acquire(scope, kind, origin).inspect_err(|refusal| match refusal {
        Refusal::AlreadyInProgress(active) => {
            debug!(%active, "{kind} on {scope} joins the running operation")
        }
        Refusal::Conflict(active) => {
            warn!(%active, "{kind} on {scope} refused: the target is owned by another operation")
        }
        Refusal::ShuttingDown => {}
    })
}

pub(crate) fn submit_crash_recovery(
    control: &Arc<WorkerControl>,
    worker_id: WorkerId,
    state: &AppState,
) {
    let Ok(guard) = crate::management::submit::acquire(
        control,
        OperationScope::Slot(worker_id),
        OperationKind::CrashRecovery,
        OperationOrigin::Supervisor,
    ) else {
        return;
    };
    let supervisor = Arc::clone(control);
    let state = state.clone();
    let _ = crate::management::submit::enqueue_operation(
        control,
        guard,
        async move {
            // An operator asking for status during a backoff gets an answer;
            // only this slot's recovery waits.
            if let Some(wait) = crate::lifecycle::recovery::pending_backoff(
                &supervisor,
                worker_id,
            )
            .await
            {
                warn!(
                    "Worker #{} backoff: waiting {:?} before restart",
                    worker_id, wait
                );
                tokio::time::sleep(wait).await;
            }
            crate::lifecycle::recovery::restart_worker(
                &supervisor,
                worker_id,
                &state,
            )
            .await
        },
    );
}

pub(crate) fn submit_heartbeat_recovery(
    control: &Arc<WorkerControl>,
    state: &AppState,
    worker_id: WorkerId,
    pid: u32,
    heartbeat_timeout: Duration,
) {
    let Ok(guard) = crate::management::submit::acquire(
        control,
        OperationScope::Slot(worker_id),
        OperationKind::HeartbeatRecovery,
        OperationOrigin::Supervisor,
    ) else {
        return;
    };
    let operation = guard.operation().id;
    let supervisor = Arc::clone(control);
    let state = state.clone();
    let _ = crate::management::submit::enqueue_operation(
        control,
        guard,
        async move {
            crate::lifecycle::recovery::drain_stale_worker(
                &supervisor,
                &state,
                worker_id,
                pid,
                heartbeat_timeout,
                operation,
            )
            .await
        },
    );
}

pub(crate) fn submit_rotation(
    control: &Arc<WorkerControl>,
    worker_id: WorkerId,
    state: &AppState,
) {
    let Ok(guard) = crate::management::submit::acquire(
        control,
        OperationScope::Slot(worker_id),
        OperationKind::Rotation,
        OperationOrigin::Supervisor,
    ) else {
        return;
    };
    let operation = guard.operation().id;
    let supervisor = Arc::clone(control);
    let state = state.clone();
    let _ = crate::management::submit::enqueue_operation(
        control,
        guard,
        async move {
            crate::lifecycle::rotation::rotate_worker(
                &supervisor,
                worker_id,
                &state,
                operation,
            )
            .await
            .map_err(|error| {
                Error::service(format!(
                    "rotation of worker #{worker_id} failed: {error}"
                ))
            })
        },
    );
}

/// Record a management command as an operation and run it in its own task.
///
/// Returns at once. `Accepted` means the operation is recorded and
/// running, not that it has completed; clients observe completion through
/// the worker snapshots. A command for a target another operation owns is
/// refused here, with that operation, instead of waiting behind it — the
/// wait used to be a lock held across a drain that `shutdown_timeout: None`
/// lets run indefinitely.
pub(crate) fn submit_manager_command(
    control: &Arc<WorkerControl>,
    state: &AppState,
    command: WorkerCommand,
) -> SubmitResult {
    // The wire carries bare bytes. The codec has already rejected a zero
    // slot id for slot commands; a zero origin means the command did not
    // come from a worker, so it becomes `None` rather than an impossible id.
    let origin = (command.origin_worker_id != 0)
        .then(|| WorkerId::new(command.origin_worker_id));
    let (scope, kind) = match command.action {
        WorkerCommandAction::Start => (
            OperationScope::Slot(WorkerId::new(command.worker_id)),
            OperationKind::Start,
        ),
        WorkerCommandAction::Stop => (
            OperationScope::Slot(WorkerId::new(command.worker_id)),
            OperationKind::Stop,
        ),
        WorkerCommandAction::Reload => (
            OperationScope::Slot(WorkerId::new(command.worker_id)),
            OperationKind::Reload,
        ),
        WorkerCommandAction::Restart => (
            OperationScope::Slot(WorkerId::new(command.worker_id)),
            OperationKind::Restart,
        ),
        WorkerCommandAction::ReloadAll => {
            (OperationScope::Fleet, OperationKind::ReloadAll)
        }
        WorkerCommandAction::RestartAll => {
            (OperationScope::Fleet, OperationKind::RestartAll)
        }
        WorkerCommandAction::Resize => (
            OperationScope::Fleet,
            OperationKind::Resize {
                from: command.expected_worker_count,
                to: command.target_worker_count,
            },
        ),
    };
    let guard = match crate::management::submit::acquire(
        control,
        scope,
        kind,
        OperationOrigin::Management {
            via_worker: origin,
            command: command.command,
        },
    ) {
        Ok(guard) => guard,
        Err(refusal) => return refusal.into(),
    };
    let operation = guard.operation();
    info!(%operation, "worker management operation accepted");

    let supervisor = Arc::clone(control);
    let state = state.clone();
    let id = operation.id;
    let submitted = crate::management::submit::enqueue_operation(
        control,
        guard,
        async move {
            match command.action {
                WorkerCommandAction::Start => {
                    crate::management::actions::worker::start_worker(
                        &supervisor,
                        WorkerId::new(command.worker_id),
                        &state,
                    )
                    .await
                }
                WorkerCommandAction::Stop => {
                    crate::management::actions::worker::stop_worker(
                        &supervisor,
                        WorkerId::new(command.worker_id),
                        &state,
                        id,
                    )
                    .await
                }
                WorkerCommandAction::Reload => {
                    crate::management::actions::reload::reload_worker(
                        &supervisor,
                        WorkerId::new(command.worker_id),
                        &state,
                    )
                    .await
                }
                WorkerCommandAction::Restart => {
                    crate::management::actions::worker::restart_worker_slot(
                        &supervisor,
                        WorkerId::new(command.worker_id),
                        &state,
                        id,
                    )
                    .await
                }
                WorkerCommandAction::ReloadAll => {
                    crate::management::actions::reload::reload_all_workers(
                        &supervisor,
                        &state,
                    )
                    .await
                }
                WorkerCommandAction::RestartAll => {
                    crate::management::actions::restart::restart_all_workers(
                        &supervisor,
                        &state,
                        origin,
                        id,
                    )
                    .await
                }
                WorkerCommandAction::Resize => {
                    crate::management::actions::resize::resize_workers(
                        &supervisor,
                        &state,
                        command.expected_worker_count,
                        command.target_worker_count,
                        id,
                    )
                    .await
                }
            }
        },
    );
    match submitted {
        Ok(()) => SubmitResult::Accepted(operation.id),
        Err(reason) => SubmitResult::Rejected(reason),
    }
}
