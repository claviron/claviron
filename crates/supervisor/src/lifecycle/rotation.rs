#[cfg(feature = "metrics")]
use std::sync::atomic::Ordering;

use claviron_runtime::{AppState, WorkerId};
use tracing::{info, warn};

use crate::Error;
use crate::control::WorkerControl;
use crate::lifecycle::{
    DrainReason, ExitDisposition, OperationId, WorkerIntent,
};

/// Gracefully rotate a single worker without overlapping process
/// incarnations for the same worker id.
///
/// [INCARNATION_MASKING]: replace the process, not the logical worker number.
///
/// [INCARNATION_MASKING]: https://github.com/claviron/claviron/blob/main/docs/design/INCARNATION_MASKING.md
pub(crate) async fn rotate_worker(
    control: &WorkerControl,
    worker_id: WorkerId,
    state: &AppState,
    operation: OperationId,
) -> claviron_runtime::registry::Result<()> {
    info!("🔄 Rotating worker #{}...", worker_id);

    let worker_count = state.worker_count();

    // A per-node Orbit lane has one active process writer. The old process
    // stays in its row while it drains; only the intent says it is leaving.
    let (identity, incarnation, reload_revision) = {
        let mut workers = control.workers.write().await;
        let Some(worker) =
            workers.iter_mut().find(|worker| worker.id == worker_id)
        else {
            warn!("Worker #{} not found for rotation", worker_id);
            return Ok(());
        };
        if !worker.is_running() {
            return Ok(());
        }
        let identity = worker
            .process
            .as_ref()
            .expect("running worker has no process")
            .identity();
        worker.begin_drain(DrainReason::Rotation, ExitDisposition::Replace);
        if let Err(error) = crate::lifecycle::drain::request_drain(
            control,
            state,
            worker,
            operation,
            DrainReason::Rotation,
        ) {
            worker.set_intent(WorkerIntent::EnsureRunning);
            return Err(error.into());
        }
        (identity, worker.incarnation, worker.reload_revision)
    };
    let old_pid = identity.pid();

    if let Err(error) = crate::lifecycle::drain::await_slot_exit(
        control, worker_id, identity, "rotation",
    )
    .await
    {
        // We could not prove that the old process exited. Restore normal
        // health monitoring; it will either keep the live worker or
        // classify and restart it after an eventual exit.
        crate::lifecycle::drain::restore_intent(
            control,
            worker_id,
            WorkerIntent::EnsureRunning,
        )
        .await;
        return Err(error.into());
    }
    if control.is_shutting_down() {
        // The exit is recorded as the fleet shutdown's; the slot rests.
        return Err(Error::service(format!(
            "rotation of worker #{worker_id} ended by shutdown; no replacement is spawned"
        ))
        .into());
    }

    let mut workers = control.workers.write().await;
    let mut new_worker = match crate::process::spawn::spawn_worker(
        control,
        worker_id,
        incarnation.saturating_add(1),
        worker_count,
        state,
    ) {
        Ok(worker) => worker,
        Err(error) => {
            if let Some(worker) =
                workers.iter_mut().find(|worker| worker.id == worker_id)
            {
                worker.mark_failed();
            }
            return Err(error.into());
        }
    };
    new_worker.reload_revision = reload_revision;
    let new_pid = new_worker.pid().expect("new worker has no process");
    let worker = workers
        .iter_mut()
        .find(|worker| worker.id == worker_id)
        .expect("rotating worker table row disappeared");
    *worker = new_worker;
    drop(workers);
    control.changed.notify_waiters();

    #[cfg(feature = "metrics")]
    control.worker_rotations_total.fetch_add(1, Ordering::Relaxed);
    info!(
        "✅ Worker #{} rotated (old PID: {}, new PID: {})",
        worker_id, old_pid, new_pid
    );

    Ok(())
}
