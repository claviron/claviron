use claviron_runtime::{AppState, WorkerId};
use tracing::info;

use crate::control::WorkerControl;
use crate::lifecycle::{
    DrainReason, ExitDisposition, OperationId, WorkerIntent, WorkerPhase,
};
use crate::{Error, Result};

/// Replace one configured worker slot without exposing the stop/start race
/// to callers. The old child is completely drained and reaped before the
/// replacement is spawned. Boot confirmation remains asynchronous and is
/// reported through the authoritative management snapshot.
pub(crate) async fn restart_worker_slot(
    control: &WorkerControl,
    worker_id: WorkerId,
    state: &AppState,
    operation: OperationId,
) -> Result<()> {
    let (running, lifecycle) = control
        .workers
        .read()
        .await
        .iter()
        .find(|worker| worker.id == worker_id)
        .map(|worker| (worker.is_running(), worker.lifecycle()))
        .ok_or_else(|| {
            Error::service(format!("Worker #{worker_id} not found"))
        })?;
    if !running {
        return Err(Error::service(format!(
            "Worker #{worker_id} is {}, not running",
            lifecycle.as_str()
        )));
    }

    crate::management::actions::worker::stop_worker(
        control, worker_id, state, operation,
    )
    .await?;
    if state.is_shutting_down() || control.is_shutting_down() {
        return Err(Error::service(format!(
            "restart of worker #{worker_id} was interrupted by shutdown after its old process stopped"
        )));
    }
    if let Err(error) = crate::management::actions::worker::start_worker(
        control, worker_id, state,
    )
    .await
    {
        let mut workers = control.workers.write().await;
        if let Some(worker) =
            workers.iter_mut().find(|worker| worker.id == worker_id)
        {
            worker.mark_failed();
        }
        return Err(error);
    }
    Ok(())
}

pub(crate) async fn start_worker(
    control: &WorkerControl,
    worker_id: WorkerId,
    state: &AppState,
) -> Result<()> {
    let (phase, intent, incarnation, reload_revision) = control
        .workers
        .read()
        .await
        .iter()
        .find(|worker| worker.id == worker_id)
        .map(|worker| {
            (
                worker.phase(),
                worker.intent(),
                worker.incarnation,
                worker.reload_revision,
            )
        })
        .ok_or_else(|| {
            Error::service(format!("Worker #{worker_id} not found"))
        })?;
    if intent.is_drain() {
        return Err(Error::service(format!(
            "Worker #{worker_id} is still stopping"
        )));
    }
    if phase.is_live() {
        return Ok(());
    }
    if phase.is_unowned() {
        // Its lane is still being watched; a second process would overlap
        // whatever may be publishing there.
        return Err(Error::service(format!(
            "Worker #{worker_id} is unclaimed"
        )));
    }

    let worker_count = state.worker_count();
    let mut workers = control.workers.write().await;
    let mut new_worker = crate::process::spawn::spawn_worker(
        control,
        worker_id,
        incarnation.saturating_add(1),
        worker_count,
        state,
    )?;
    new_worker.reload_revision = reload_revision;
    let worker =
        workers.iter_mut().find(|worker| worker.id == worker_id).ok_or_else(
            || Error::service(format!("Worker #{worker_id} not found")),
        )?;
    *worker = new_worker;
    drop(workers);
    control.changed.notify_waiters();
    info!(%worker_id, "worker start requested by management API");
    Ok(())
}

pub(crate) async fn stop_worker(
    control: &WorkerControl,
    worker_id: WorkerId,
    state: &AppState,
    operation: OperationId,
) -> Result<()> {
    let identity = {
        let mut workers = control.workers.write().await;
        let running =
            workers.iter().filter(|worker| worker.is_serving()).count();
        let worker = workers
            .iter_mut()
            .find(|worker| worker.id == worker_id)
            .ok_or_else(|| {
            Error::service(format!("Worker #{worker_id} not found"))
        })?;
        if worker.is_stopped() {
            return Ok(());
        }
        if matches!(worker.phase(), WorkerPhase::Exited { .. }) {
            // Already gone; the operator only wants it to stay that way.
            worker.set_intent(WorkerIntent::Stopped);
            return Ok(());
        }
        if worker.phase().is_unowned() {
            return Err(Error::service(format!(
                "Worker #{worker_id} is unclaimed"
            )));
        }
        if worker.is_serving() && running <= 1 {
            return Err(Error::service(format!(
                "Worker #{worker_id} is the last running worker; refusing to stop it"
            )));
        }
        let identity = worker
            .process
            .as_ref()
            .ok_or_else(|| {
                Error::service(format!("Worker #{worker_id} has no process"))
            })?
            .identity();
        let previous_intent = worker
            .begin_drain(DrainReason::ManualStop, ExitDisposition::StayStopped);
        if let Err(error) = crate::lifecycle::drain::request_drain(
            control,
            state,
            worker,
            operation,
            DrainReason::ManualStop,
        ) {
            worker.set_intent(previous_intent);
            return Err(error);
        }
        info!(%worker_id, pid = identity.pid(), "worker stop requested by management API");
        identity
    };
    if let Err(error) = crate::lifecycle::drain::await_slot_exit(
        control,
        worker_id,
        identity,
        "manual stop",
    )
    .await
    {
        crate::lifecycle::drain::restore_intent(
            control,
            worker_id,
            WorkerIntent::EnsureRunning,
        )
        .await;
        return Err(error);
    }
    Ok(())
}
