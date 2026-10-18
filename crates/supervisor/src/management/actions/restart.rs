//! Fleet-scope rolling worker replacement.
//!
//! A restart is not a new lifecycle state for one process: it is a graceful
//! drain followed by a new incarnation of the same worker slot. This module
//! owns the fleet ordering and readiness barrier that turn those primitives
//! into the `restart-workers` management operation.

use std::time::Duration;

use claviron_runtime::{AppState, WorkerId};
use tracing::{debug, info};

use crate::control::{WorkerControl, WorkerProcess};
use crate::lifecycle::{
    DrainReason, ExitDisposition, OperationId, WorkerIntent, WorkerPhase,
};
use crate::{Error, Result};

const CHILD_EXIT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Replace every running worker, one at a time. Stopped slots retain their
/// operator-selected state. The worker that carried the command is moved
/// to the end so its gRPC response has the longest possible flush window.
pub(crate) async fn restart_all_workers(
    control: &WorkerControl,
    state: &AppState,
    origin: Option<WorkerId>,
    operation: OperationId,
) -> Result<()> {
    let targets = restart_order(&control.workers.read().await, origin)?;

    info!(workers = ?targets, "rolling worker restart started");
    for worker_id in &targets {
        crate::management::actions::restart::restart_running_worker(
            control,
            state,
            *worker_id,
            state.worker_count(),
            operation,
        )
        .await?;
    }
    info!(workers = targets.len(), "rolling worker restart completed");
    Ok(())
}

pub(crate) async fn restart_running_worker(
    control: &WorkerControl,
    state: &AppState,
    worker_id: WorkerId,
    replacement_worker_count: u8,
    operation: OperationId,
) -> Result<()> {
    let (identity, incarnation, reload_revision) = {
        let mut workers = control.workers.write().await;
        let running =
            workers.iter().filter(|worker| worker.is_running()).count();
        if running < 2 {
            return Err(Error::service(format!(
                "FLEET_CONTINUITY requires another running worker before restarting worker #{worker_id}"
            )));
        }
        let worker = workers
            .iter_mut()
            .find(|worker| worker.id == worker_id)
            .ok_or_else(|| {
            Error::service(format!("Worker #{worker_id} not found"))
        })?;
        if !worker.is_running() {
            return Err(Error::service(format!(
                "Worker #{worker_id} is {}, not running",
                worker.lifecycle().as_str()
            )));
        }
        let identity = worker
            .process
            .as_ref()
            .ok_or_else(|| {
                Error::service(format!("Worker #{worker_id} has no process"))
            })?
            .identity();
        worker
            .begin_drain(DrainReason::RollingRestart, ExitDisposition::Replace);
        info!(%worker_id, old_pid = identity.pid(), "draining worker for rolling restart");
        if let Err(error) = crate::lifecycle::drain::request_drain(
            control,
            state,
            worker,
            operation,
            DrainReason::RollingRestart,
        ) {
            worker.set_intent(WorkerIntent::EnsureRunning);
            return Err(error);
        }
        (identity, worker.incarnation, worker.reload_revision)
    };
    let old_pid = identity.pid();

    if let Err(error) = crate::lifecycle::drain::await_slot_exit(
        control,
        worker_id,
        identity,
        "rolling restart",
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
    if control.is_shutting_down() {
        // The latch turned this drain into the fleet shutdown's; its exit
        // is recorded and the slot rests without a replacement.
        return Err(Error::service(format!(
            "rolling restart of worker #{worker_id} ended by shutdown; no replacement is spawned"
        )));
    }

    // A registry callback may run on another executor thread immediately.
    // Publish the new identity under the same lock that excludes its ack.
    let mut workers = control.workers.write().await;
    let mut new_worker = match crate::process::spawn::spawn_worker(
        control,
        worker_id,
        incarnation.saturating_add(1),
        replacement_worker_count,
        state,
    ) {
        Ok(worker) => worker,
        Err(error) => {
            if let Some(worker) =
                workers.iter_mut().find(|worker| worker.id == worker_id)
            {
                worker.mark_failed();
            }
            return Err(error);
        }
    };
    new_worker.reload_revision = reload_revision;
    let new_pid = new_worker.pid().expect("new worker has no process");
    {
        let worker = workers
            .iter_mut()
            .find(|worker| worker.id == worker_id)
            .expect("rolling restart worker row disappeared");
        *worker = new_worker;
    }
    drop(workers);
    control.changed.notify_waiters();

    crate::management::actions::restart::wait_for_replacement(
        control, state, worker_id, new_pid,
    )
    .await?;
    crate::observation::counters::record_worker_restart(control);
    info!(%worker_id, old_pid, new_pid, "worker replacement is running");
    Ok(())
}

pub(crate) async fn wait_for_replacement(
    control: &WorkerControl,
    state: &AppState,
    worker_id: WorkerId,
    expected_pid: u32,
) -> Result<()> {
    let timeout = control.config().startup.timeout;
    let deadline =
        tokio::time::Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::service("startup_timeout exceeds the supported timer range")
        })?;
    let mut child_check = tokio::time::interval(CHILD_EXIT_CHECK_INTERVAL);
    child_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        match crate::management::actions::restart::replacement_state(
            control,
            worker_id,
            expected_pid,
        )
        .await?
        {
            ReplacementState::Running => return Ok(()),
            ReplacementState::Starting => {}
        }
        if state.is_shutting_down() || control.is_shutting_down() {
            return Err(Error::service(format!(
                "rolling restart of worker #{worker_id} was interrupted by shutdown"
            )));
        }

        tokio::select! {
            biased;
            _ = state.on_shutdown() => {
                return Err(Error::service(format!(
                    "rolling restart of worker #{worker_id} was interrupted by shutdown"
                )));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(Error::service(format!(
                    "rolling restart timed out after {timeout:?} waiting for worker #{worker_id} PID {expected_pid} to complete boot"
                )));
            }
            _ = control.changed.notified() => {}
            _ = child_check.tick() => {}
        }
    }
}

async fn replacement_state(
    control: &WorkerControl,
    worker_id: WorkerId,
    expected_pid: u32,
) -> Result<ReplacementState> {
    let mut workers = control.workers.write().await;
    let worker =
        workers.iter_mut().find(|worker| worker.id == worker_id).ok_or_else(
            || Error::service(format!("Worker #{worker_id} disappeared")),
        )?;
    if worker.pid() != Some(expected_pid) {
        return Err(Error::service(format!(
            "Worker #{worker_id} changed incarnation while rolling restart awaited PID {expected_pid}"
        )));
    }
    match (worker.phase(), worker.intent()) {
        (WorkerPhase::Running, WorkerIntent::EnsureRunning) => {
            Ok(ReplacementState::Running)
        }
        (WorkerPhase::Starting, WorkerIntent::EnsureRunning) => {
            let process = worker
                .process
                .as_mut()
                .expect("starting worker has no process");
            match process.try_reap() {
                Ok(None) => Ok(ReplacementState::Starting),
                Ok(Some(status)) => {
                    worker.record_exit();
                    Err(Error::service(format!(
                        "Worker #{worker_id} PID {expected_pid} exited during rolling restart startup with status {status}"
                    )))
                }
                Err(error) => {
                    worker.record_exit();
                    Err(Error::service(format!(
                        "failed to inspect worker #{worker_id} PID {expected_pid} during rolling restart startup: {error}"
                    )))
                }
            }
        }
        _ => Err(Error::service(format!(
            "Worker #{worker_id} entered {} while rolling restart awaited startup",
            worker.lifecycle().as_str()
        ))),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplacementState {
    Starting,
    Running,
}

fn restart_order(
    workers: &[WorkerProcess],
    origin: Option<WorkerId>,
) -> Result<Vec<WorkerId>> {
    let mut targets = Vec::new();
    for worker in workers {
        if worker.is_running() {
            targets.push(worker.id);
        } else if !worker.is_stopped() {
            return Err(Error::service(format!(
                "rolling restart requires a stable fleet; worker #{} is {}",
                worker.id,
                worker.lifecycle().as_str()
            )));
        }
    }
    if targets.len() < 2 {
        return Err(Error::service(format!(
            "rolling restart requires at least two running workers; found {}",
            targets.len()
        )));
    }
    targets.sort_unstable();
    if let Some(index) =
        origin.and_then(|origin| targets.iter().position(|id| *id == origin))
    {
        let origin = targets.remove(index);
        targets.push(origin);
    }
    debug!(?origin, workers = ?targets, "planned rolling worker restart");
    Ok(targets)
}

#[cfg(test)]
#[path = "../../../tests/management/actions/restart.rs"]
mod tests;
