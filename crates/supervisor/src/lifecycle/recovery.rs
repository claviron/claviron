use std::time::{Duration, Instant};

use claviron_runtime::{AppState, WorkerId};
use tracing::{error, info, warn};

use crate::control::{WorkerControl, WorkerProcess};
use crate::lifecycle::{
    DrainReason, ExitDisposition, OperationId, WorkerIntent, WorkerPhase,
};
use crate::{Error, Result};

/// Drain the exact worker incarnation reported stale by the heartbeat
/// observer. Runs inside a `HeartbeatRecovery` operation on the slot, so no
/// operator command can replace the slot while it is being drained.
pub(crate) async fn drain_stale_worker(
    control: &WorkerControl,
    state: &AppState,
    worker_id: WorkerId,
    expected_pid: u32,
    heartbeat_timeout: Duration,
    operation: OperationId,
) -> Result<()> {
    let (identity, previous_intent) = {
        let mut workers = control.workers.write().await;
        let Some(worker) = workers.iter_mut().find(|worker| {
            worker.id == worker_id
                && worker.pid() == Some(expected_pid)
                && worker.is_serving()
        }) else {
            return Ok(());
        };
        let identity = worker
            .process
            .as_ref()
            .expect("serving worker has no process")
            .identity();
        let previous_intent = worker.begin_drain(
            DrainReason::HeartbeatTimeout,
            ExitDisposition::Replace,
        );
        warn!(
            proc_worker_id = %worker_id,
            pid = %expected_pid,
            // IMPORTANT: `hung (no heartbeat` is a stable operator-facing
            // marker — the substring an alert greps for. `scripts/hol-logs.sh`
            // used to be the consumer in the tree; it left with the
            // application and no script here greps it today, but the string
            // stays part of the contract for whatever does downstream.
            "⚠️  worker #{} hung (no heartbeat ≥ {:?}) → requesting drain",
            worker_id,
            heartbeat_timeout
        );
        if let Err(error) = crate::lifecycle::drain::request_drain(
            control,
            state,
            worker,
            operation,
            DrainReason::HeartbeatTimeout,
        ) {
            worker.set_intent(previous_intent);
            return Err(error);
        }
        (identity, previous_intent)
    };

    // A hung worker that had to be drained is a failure; the exit is
    // recorded as one and the health pass recovers it within the budget.
    if let Err(error) = crate::lifecycle::drain::await_slot_exit(
        control,
        worker_id,
        identity,
        "heartbeat recovery",
    )
    .await
    {
        crate::lifecycle::drain::restore_intent(
            control,
            worker_id,
            previous_intent,
        )
        .await;
        return Err(error);
    }
    Ok(())
}

/// Treat an accepted operator reload as a new worker recovery epoch.
///
/// This is called only after the replacement supervisor configuration has
/// loaded successfully. Clearing both fields lets a worker that exhausted
/// the previous epoch's retry budget be restarted immediately with the
/// corrected configuration instead of retaining its old backoff window.
pub(crate) async fn reset_worker_restart_budgets(
    control: &WorkerControl
) -> usize {
    let mut workers = control.workers.write().await;
    let mut reset = 0;

    for worker in workers.iter_mut() {
        if worker.recovery_attempts != 0 || worker.last_restart.is_some() {
            worker.recovery_attempts = 0;
            worker.last_restart = None;
            reset += 1;
        }
    }

    reset
}

/// The attempt count that still counts, given how long the worker has been
/// up since its last restart.
///
/// `recovery_attempts` was a lifetime total: a worker crashing once a week
/// was abandoned after `restart_max_attempts` weeks. An operator setting a
/// retry limit means "stop retrying one that keeps failing", and "keeps" is
/// a rate — so a worker that has stayed up for `restart_window` starts
/// clean, the way systemd's `StartLimitIntervalSec` works.
pub(crate) fn effective_attempts(
    recovery_attempts: u32,
    last_restart: Option<Instant>,
    window: Duration,
) -> u32 {
    match last_restart {
        Some(at) if at.elapsed() < window => recovery_attempts,
        _ => 0,
    }
}

/// Forgives a worker's restart history once it has been up for the window,
/// so the decay is recorded rather than only applied.
pub(crate) async fn decay_restart_history(
    control: &WorkerControl,
    worker_id: WorkerId,
) {
    let window = control.config().recovery.window;
    let mut workers = control.workers.write().await;
    let Some(worker) = workers.iter_mut().find(|worker| worker.id == worker_id)
    else {
        return;
    };
    if worker.recovery_attempts == 0
        || crate::lifecycle::recovery::effective_attempts(
            worker.recovery_attempts,
            worker.last_restart,
            window,
        ) != 0
    {
        return;
    }

    info!(
        "Worker #{} stayed up for {:?}; clearing {} earlier restart attempt(s)",
        worker_id, window, worker.recovery_attempts
    );
    worker.recovery_attempts = 0;
    worker.last_restart = None;
}

/// How long this worker still owes its restart backoff, if any.
///
/// Separate from [`restart_worker`](Self::restart_worker) so the wait
/// happens inside the slot's own recovery task. It used to be inside the
/// monitor under a lock every command needed, and with the defaults — 2s
/// doubling over ten attempts — that meant up to about seventeen minutes in
/// which no operator command could be answered.
///
/// The growth is capped at `restart_backoff_max` and computed with checked
/// arithmetic. `restart_backoff * multiplier.pow(attempts)` is arithmetic
/// on operator input: `u32::pow` overflows, `Duration * u32` overflows, and
/// both panic — in the master.
pub(crate) async fn pending_backoff(
    control: &WorkerControl,
    worker_id: WorkerId,
) -> Option<Duration> {
    let cfg = control.config();
    let (recovery_attempts, last_restart) = {
        let workers = control.workers.read().await;
        let worker = workers.iter().find(|w| w.id == worker_id)?;
        (worker.recovery_attempts, worker.last_restart?)
    };

    let recovery_attempts = crate::lifecycle::recovery::effective_attempts(
        recovery_attempts,
        Some(last_restart),
        cfg.recovery.window,
    );
    let backoff = cfg
        .recovery
        .backoff
        .multiplier
        .checked_pow(recovery_attempts)
        .and_then(|factor| cfg.recovery.backoff.initial.checked_mul(factor))
        .unwrap_or(cfg.recovery.backoff.max)
        .min(cfg.recovery.backoff.max);

    backoff.checked_sub(last_restart.elapsed()).filter(|wait| !wait.is_zero())
}

/// Restart a failed worker with backoff logic
pub(crate) async fn restart_worker(
    control: &WorkerControl,
    worker_id: WorkerId,
    state: &AppState,
) -> Result<()> {
    // Before the budget is consulted, not after: a worker that has been up
    // for the window is not on its eleventh attempt, it is on its first.
    crate::lifecycle::recovery::decay_restart_history(control, worker_id).await;

    let cfg = control.config();
    let max_attempts = cfg.recovery.max_attempts;

    // First, get worker info and check if we can restart
    let (recovery_attempts, incarnation, reload_revision) = {
        let workers = control.workers.read().await;

        let worker =
            workers.iter().find(|w| w.id == worker_id).ok_or_else(|| {
                Error::service(format!("Worker #{} not found", worker_id))
            })?;

        // Check max restart attempts
        if worker.recovery_attempts >= max_attempts {
            error!(
                "❌ Worker #{} exceeded max restart attempts ({}), giving up",
                worker_id, max_attempts
            );
            return Err(Error::service(format!(
                "Worker #{} exceeded max restart attempts",
                worker_id
            )));
        }

        (worker.recovery_attempts, worker.incarnation, worker.reload_revision)
    };

    // The backoff wait happened before this call, in the recovery task —
    // see `pending_backoff`. Nothing else waits on it.

    // Spawn new worker process
    info!(
        "🔄 Restarting worker #{} (attempt {}/{})",
        worker_id,
        recovery_attempts + 1,
        max_attempts
    );

    let worker_count = state.worker_count();
    let mut workers = control.workers.write().await;
    let new_worker = crate::process::spawn::spawn_worker(
        control,
        worker_id,
        incarnation.saturating_add(1),
        worker_count,
        state,
    )?;

    // Update the worker entry
    if let Some(worker) = workers.iter_mut().find(|w| w.id == worker_id) {
        *worker = WorkerProcess::new(
            worker_id,
            new_worker.process,
            WorkerPhase::Starting,
            WorkerIntent::EnsureRunning,
        )
        .with_incarnation(new_worker.incarnation)
        .with_reload_revision(reload_revision)
        .with_recovery(recovery_attempts + 1, Some(Instant::now()));
        crate::observation::counters::record_worker_restart(control);
    }
    drop(workers);
    control.changed.notify_waiters();

    Ok(())
}

/// Whether every one of `dead` has used up its restart budget for the
/// current recovery window.
pub(crate) async fn recovery_exhausted(
    control: &WorkerControl,
    dead: &[WorkerId],
) -> bool {
    let cfg = control.config();
    let workers = control.workers.read().await;
    dead.iter().all(|worker_id| {
        workers.iter().find(|worker| worker.id == *worker_id).is_some_and(
            |worker| {
                crate::lifecycle::recovery::effective_attempts(
                    worker.recovery_attempts,
                    worker.last_restart,
                    cfg.recovery.window,
                ) >= cfg.recovery.max_attempts
            },
        )
    })
}
