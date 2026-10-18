//! Startup convergence and exact-child acknowledgement projection.
use std::collections::BTreeMap;
use std::time::Duration;

use claviron_runtime::{AppState, WorkerId};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::control::WorkerControl;
use crate::events::startup::{WorkerStarted, WorkersBootConfirmed};
use crate::lifecycle::{OwnedProcess, WorkerIntent, WorkerPhase};
use crate::{Error, Result};
const CHILD_EXIT_CHECK_INTERVAL: Duration = Duration::from_millis(100);
/// The registry installs acknowledgement handling before any child can spawn.
/// This waiter consumes table state, not a generation-local subscription.
pub(crate) async fn wait_workers_started(
    control: &WorkerControl,
    state: &AppState,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> Result<()> {
    let deadline =
        tokio::time::Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::service("startup_timeout exceeds the supported timer range")
        })?;
    // A slot still under watch is one boot has not finished deciding: its
    // lane either moves, and the id is conceded to whoever is publishing
    // it, or it holds still and the slot becomes a worker. Confirming boot
    // before that window closes would report a count the master has not
    // finished producing — and on an ordinary restart, where every lane is
    // still warm with this master's own last generation, that count is
    // zero. The window is bounded and control-closing, so waiting it out
    // needs no second deadline of its own.
    let watch_deadline =
        crate::lifecycle::reconcile::unclaimed_watch_deadline(control).await;
    let mut child_check = tokio::time::interval(CHILD_EXIT_CHECK_INTERVAL);
    child_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if cancellation.is_cancelled() || control.is_shutting_down() {
            return Err(Error::service(
                "worker startup interrupted by shutdown",
            ));
        }
        // Watched here rather than slept on: this loop is already ticking,
        // and a slot that frees up during it joins the boot it belongs to.
        crate::lifecycle::reconcile::reconcile_unclaimed_workers(
            control, state,
        )
        .await?;
        // Only process exit is sampled. Boot completion is event-driven;
        // heartbeat freshness does not participate here.
        let (_, dead) =
            crate::process::health::check_worker_health(control).await;
        if !dead.is_empty() {
            return Err(Error::service(format!(
                "worker startup failed: worker(s) exited early: {dead:?}"
            )));
        }
        // Read back each pass rather than tracked, so a worker spawned onto
        // a slot the watch has just released is waited for like any other.
        let pending = crate::lifecycle::startup::pending_boot(control).await;
        if pending.is_empty()
            && watch_deadline
                .is_none_or(|until| tokio::time::Instant::now() >= until)
        {
            let count =
                crate::lifecycle::startup::booted_count(control).await?;
            state.events().emit(WorkersBootConfirmed { count });
            info!(
                server_name = state.name(),
                workers = count,
                "✅ Worker boot confirmed for {count} workers through Orbit events"
            );
            return Ok(());
        }

        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(Error::service("worker startup interrupted by shutdown"));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(Error::service(format!("worker startup timed out after {timeout:?}; pending worker ids and PIDs: {pending:?}")));
            }
            _ = control.changed.notified() => {}
            _ = child_check.tick() => {}
        }
    }
}

/// Workers spawned but not yet confirmed, with the pid boot is waiting on.
pub(crate) async fn pending_boot(
    control: &WorkerControl
) -> BTreeMap<WorkerId, Option<u32>> {
    control
        .workers
        .read()
        .await
        .iter()
        .filter(|worker| !worker.intent().is_drain())
        .filter_map(|worker| match worker.phase() {
            WorkerPhase::Starting => Some((worker.id, worker.pid())),
            WorkerPhase::Unclaimed { .. } => Some((worker.id, None)),
            WorkerPhase::OwnershipPending { presence, .. } => {
                Some((worker.id, Some(presence.identity().process().pid())))
            }
            WorkerPhase::ReconciliationBlocked { presence, .. } => {
                Some((worker.id, Some(presence.identity().process().pid())))
            }
            _ => None,
        })
        .collect()
}

/// How many workers this boot actually produced — not how many were asked
/// for, and not how many slots the configuration names.
pub(crate) async fn booted_count(control: &WorkerControl) -> Result<u8> {
    let running = control
        .workers
        .read()
        .await
        .iter()
        .filter(|worker| worker.is_running())
        .count();
    u8::try_from(running).map_err(|_| {
        Error::service("worker startup count exceeds protocol limit")
    })
}

/// Only an event from the currently tracked child may complete its boot.
/// Duplicate events and events from retired PIDs have no effect.
pub(crate) async fn confirm_worker_started(
    control: &WorkerControl,
    event: WorkerStarted,
) -> bool {
    let mut workers = control.workers.write().await;
    let Some(worker) = workers.iter_mut().find(|worker| {
        worker.id == event.worker_id && worker.pid() == Some(event.pid)
    }) else {
        debug!(
            ?event,
            "ignoring startup event from an untracked worker incarnation"
        );
        return false;
    };
    if worker.phase() != WorkerPhase::Starting
        || worker.intent() != WorkerIntent::EnsureRunning
        || !matches!(
            worker.process.as_mut().map(OwnedProcess::try_reap),
            Some(Ok(None))
        )
    {
        return false;
    }
    control.observer.track_worker(worker.id, event.pid).await;
    worker.set_phase(WorkerPhase::Running);
    debug!(worker_id = %event.worker_id, pid = event.pid, "worker provider boot completed");
    true
}
