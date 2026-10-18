use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use claviron_orbit::{OrbitEpoch, OrbitStateExt};
use claviron_runtime::{AppState, WorkerId};
use claviron_sys::process::ProcessIdentity;
use nix::sys::signal::Signal;
use tracing::{debug, error, info, warn};

use crate::control::{BlockedWorker, WorkerControl, WorkerProcess};
use crate::lifecycle::{
    DrainReason, ExitObservation, ExitOutcome, OperationId, OperationScope,
    OwnedProcess, ProcessOrigin, WorkerIntent, WorkerPhase,
};
use crate::presence::latest_worker_presence;
use crate::shutdown::{
    WorkerDrainAcknowledged, WorkerDrainRequested, WorkerTeardownCompleted,
};
use crate::{Error, Result};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShutdownExitPolicy {
    Wait,
    LeaveRunning,
}

/// A finite operator deadline authorizes the existing wait/escalation path for
/// adopted processes too. With no deadline, only children spawned by this
/// master are part of its unbounded exit barrier; external-origin processes
/// are asked to drain but are neither killed nor allowed to pin the master.
pub(crate) const fn shutdown_exit_policy(
    origin: Option<ProcessOrigin>,
    shutdown_timeout: Option<Duration>,
) -> ShutdownExitPolicy {
    match (origin, shutdown_timeout) {
        (Some(ProcessOrigin::Spawned), _)
        | (Some(ProcessOrigin::Adopted), Some(_)) => ShutdownExitPolicy::Wait,
        (Some(ProcessOrigin::Adopted), None) | (None, _) => {
            ShutdownExitPolicy::LeaveRunning
        }
    }
}

fn process_is_gone(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
        || error.raw_os_error() == Some(nix::libc::ESRCH)
}

/// Ask the slot's exact process to leave, through the fleet control ring.
///
/// The request names the complete worker identity and the operation it
/// belongs to; the worker acts only when every field is its own and
/// answers with an acknowledgement that the master records on the slot.
/// Direct `SIGTERM` is not the lifecycle transport — an embedded runtime
/// may own the process-wide handler, and the 2026-09-13 worker never saw
/// the signal its master sent — and there is no fallback to it: a request
/// that is not acknowledged leaves the slot draining and visible, which is
/// what the blocked-shutdown report and the snapshot then say.
pub(crate) fn request_drain(
    control: &WorkerControl,
    state: &AppState,
    worker: &WorkerProcess,
    operation: OperationId,
    reason: DrainReason,
) -> Result<()> {
    let target = worker.identity(control.master()?).ok_or_else(|| {
        Error::service(format!("Worker #{} has no process to drain", worker.id))
    })?;
    state.events().emit(WorkerDrainRequested { operation, target, reason });
    debug!(
        worker_id = %worker.id,
        pid = target.process().pid(),
        %operation,
        ?reason,
        "worker drain requested through Orbit control event"
    );
    Ok(())
}

/// Whether `operation` may act on `worker_id` right now: the operation
/// that owns the slot, the fleet-wide one that owns every slot, or the
/// fleet shutdown that outranks both.
pub(crate) fn operation_targets(
    control: &WorkerControl,
    worker_id: WorkerId,
    operation: OperationId,
) -> bool {
    control
        .operations
        .slot_operation(worker_id)
        .is_some_and(|active| active.id == operation)
        || control
            .operations
            .fleet_operation()
            .is_some_and(|active| active.id == operation)
        || control
            .operations
            .fleet_shutdown()
            .is_some_and(|active| active.id == operation)
}

/// Ask every live worker, one exact identity at a time, to leave for the
/// fleet shutdown. The latch and the intents are already committed; this
/// only carries the request. Returns how many were asked.
pub(crate) async fn request_fleet_drain(
    control: &WorkerControl,
    state: &AppState,
) -> usize {
    let shutdown = control.operations.begin_fleet_shutdown();
    let workers = control.workers.read().await;
    let mut requested = 0;
    for worker in workers.iter().filter(|worker| worker.phase().is_live()) {
        match crate::lifecycle::drain::request_drain(
            control,
            state,
            worker,
            shutdown.id,
            DrainReason::Shutdown,
        ) {
            Ok(()) => requested += 1,
            Err(error) => {
                warn!(worker_id = %worker.id, %error, "fleet shutdown could not request a drain")
            }
        }
    }
    requested
}

/// Record that the exact worker named acknowledged the drain of an
/// operation that owns its slot. Anything else — another identity, a
/// stale incarnation, an operation that is not running — changes nothing.
pub(crate) async fn drain_acknowledged(
    control: &WorkerControl,
    event: &WorkerDrainAcknowledged,
) -> bool {
    let Ok(master) = control.master() else {
        return false;
    };
    if !crate::lifecycle::drain::operation_targets(
        control,
        event.worker.worker_id(),
        event.operation,
    ) {
        return false;
    }
    let mut workers = control.workers.write().await;
    let Some(worker) = workers.iter_mut().find(|worker| {
        worker.identity(master) == Some(event.worker)
            && matches!(
                worker.phase(),
                WorkerPhase::Starting
                    | WorkerPhase::Running
                    | WorkerPhase::Reloading
            )
    }) else {
        return false;
    };
    worker.set_phase(WorkerPhase::DrainAcknowledged);
    true
}

/// Record that the exact worker named finished its managed teardown. The
/// process still exists until the kernel says otherwise; this changes the
/// phase, not the intent, and never reaps.
pub(crate) async fn teardown_completed(
    control: &WorkerControl,
    event: &WorkerTeardownCompleted,
) -> bool {
    let Ok(master) = control.master() else {
        return false;
    };
    let mut workers = control.workers.write().await;
    let Some(worker) = workers.iter_mut().find(|worker| {
        worker.identity(master) == Some(event.worker)
            && worker.phase().is_live()
    }) else {
        return false;
    };
    let solicited = match event.operation {
        Some(operation) => crate::lifecycle::drain::operation_targets(
            control, worker.id, operation,
        ),
        // Nobody asked through the protocol: the worker is leaving on its
        // own or for the whole-fleet shutdown. Believe it only when the
        // slot was told to go.
        None => worker.intent().is_drain() || control.is_shutting_down(),
    };
    if !solicited || matches!(worker.phase(), WorkerPhase::TeardownCompleted) {
        return false;
    }
    worker.set_phase(WorkerPhase::TeardownCompleted);
    true
}

/// Cleanup helper used when startup policy fails.
pub(crate) async fn cleanup_failed_boot(
    control: &WorkerControl,
    state: &AppState,
) {
    if control.workers.read().await.is_empty() {
        return;
    }

    warn!("startup failed: terminating spawned workers");
    crate::lifecycle::drain::shutdown_workers(control, state).await;
    crate::lifecycle::drain::wait_workers(control).await;
    // Ownership rows survive until non-running resource finalization.
}

/// Put an intent back after a drain whose exit could not be proven. Only a
/// still-draining slot is touched: an exit recorded meanwhile stands.
pub(crate) async fn restore_intent(
    control: &WorkerControl,
    worker_id: WorkerId,
    intent: WorkerIntent,
) {
    let mut workers = control.workers.write().await;
    if let Some(worker) =
        workers.iter_mut().find(|worker| worker.id == worker_id)
        && worker.intent().is_drain()
    {
        worker.set_intent(intent);
    }
}

/// Wait for every remaining worker process in this master's exit barrier
/// to be gone, then reap them.
///
/// Nothing polls a handle here. Each process has one exit observer, and
/// this awaits the observations; the reap happens afterwards under the
/// table lock, once the kernel has said each identity is gone. The handles
/// stay in their rows throughout, so status can be read and other slots
/// acted on while this waits. During an unbounded fleet shutdown, adopted
/// processes are deliberately outside this barrier: they were not spawned
/// by this master and are left alive if their requested drain does not end
/// them.
pub(crate) async fn wait_workers(control: &WorkerControl) {
    // Re-collected after every wait: `WorkersExited` is emitted on this
    // returning, so it returns only when no identity in this master's exit
    // barrier is alive — not merely when the set it first saw has gone.
    loop {
        let detach_external = control.is_shutting_down()
            && control.config().shutdown.timeout.is_none();
        let pending: Vec<(WorkerId, Arc<ExitObservation>)> = {
            let workers = control.workers.read().await;
            workers
                .iter()
                .filter_map(|worker| {
                    worker.process.as_ref().and_then(|process| {
                        (!detach_external || !process.is_adopted())
                            .then(|| (worker.id, process.exit()))
                    })
                })
                .filter(|(_, exit)| !exit.is_exited())
                .collect()
        };
        if pending.is_empty() {
            break;
        }
        debug!("⏳ Master waiting for {} worker(s) to exit...", pending.len());
        for (_, exit) in &pending {
            exit.exited().await;
        }
    }
    crate::process::health::reap_exited(control).await;
    let adopted_left_running = if control.is_shutting_down()
        && control.config().shutdown.timeout.is_none()
    {
        control
            .workers
            .read()
            .await
            .iter()
            .filter_map(|worker| worker.process.as_ref())
            .filter(|process| process.is_adopted() && !process.has_exited())
            .count()
    } else {
        0
    };
    if adopted_left_running != 0 {
        warn!(
            adopted_workers_left_running = adopted_left_running,
            "master exit barrier completed; adopted workers remain alive by origin policy"
        );
    }
}

/// Every identity that still belongs to this master's exit barrier, with
/// what is known about why. Empty once the fleet may be reported exited.
pub(crate) async fn blocked_workers(
    control: &WorkerControl
) -> Vec<BlockedWorker> {
    let workers = control.workers.read().await;
    workers
        .iter()
        .filter_map(|worker| {
            let (identity, alive_for, operation) = if let Some(process) =
                worker.process.as_ref().filter(|process| {
                    !process.has_exited()
                        && shutdown_exit_policy(
                            Some(process.origin()),
                            control.config().shutdown.timeout,
                        ) == ShutdownExitPolicy::Wait
                }) {
                (
                    process.identity(),
                    process.age(),
                    control.operations.slot_operation(worker.id),
                )
            } else {
                let presence = match worker.phase() {
                    WorkerPhase::OwnershipPending { presence, .. }
                    | WorkerPhase::ReconciliationBlocked { presence, .. } => {
                        presence
                    }
                    _ => return None,
                };
                if !presence.exact_process_alive() {
                    return None;
                }
                (
                    presence.identity().process(),
                    Duration::from_millis(
                        OrbitEpoch::now().as_unix_ms().saturating_sub(
                            presence.runtime_started_at_unix_ms(),
                        ),
                    ),
                    presence
                        .active_operation_id()
                        .and_then(OperationId::from_raw)
                        .and_then(|id| {
                            control
                                .operations
                                .active()
                                .into_iter()
                                .find(|op| op.id == id)
                        }),
                )
            };
            Some(BlockedWorker {
                worker_id: worker.id,
                identity,
                phase: worker.phase(),
                intent: worker.intent(),
                operation,
                alive_for,
            })
        })
        .collect()
}

/// The periodic word on a shutdown that `shutdown_timeout: None` lets wait:
/// one line per live identity, so the blocking worker, its exact process,
/// its state and the operation on it can be read from the master's log.
pub(crate) async fn log_blocked_shutdown(
    control: &WorkerControl,
    elapsed: Duration,
) {
    let blocked = crate::lifecycle::drain::blocked_workers(control).await;
    warn!(
        elapsed = %humantime::format_duration(Duration::from_secs(elapsed.as_secs())),
        fleet_operation = ?control.operations.active().iter().find(|op| op.scope == OperationScope::Fleet),
        "⏳ fleet shutdown is blocked by {} live worker(s); waiting, not killing (shutdown_timeout: None)",
        blocked.len()
    );
    for worker in blocked {
        warn!("   blocked by {worker}");
    }
}

/// Drain every worker with a direct `SIGTERM`. Only for the two paths on
/// which the fleet control ring cannot carry a request: startup rollback,
/// when the children may not have installed their listeners yet, and the
/// bootstrap's fallback after the master runtime has already stopped. The
/// normal shutdown path uses [`Self::request_fleet_drain`].
pub(crate) async fn shutdown_workers(
    control: &WorkerControl,
    state: &AppState,
) {
    crate::lifecycle::drain::shutdown_workers_inner(control, state, true).await;
}

/// Wait for workers already asked to leave through targeted drain requests.
/// Sending SIGTERM as well is unsafe when an embedded runtime has installed
/// a process-wide handler from one of its execution threads.
pub(crate) async fn shutdown_workers_after_drain_requests(
    control: &WorkerControl,
    state: &AppState,
) {
    crate::lifecycle::drain::shutdown_workers_inner(control, state, false)
        .await;
}

pub(crate) async fn shutdown_workers_inner(
    control: &WorkerControl,
    state: &AppState,
    signal: bool,
) {
    // The latch and the slot intents may already be committed by
    // `initiate_shutdown`; the bootstrap fallback path reaches here without
    // it. Either way only the first caller drives the drain.
    control.begin_fleet_shutdown().await;
    if control.shutdown_driven.swap(true, Ordering::AcqRel) {
        debug!(
            "worker shutdown already in progress; skipping duplicate shutdown request"
        );
        return;
    }

    // Get graceful shutdown timeout from supervisor config.
    // - Some(timeout): wait up to timeout, then force-kill remaining managed workers.
    // - None: wait indefinitely for this master's child workers; adopted
    //   and unowned processes remain alive outside its exit barrier.
    let shutdown_timeout: Option<Duration> = control.config().shutdown.timeout;

    // Phase 1: collect every live slot's exit observation, signalling if
    // asked. The intents were set with the latch. The normal
    // server-shutdown path has already requested each worker's drain over
    // Orbit and must not also enter an embedded runtime's process-wide
    // POSIX handler.
    let exits: Vec<Arc<ExitObservation>> = {
        let workers = control.workers.read().await;
        if workers.is_empty() {
            return;
        }
        info!("🔸 Master terminating {} worker(s)...", workers.len());
        let mut exits = Vec::new();
        for worker in workers.iter().filter(|worker| worker.phase().is_live()) {
            let policy = shutdown_exit_policy(
                worker.process.as_ref().map(OwnedProcess::origin),
                shutdown_timeout,
            );
            if signal
                && policy == ShutdownExitPolicy::Wait
                && let Some(pid) =
                    crate::process::health::signal_worker_if_alive(
                        worker,
                        Signal::SIGTERM,
                    )
            {
                debug!(
                    "Sending SIGTERM to worker #{} (PID {})",
                    worker.id, pid
                );
            }
            if let Some(process) = worker.process.as_ref() {
                match policy {
                    ShutdownExitPolicy::Wait => exits.push(process.exit()),
                    ShutdownExitPolicy::LeaveRunning if signal => warn!(
                        worker_id = %worker.id,
                        pid = process.pid(),
                        start_stamp = process.identity().started_at(),
                        "adopted worker is outside this master's unbounded exit barrier; leaving the exact process alive without an implicit signal"
                    ),
                    ShutdownExitPolicy::LeaveRunning => warn!(
                        worker_id = %worker.id,
                        pid = process.pid(),
                        start_stamp = process.identity().started_at(),
                        "adopted worker was asked to drain but will not block this unbounded shutdown; leaving the exact process alive"
                    ),
                }
            }
        }
        for worker in
            workers.iter().filter(|worker| worker.phase().is_unowned())
        {
            let presence = match worker.phase() {
                WorkerPhase::OwnershipPending { presence, .. }
                | WorkerPhase::ReconciliationBlocked { presence, .. } => {
                    Some(presence)
                }
                WorkerPhase::Unclaimed { .. } => {
                    state.fleet().and_then(|fleet| {
                        latest_worker_presence(&fleet, worker.id)
                    })
                }
                _ => None,
            };
            if let Some(identity) = presence
                .filter(|presence| presence.exact_process_alive())
                .map(|presence| presence.identity().process())
            {
                warn!(
                    worker_id = %worker.id,
                    pid = identity.pid(),
                    start_stamp = identity.started_at(),
                    "unowned worker identity will not be signalled or included in the master exit barrier; leaving the exact process alive"
                );
            }
        }
        exits
    };

    // Phase 2: wait for the kernel to report every barrier identity gone.
    match shutdown_timeout {
        Some(timeout) => {
            info!(
                "⏳ Waiting up to {:?} for workers to exit gracefully...",
                timeout
            );
        }
        None => {
            info!(
                "⏳ Waiting indefinitely for this master's child workers to exit gracefully (shutdown_timeout = None)..."
            );
        }
    }
    let start = Instant::now();
    let mut all_exited = std::pin::pin!(async {
        for exit in &exits {
            exit.exited().await;
        }
    });
    match shutdown_timeout {
        None => loop {
            tokio::select! {
                () = &mut all_exited => break,
                // For indefinite graceful waits, say periodically who is
                // being waited for. Diagnostic only: no elapsed time here
                // creates termination authority.
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    crate::lifecycle::drain::log_blocked_shutdown(control, start.elapsed()).await;
                }
            }
        },
        Some(timeout) => {
            tokio::select! {
                () = &mut all_exited => {}
                () = tokio::time::sleep(timeout) => {
                    // Phase 3: the operator's deadline authorizes SIGKILL for
                    // whatever is still there.
                    let killed = crate::process::health::signal_alive_workers(control, Signal::SIGKILL).await;
                    for (worker_id, pid) in &killed {
                        error!(
                            "⚠️  ⚠️  ⚠️  FORCE KILLING worker #{} (PID {}) - DID NOT RESPOND TO SIGTERM!",
                            worker_id, pid
                        );
                        error!(
                            "    Worker #{} ignored graceful shutdown for {:?} - sending SIGKILL!",
                            worker_id, timeout
                        );
                    }
                    if !killed.is_empty() {
                        error!(
                            "💥 FORCE KILLED {} worker(s) that did not respond to graceful shutdown!",
                            killed.len()
                        );
                        error!("   This indicates workers may be stuck/hanging - investigate logs!");
                    }
                    all_exited.await;
                }
            }
        }
    }

    crate::process::health::reap_exited(control).await;
    if crate::lifecycle::drain::blocked_workers(control).await.is_empty() {
        info!(
            "✅ All workers in this master's shutdown barrier exited gracefully"
        );
    }
}

/// Wait for the exact process a slot is draining to be gone, then reap it
/// and record the exit.
///
/// The handle never leaves the table: the row keeps its process and PID for
/// as long as the process exists, every waiter observes the same
/// [`ExitObservation`], and the reap happens here, under the table lock,
/// once the kernel has confirmed the identity is dead.
///
/// `shutdown_timeout` is the operator's policy: `Some(d)` authorizes one
/// `SIGKILL` to the still-live identity after `d`; `None` waits for as long
/// as it takes. Nothing else here is a deadline; see
/// [GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION][graceful-ownership].
///
/// [graceful-ownership]: https://github.com/claviron/claviron/blob/main/docs/design/GRACEFUL_OWNERSHIP.md
pub(crate) async fn await_slot_exit(
    control: &WorkerControl,
    worker_id: WorkerId,
    identity: ProcessIdentity,
    operation: &'static str,
) -> Result<ExitOutcome> {
    let exit = {
        let workers = control.workers.read().await;
        crate::lifecycle::drain::draining_process(
            &workers, worker_id, identity, operation,
        )?
        .exit()
    };
    let timeout = control.config().shutdown.timeout;

    match timeout {
        None => exit.exited().await,
        Some(timeout) => {
            tokio::select! {
                () = exit.exited() => {}
                () = tokio::time::sleep(timeout) => {
                    {
                        let workers = control.workers.read().await;
                        let process =
                            crate::lifecycle::drain::draining_process(&workers, worker_id, identity, operation)?;
                        match process.signal(Signal::SIGKILL) {
                            Ok(()) => error!(
                                "Worker #{} (PID {}) exceeded {} drain timeout {:?}; sent SIGKILL",
                                worker_id, identity.pid(), operation, timeout
                            ),
                            Err(error) if process_is_gone(&error) => {}
                            Err(error) => {
                                return Err(Error::service(format!(
                                    "failed to force-kill worker #{} during {}: {}",
                                    worker_id, operation, error
                                )));
                            }
                        }
                    }
                    exit.exited().await;
                }
            }
        }
    }

    let mut workers = control.workers.write().await;
    let worker =
        workers.iter_mut().find(|worker| worker.id == worker_id).ok_or_else(
            || Error::service(format!("Worker #{worker_id} disappeared")),
        )?;
    let process = worker
        .process
        .as_mut()
        .filter(|process| process.identity() == identity)
        .ok_or_else(|| {
            Error::service(format!(
                "Worker #{worker_id} no longer holds PID {} during {operation}",
                identity.pid()
            ))
        })?;
    let status = process.try_reap().map_err(|error| {
        Error::service(format!(
            "failed to reap worker #{worker_id} during {operation}: {error}"
        ))
    })?;
    debug!(
        "Worker #{} (PID {}) exited for {} with status: {}",
        worker_id,
        identity.pid(),
        operation,
        status.map_or_else(
            || "unreaped".to_string(),
            |status| status.to_string()
        )
    );
    Ok(worker.record_exit())
}

pub(crate) fn draining_process<'a>(
    workers: &'a [WorkerProcess],
    worker_id: WorkerId,
    identity: ProcessIdentity,
    operation: &'static str,
) -> Result<&'a OwnedProcess> {
    workers
        .iter()
        .find(|worker| worker.id == worker_id)
        .and_then(|worker| worker.process.as_ref())
        .filter(|process| process.identity() == identity)
        .ok_or_else(|| {
            Error::service(format!(
                "Worker #{worker_id} no longer holds PID {} during {operation}",
                identity.pid()
            ))
        })
}
