//! The worker drain protocol: a request that names one exact worker, the
//! worker's acknowledgement, and its teardown completion — three facts, none
//! of which is the process exiting.

use std::mem::size_of;

use claviron_orbit::{FleetEventCodec, FleetEventRegistry};
use claviron_runtime::{AppState, Event, RunMode};
use tracing::{debug, warn};

use crate::lifecycle::{
    DrainReason, OperationId, TeardownOutcome, WORKER_IDENTITY_BYTES,
    WorkerIdentity, WorkerIdentityProvider,
};
use crate::observer::HeartbeatObserver;
use crate::{Error, Result, WorkerControl};

const OPERATION_BYTES: usize = size_of::<u64>();

/// The master asks one exact worker process to leave.
///
/// The target is a complete [`WorkerIdentity`]; a worker acts only when every
/// field is its own. Direct `SIGTERM` is not the lifecycle transport: an
/// embedded runtime may own the process-wide handler, and on 2026-09-13 a
/// worker never saw the signal its master sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerDrainRequested {
    pub(crate) operation: OperationId,
    pub(crate) target: WorkerIdentity,
    pub(crate) reason: DrainReason,
}

impl Event for WorkerDrainRequested {}

impl FleetEventCodec for WorkerDrainRequested {
    const TOPIC: &'static str = "claviron.supervisor.worker.drain.requested.v2";

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload =
            Vec::with_capacity(OPERATION_BYTES + WORKER_IDENTITY_BYTES + 1);
        payload.extend_from_slice(&self.operation.raw().to_be_bytes());
        self.target.encode_into(&mut payload);
        payload.push(self.reason.code());
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> std::result::Result<Self, String> {
        let expected = OPERATION_BYTES + WORKER_IDENTITY_BYTES + 1;
        if bytes.len() != expected {
            return Err(format!(
                "invalid drain request payload length: expected {expected}, got {}",
                bytes.len()
            ));
        }
        let operation = decode_operation(&bytes[..OPERATION_BYTES])
            .ok_or_else(|| {
                "drain request requires an operation id".to_string()
            })?;
        let target = decode_identity(
            &bytes[OPERATION_BYTES..OPERATION_BYTES + WORKER_IDENTITY_BYTES],
        )?;
        let reason =
            DrainReason::from_code(bytes[expected - 1]).ok_or_else(|| {
                format!("unknown drain reason {}", bytes[expected - 1])
            })?;
        Ok(Self { operation, target, reason })
    }
}

/// The worker's runtime has taken the drain request: shutdown is initiated in
/// that process. Its work is being brought down, not gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerDrainAcknowledged {
    pub(crate) operation: OperationId,
    pub(crate) worker: WorkerIdentity,
}

impl Event for WorkerDrainAcknowledged {}

impl FleetEventCodec for WorkerDrainAcknowledged {
    const TOPIC: &'static str =
        "claviron.supervisor.worker.drain.acknowledged.v2";

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload =
            Vec::with_capacity(OPERATION_BYTES + WORKER_IDENTITY_BYTES);
        payload.extend_from_slice(&self.operation.raw().to_be_bytes());
        self.worker.encode_into(&mut payload);
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> std::result::Result<Self, String> {
        let expected = OPERATION_BYTES + WORKER_IDENTITY_BYTES;
        if bytes.len() != expected {
            return Err(format!(
                "invalid drain acknowledgement payload length: expected {expected}, got {}",
                bytes.len()
            ));
        }
        let operation = decode_operation(&bytes[..OPERATION_BYTES])
            .ok_or_else(|| {
                "drain acknowledgement requires an operation id".to_string()
            })?;
        let worker = decode_identity(&bytes[OPERATION_BYTES..])?;
        Ok(Self { operation, worker })
    }
}

/// The worker finished its managed teardown. It has not necessarily exited,
/// and individual finalizers may have failed while completing.
///
/// `operation` is the drain the worker acted on; `None` when it is leaving
/// for a reason nobody requested through the protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerTeardownCompleted {
    pub(crate) operation: Option<OperationId>,
    pub(crate) worker: WorkerIdentity,
    pub(crate) outcome: TeardownOutcome,
}

impl Event for WorkerTeardownCompleted {}

impl FleetEventCodec for WorkerTeardownCompleted {
    const TOPIC: &'static str =
        "claviron.supervisor.worker.teardown.completed.v2";

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload =
            Vec::with_capacity(OPERATION_BYTES + WORKER_IDENTITY_BYTES + 2);
        payload.extend_from_slice(
            &self.operation.map_or(0, OperationId::raw).to_be_bytes(),
        );
        self.worker.encode_into(&mut payload);
        let failed = match self.outcome {
            TeardownOutcome::Completed => 0,
            TeardownOutcome::FinalizersFailed { count } => count,
        };
        payload.extend_from_slice(&failed.to_be_bytes());
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> std::result::Result<Self, String> {
        let expected = OPERATION_BYTES + WORKER_IDENTITY_BYTES + 2;
        if bytes.len() != expected {
            return Err(format!(
                "invalid teardown completion payload length: expected {expected}, got {}",
                bytes.len()
            ));
        }
        let operation = decode_operation(&bytes[..OPERATION_BYTES]);
        let worker = decode_identity(
            &bytes[OPERATION_BYTES..OPERATION_BYTES + WORKER_IDENTITY_BYTES],
        )?;
        let failed =
            u16::from_be_bytes([bytes[expected - 2], bytes[expected - 1]]);
        let outcome = if failed == 0 {
            TeardownOutcome::Completed
        } else {
            TeardownOutcome::FinalizersFailed { count: failed }
        };
        Ok(Self { operation, worker, outcome })
    }
}

fn decode_operation(bytes: &[u8]) -> Option<OperationId> {
    OperationId::from_raw(u64::from_be_bytes(
        bytes.try_into().expect("8 bytes"),
    ))
}

fn decode_identity(
    bytes: &[u8]
) -> std::result::Result<WorkerIdentity, String> {
    WorkerIdentity::decode(bytes.try_into().expect("identity bytes"))
}

/// Publish the worker's teardown completion directly to the Orbit ring.
///
/// Call once, after all managed runnables have drained (or been aborted and
/// joined) and provider finalization has finished. Cancellation alone does not
/// satisfy that contract. Completion does not imply error-free teardown or OS
/// process exit; the master still reaps the child and retains its timeout policy.
///
/// This synchronous publication does not depend on a local event-forwarding task
/// getting another executor turn. It does not wait for a master acknowledgement.
pub fn publish_worker_teardown_completed(
    state: &AppState,
    failed_finalizers: usize,
) -> Result<()> {
    let RunMode::Worker(worker_id) = state.run_mode() else {
        return Err(Error::service("teardown completion requires worker mode"));
    };
    if !state.is_shutting_down() {
        return Err(Error::worker(
            worker_id,
            "shutdown has not been initiated",
        ));
    }
    let me = state
        .registry_ref()
        .resolve::<WorkerIdentityProvider>()
        .ok_or_else(|| {
            Error::worker(
                worker_id,
                "WorkerIdentityProvider is unavailable for teardown completion",
            )
        })?;
    let registry = state
        .registry_ref()
        .resolve::<FleetEventRegistry>()
        .ok_or_else(|| {
            Error::service(
                "FleetEventRegistry is unavailable for teardown completion",
            )
        })?;
    let outcome = match u16::try_from(failed_finalizers) {
        Ok(0) => TeardownOutcome::Completed,
        Ok(count) => TeardownOutcome::FinalizersFailed { count },
        Err(_) => TeardownOutcome::FinalizersFailed { count: u16::MAX },
    };
    let event = WorkerTeardownCompleted {
        operation: me.accepted_drain(),
        worker: me.identity(),
        outcome,
    };
    registry
        .publish_raw(WorkerTeardownCompleted::TOPIC, &event.encode_orbit())
        .map_err(|error| Error::worker(worker_id, error))?;
    debug!(
        worker_id = %worker_id,
        pid = event.worker.process().pid(),
        operation = ?event.operation,
        ?outcome,
        "worker teardown completion published to Orbit"
    );
    Ok(())
}

pub(crate) fn install_events(state: &AppState) -> Result<()> {
    if state.is_standalone() {
        return Ok(());
    }
    let registry = state
        .registry_ref()
        .resolve::<FleetEventRegistry>()
        .ok_or_else(|| {
            Error::bootstrap(
                "FleetEventRegistry is unavailable for shutdown events",
            )
        })?;
    registry
        .register_bridge::<WorkerDrainRequested>(state)
        .map_err(Error::Bootstrap)?;
    registry
        .register_bridge::<WorkerDrainAcknowledged>(state)
        .map_err(Error::Bootstrap)?;
    registry
        .register_bridge::<WorkerTeardownCompleted>(state)
        .map_err(Error::Bootstrap)?;

    if state.is_worker() {
        return install_worker_events(state);
    }

    let observer = state
        .registry_ref()
        .resolve::<HeartbeatObserver>()
        .ok_or_else(|| {
            Error::bootstrap(
                "HeartbeatObserver is unavailable for worker liveness tracking",
            )
        })?;
    let supervisor =
        state.registry_ref().resolve::<WorkerControl>().ok_or_else(|| {
            Error::bootstrap(
                "WorkerControl is unavailable for worker drain events",
            )
        })?;
    // Install before any child can boot. These receivers are independent of
    // the supervisor monitor, which stops before the master finishes reaping.
    let acknowledged = supervisor.clone();
    state
        .events()
        .listen::<WorkerDrainAcknowledged, _>(move |_, event| {
            let supervisor = acknowledged.clone();
            async move {
                if crate::lifecycle::drain::drain_acknowledged(&supervisor, &event).await {
                    tracing::debug!(
                        worker_id = %event.worker.worker_id(),
                        pid = event.worker.process().pid(),
                        operation = %event.operation,
                        "worker acknowledged its drain"
                    );
                } else {
                    tracing::debug!(
                        ?event,
                        "ignoring drain acknowledgement for an unknown identity or operation"
                    );
                }
                Ok(())
            }
        })
        .map_err(Error::Bootstrap)?;
    state
        .events()
        .listen::<WorkerTeardownCompleted, _>(move |_, event| {
            let supervisor = supervisor.clone();
            let observer = observer.clone();
            async move {
                if crate::lifecycle::drain::teardown_completed(&supervisor, &event).await {
                    observer
                        .complete_worker_shutdown(
                            event.worker.worker_id(),
                            event.worker.process().pid()
                        )
                        .await;
                    tracing::debug!(
                        worker_id = %event.worker.worker_id(),
                        pid = event.worker.process().pid(),
                        operation = ?event.operation,
                        outcome = ?event.outcome,
                        "worker teardown completed through Orbit events"
                    );
                } else {
                    tracing::debug!(
                        ?event,
                        "ignoring teardown completion for an unknown identity or operation"
                    );
                }
                Ok(())
            }
        })
        .map_err(Error::Bootstrap)
}

fn install_worker_events(state: &AppState) -> Result<()> {
    let me = state.registry_ref().resolve::<WorkerIdentityProvider>().ok_or_else(|| {
        Error::bootstrap(
            "WorkerIdentityProvider is unavailable; the worker cannot verify drain requests"
        )
    })?;
    state
        .events()
        .listen::<WorkerDrainRequested, _>(move |state, request| {
            let me = me.clone();
            async move {
                let identity = me.identity();
                if let Some(field) = identity.mismatch(request.target) {
                    debug!(
                        ?request,
                        field,
                        "ignoring drain request that names another worker"
                    );
                    return Ok(());
                }
                // A worker leaves once. The first operation to ask owns the
                // teardown; a repeat of it is acknowledged again, another
                // operation is told which one already has it.
                let owner = me.accept_drain(request.operation);
                if owner != request.operation {
                    tracing::debug!(
                        operation = %request.operation,
                        %owner,
                        "ignoring drain request: this worker is already leaving for another operation"
                    );
                    return Ok(());
                }
                if !state.is_shutting_down() {
                    tracing::debug!(
                        operation = %request.operation,
                        reason = ?request.reason,
                        "worker drain requested through Orbit control event"
                    );
                    state.initiate_shutdown();
                }
                state.events().emit(WorkerDrainAcknowledged { operation: owner, worker: identity });
                Ok(())
            }
        })
        .map_err(Error::Bootstrap)
}

#[cfg(test)]
#[path = "../../tests/shutdown.rs"]
mod tests;

/// Master-side: SIGTERM has been sent to worker children; drain
/// window opens. Master providers stay fully alive at this point —
/// workers may still be flushing in-process state to them.
///
/// Emitted by `supervisor::initiate_shutdown` at phase 1.
#[derive(Clone, Debug)]
pub struct WorkersDraining;

impl Event for WorkersDraining {}

/// Master-side: all worker children have exited (or been
/// force-killed past the grace period). Safe point for master
/// providers to take a final snapshot — no more concurrent writers
/// from worker side.
///
/// Emitted by `supervisor::initiate_shutdown` after `wait_workers`.
#[derive(Clone, Debug)]
pub struct WorkersExited;

impl Event for WorkersExited {}

/// Master-mode shutdown coordinator.
///
/// Called from the SIGTERM/SIGINT signal handler in master mode (see
/// `signals::listen_shutdown_for_main_process`). Drains worker
/// processes BEFORE flipping the global `state.is_shutting_down()`
/// flag, so master services stay fully alive while workers exit
/// and have a chance to flush any in-process state to the master.
///
/// Sequence:
/// 0. latch `FleetMode::ShuttingDown` and mark every live slot as draining —
///    atomically, before any event, publication or lock, so nothing new
///    starts and no exit caused by this shutdown can read as a crash.
/// 1. emit [`WorkersDraining`], then one [`WorkerDrainRequested`] per live
///    identity under the fleet shutdown operation, and wait for the exits
///    in this master's origin-based exit barrier (with the operator's
///    `shutdown_timeout` escalation, if any). Under an unbounded policy,
///    adopted and unowned processes are logged and left alive rather than
///    pinning a master that cannot reap them.
/// 2. `wait_workers` — reap exited child processes.
/// 3. emit [`WorkersExited`] + `state.initiate_shutdown()` (which
///    itself emits `ShutdownInitiated` for any subscriber).
///
/// Defensive fallbacks (should not fire in practice):
/// - Outside master mode → falls through to `state.initiate_shutdown()`.
/// - `WorkerControl` not registered → same fallback.
pub async fn initiate_shutdown(state: &AppState) {
    if !state.is_master() {
        warn!(
            "supervisor::initiate_shutdown called outside master mode; falling through to state"
        );
        state.initiate_shutdown();
        return;
    }

    let Some(supervisor) = state.registry_ref().resolve::<WorkerControl>()
    else {
        warn!(
            "supervisor::initiate_shutdown: WorkerControl not registered; falling through to state"
        );
        state.initiate_shutdown();
        return;
    };

    // SHUTDOWN_HAS_PRECEDENCE: the latch and the slot intents are committed
    // before the first event leaves this function.
    if !supervisor.begin_fleet_shutdown().await {
        debug!("master shutdown already latched; not repeating phase 1");
    }
    debug!("🚦 master shutdown phase 1/3 — draining workers");
    state.events().emit(WorkersDraining);
    let requested =
        crate::lifecycle::drain::request_fleet_drain(&supervisor, state).await;
    debug!(requested, "fleet shutdown drain requests published");
    crate::lifecycle::drain::shutdown_workers_after_drain_requests(
        &supervisor,
        state,
    )
    .await;

    debug!("🚦 master shutdown phase 2/3 — reaping workers");
    crate::lifecycle::drain::wait_workers(&supervisor).await;

    debug!(
        "🚦 master shutdown phase 3/3 — flipping global shutdown flag (providers wind down)"
    );
    state.events().emit(WorkersExited);
    state.initiate_shutdown();
}
