//! Worker boot acknowledgements. Heartbeats prove liveness, not boot completion.

use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};

use claviron_orbit::{FleetEventCodec, FleetEventRegistry};
use claviron_runtime::events::Event;
use claviron_runtime::{AppState, ProcessStarted, RunMode, WorkerId};
#[cfg(test)]
use tokio::sync::broadcast;

use crate::control::WorkerControl;
use crate::{Error, Result};

const WORKER_STARTED_BYTES: usize = size_of::<u8>() + size_of::<u32>();

/// Supervisor wire envelope for a worker's local ProcessStarted milestone.
/// Never decoded back into ProcessStarted in another process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerStarted {
    pub(crate) worker_id: WorkerId,
    pub(crate) pid: u32,
}

impl Event for WorkerStarted {}

impl FleetEventCodec for WorkerStarted {
    const TOPIC: &'static str = "claviron.supervisor.worker.started.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(WORKER_STARTED_BYTES);
        payload.push(self.worker_id.raw());
        payload.extend_from_slice(&self.pid.to_be_bytes());
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> std::result::Result<Self, String> {
        let bytes: &[u8; WORKER_STARTED_BYTES] = bytes.try_into().map_err(|_| {
            format!(
                "invalid worker started payload length: expected {WORKER_STARTED_BYTES}, got {}",
                bytes.len()
            )
        })?;
        let pid = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if bytes[0] == 0 || pid == 0 {
            return Err("worker started requires a non-zero worker id and PID"
                .to_string());
        }
        Ok(Self { worker_id: WorkerId::new(bytes[0]), pid })
    }
}

/// Master-side, one-time confirmation that all initially spawned workers
/// completed boot. This is not ongoing liveness or application readiness.
#[derive(Clone, Debug)]
pub struct WorkersBootConfirmed {
    pub count: u8,
}

impl Event for WorkersBootConfirmed {}

pub(crate) fn install_events(state: &AppState) -> Result<()> {
    if state.is_standalone() {
        return Ok(());
    }
    let registry = state.registry_ref().resolve::<FleetEventRegistry>().ok_or_else(|| {
        Error::bootstrap("FleetEventRegistry is unavailable for supervisor startup events")
    })?;
    registry
        .register_bridge::<WorkerStarted>(state)
        .map_err(Error::Bootstrap)?;

    if state.is_master() {
        let control = state
            .registry_ref()
            .resolve::<WorkerControl>()
            .ok_or_else(|| {
                Error::bootstrap(
                    "WorkerControl is unavailable for startup observations",
                )
            })?;
        state
            .events()
            .listen::<WorkerStarted, _>(move |_, event| {
                let control = control.clone();
                async move {
                    if crate::lifecycle::startup::confirm_worker_started(
                        &control, event,
                    )
                    .await
                    {
                        control.changed.notify_waiters();
                    }
                    Ok(())
                }
            })
            .map_err(Error::Bootstrap)?;
    }

    if let RunMode::Worker(worker_id) = state.run_mode() {
        let me = state
            .registry_ref()
            .resolve::<crate::lifecycle::WorkerIdentityProvider>()
            .ok_or_else(|| {
                Error::bootstrap(
                    "WorkerIdentityProvider is unavailable for worker startup acknowledgement"
                )
            })?;
        // Subscribe synchronously before bootstrap can emit. The local event
        // stays identity-free; only this supervisor adapter adds topology.
        let started = AtomicBool::new(false);
        state
            .events()
            .listen::<ProcessStarted, _>(move |state, _| {
                let me = me.clone();
                let first = !started.swap(true, Ordering::Relaxed);
                async move {
                    if first && !state.is_shutting_down() {
                        me.mark_started();
                        state.events().emit(WorkerStarted {
                            worker_id,
                            pid: std::process::id(),
                        });
                    }
                    Ok(())
                }
            })
            .map_err(Error::Bootstrap)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn received(
    event: std::result::Result<WorkerStarted, broadcast::error::RecvError>
) -> Result<WorkerStarted> {
    event.map_err(|error| match error {
        broadcast::error::RecvError::Lagged(skipped) => Error::service(
            format!("worker startup event stream lagged by {skipped} event(s)"),
        ),
        broadcast::error::RecvError::Closed => {
            Error::service("worker startup event stream closed")
        }
    })
}
