use std::collections::BTreeSet;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use claviron_orbit::{FleetEventCodec, FleetEventRegistry};
use claviron_runtime::events::Event;
use claviron_runtime::{AppState, RunMode, WorkerId};

use crate::Error;

const SERVER_RELOADING_BYTES: usize = size_of::<u64>() + size_of::<u8>();
const WORKER_RELOADED_BYTES: usize = size_of::<u64>() + size_of::<u8>();
static NEXT_RELOAD_GENERATION: AtomicU64 = AtomicU64::new(1);

/// Master request to reload one worker or the active fleet in place.
/// Carries a correlation generation and target; it requests execution but does
/// not itself acknowledge acceptance or completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ServerReloading {
    pub(crate) generation: u64,
    /// Zero broadcasts to all workers; non-zero targets one worker id.
    pub(crate) target_worker_id: u8,
}

impl Event for ServerReloading {}

impl FleetEventCodec for ServerReloading {
    const TOPIC: &'static str = "claviron.supervisor.server.reloading.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(SERVER_RELOADING_BYTES);
        payload.extend_from_slice(&self.generation.to_be_bytes());
        payload.push(self.target_worker_id);
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != SERVER_RELOADING_BYTES {
            return Err(format!(
                "invalid server reload payload length: expected {SERVER_RELOADING_BYTES}, got {}",
                bytes.len()
            ));
        }

        let mut generation = [0; size_of::<u64>()];
        generation.copy_from_slice(&bytes[..size_of::<u64>()]);
        Ok(Self {
            generation: u64::from_be_bytes(generation),
            target_worker_id: bytes[size_of::<u64>()],
        })
    }
}

/// Terminal acknowledgement emitted after one worker finishes its local
/// registry reload cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerReloaded {
    pub(crate) generation: u64,
    pub(crate) worker_id: u8,
}

impl Event for WorkerReloaded {}

impl FleetEventCodec for WorkerReloaded {
    const TOPIC: &'static str = "claviron.supervisor.worker.reloaded.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(WORKER_RELOADED_BYTES);
        payload.extend_from_slice(&self.generation.to_be_bytes());
        payload.push(self.worker_id);
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != WORKER_RELOADED_BYTES {
            return Err(format!(
                "invalid worker reload payload length: expected {WORKER_RELOADED_BYTES}, got {}",
                bytes.len()
            ));
        }

        let mut generation = [0; size_of::<u64>()];
        generation.copy_from_slice(&bytes[..size_of::<u64>()]);
        Ok(Self {
            generation: u64::from_be_bytes(generation),
            worker_id: bytes[size_of::<u64>()],
        })
    }
}

#[derive(Debug)]
pub(crate) enum ReloadOutcome {
    Complete {
        reloaded: Vec<WorkerId>,
    },
    Partial {
        reloaded: Vec<WorkerId>,
        timed_out: Vec<WorkerId>,
        unavailable: Vec<WorkerId>,
    },
}

/// Publish one fleet reload event and wait until every expected worker has
/// acknowledged that reload generation.
pub(crate) async fn reload_workers(
    state: &AppState,
    active_worker_ids: &[WorkerId],
    unavailable_worker_ids: Vec<WorkerId>,
    timeout: Duration,
) -> Result<ReloadOutcome, String> {
    reload_workers_with_target(
        state,
        active_worker_ids,
        unavailable_worker_ids,
        timeout,
        None,
    )
    .await
}

pub(crate) async fn reload_worker(
    state: &AppState,
    worker_id: WorkerId,
    timeout: Duration,
) -> Result<(), String> {
    match reload_workers_with_target(
        state,
        &[worker_id],
        Vec::new(),
        timeout,
        Some(worker_id),
    )
    .await?
    {
        ReloadOutcome::Complete { .. } => Ok(()),
        ReloadOutcome::Partial { timed_out, .. } => Err(format!(
            "worker reload timed out for worker ids: {timed_out:?}"
        )),
    }
}

async fn reload_workers_with_target(
    state: &AppState,
    active_worker_ids: &[WorkerId],
    unavailable_worker_ids: Vec<WorkerId>,
    timeout: Duration,
    target: Option<WorkerId>,
) -> Result<ReloadOutcome, String> {
    let pending = active_worker_ids.iter().copied().collect::<BTreeSet<_>>();
    if pending.is_empty() {
        return Ok(outcome(Vec::new(), Vec::new(), unavailable_worker_ids));
    }

    let generation = NEXT_RELOAD_GENERATION.fetch_add(1, Ordering::Relaxed);
    let observer = state
        .registry_ref()
        .resolve::<super::reload_observer::ReloadObserver>()
        .ok_or_else(|| {
            "worker reload observer was not registered".to_owned()
        })?;
    let acknowledgements = observer.begin(generation);

    // Zero on the wire is the whole-fleet broadcast, not a worker id.
    state.events().emit(ServerReloading {
        generation,
        target_worker_id: target.map_or(0, WorkerId::raw),
    });

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if pending.is_subset(&acknowledgements.received()) {
            break;
        }
        if tokio::time::timeout_at(
            deadline,
            acknowledgements.changed.notified(),
        )
        .await
        .is_err()
        {
            break;
        }
    }
    let received = acknowledgements.received();
    let reloaded = pending.intersection(&received).copied().collect();
    let timed_out = pending.difference(&received).copied().collect();
    Ok(outcome(reloaded, timed_out, unavailable_worker_ids))
}

fn outcome(
    reloaded: Vec<WorkerId>,
    timed_out: Vec<WorkerId>,
    unavailable: Vec<WorkerId>,
) -> ReloadOutcome {
    if timed_out.is_empty() && unavailable.is_empty() {
        ReloadOutcome::Complete { reloaded }
    } else {
        ReloadOutcome::Partial { reloaded, timed_out, unavailable }
    }
}

pub(crate) fn install_events(state: &AppState) -> crate::Result<()> {
    if state.is_standalone() {
        return Ok(());
    }

    let event_registry = state
        .registry_ref()
        .resolve::<FleetEventRegistry>()
        .ok_or_else(|| {
        Error::bootstrap(
            "FleetEventRegistry is unavailable for supervisor reload events",
        )
    })?;
    event_registry
        .register_bridge::<ServerReloading>(state)
        .map_err(Error::Bootstrap)?;
    event_registry
        .register_bridge::<WorkerReloaded>(state)
        .map_err(Error::Bootstrap)?;

    if state.is_master() {
        let observer = std::sync::Arc::new(
            super::reload_observer::ReloadObserver::default(),
        );
        state
            .registry_ref()
            .insert(observer.clone())
            .map_err(Error::Bootstrap)?;
        state
            .events()
            .listen::<WorkerReloaded, _>(move |_, event| {
                observer.observe(event);
                async { Ok(()) }
            })
            .map_err(Error::Bootstrap)?;
    }

    if let RunMode::Worker(worker_id) = state.run_mode() {
        let runner = state
            .registry_ref()
            .resolve::<crate::management::worker_reload::WorkerReloadRunner>()
            .ok_or_else(|| {
                Error::bootstrap("WorkerReloadRunner is unavailable")
            })?;
        state
            .events()
            .listen::<ServerReloading, _>(move |state, request| {
                let runner = runner.clone();
                async move {
                    if state.is_shutting_down()
                        || (request.target_worker_id != 0
                            && request.target_worker_id != worker_id.raw())
                    {
                        return Ok(());
                    }
                    if let Err(reason) =
                        runner.submit(state, request, worker_id)
                    {
                        tracing::warn!(
                            ?reason,
                            "worker reload refused: reload runner is stopped"
                        );
                    }
                    Ok(())
                }
            })
            .map_err(Error::Bootstrap)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/reload.rs"]
mod tests;
