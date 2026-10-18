//! Explicit transfer of a live worker from a dead master to its successor.
//!
//! Presence proves which exact process is asking to keep its slot. It does not
//! transfer ownership by itself: the new master names that complete identity,
//! the worker verifies its original master is dead, then records and
//! acknowledges the new master identity. Only that acknowledgement (or the
//! worker's subsequent durable presence carrying the same identity) permits
//! the master to install an adopted process handle.

use std::fmt;
use std::time::Duration;

use claviron_orbit::{
    FleetEventCodec, FleetEventRegistry, OrbitEpoch, OrbitStateExt,
};
use claviron_runtime::events::Event;
use claviron_runtime::{AppState, RunMode};
use tracing::{debug, info, warn};

use crate::control::WorkerControl;
use crate::lifecycle::{
    MasterIdentity, WORKER_IDENTITY_BYTES, WorkerBlockReason, WorkerIdentity,
    master_identity_is_known,
};
use crate::presence::{PresencePhase, WorkerPresence};
use crate::{Error, Result, WorkerIdentityProvider};

const OWNERSHIP_CLAIM_BYTES: usize =
    WORKER_IDENTITY_BYTES + MASTER_IDENTITY_BYTES;
const OWNERSHIP_ACCEPTED_BYTES: usize = WORKER_IDENTITY_BYTES * 2;
const MASTER_IDENTITY_BYTES: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerOwnershipClaim {
    pub(crate) target: WorkerIdentity,
    pub(crate) claimant: MasterIdentity,
}

impl Event for WorkerOwnershipClaim {}

impl FleetEventCodec for WorkerOwnershipClaim {
    const TOPIC: &'static str = "claviron.supervisor.worker.ownership.claim.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(OWNERSHIP_CLAIM_BYTES);
        self.target.encode_into(&mut payload);
        encode_master(self.claimant, &mut payload);
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> std::result::Result<Self, String> {
        if bytes.len() != OWNERSHIP_CLAIM_BYTES {
            return Err(format!(
                "invalid ownership claim payload length: expected {OWNERSHIP_CLAIM_BYTES}, got {}",
                bytes.len()
            ));
        }
        Ok(Self {
            target: decode_worker(&bytes[..WORKER_IDENTITY_BYTES])?,
            claimant: decode_master(&bytes[WORKER_IDENTITY_BYTES..])?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerOwnershipAccepted {
    pub(crate) previous: WorkerIdentity,
    pub(crate) current: WorkerIdentity,
}

impl Event for WorkerOwnershipAccepted {}

impl FleetEventCodec for WorkerOwnershipAccepted {
    const TOPIC: &'static str =
        "claviron.supervisor.worker.ownership.accepted.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(OWNERSHIP_ACCEPTED_BYTES);
        self.previous.encode_into(&mut payload);
        self.current.encode_into(&mut payload);
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> std::result::Result<Self, String> {
        if bytes.len() != OWNERSHIP_ACCEPTED_BYTES {
            return Err(format!(
                "invalid ownership acceptance payload length: expected {OWNERSHIP_ACCEPTED_BYTES}, got {}",
                bytes.len()
            ));
        }
        Ok(Self {
            previous: decode_worker(&bytes[..WORKER_IDENTITY_BYTES])?,
            current: decode_worker(&bytes[WORKER_IDENTITY_BYTES..])?,
        })
    }
}

fn encode_master(
    master: MasterIdentity,
    out: &mut Vec<u8>,
) {
    out.extend_from_slice(&master.process().pid().to_be_bytes());
    out.extend_from_slice(&master.process().started_at().to_be_bytes());
    out.extend_from_slice(&master.generation().to_be_bytes());
}

fn decode_master(bytes: &[u8]) -> std::result::Result<MasterIdentity, String> {
    let bytes: &[u8; MASTER_IDENTITY_BYTES] =
        bytes.try_into().map_err(|_| {
            "invalid ownership claim master identity length".to_string()
        })?;
    let pid = u32::from_be_bytes(bytes[..4].try_into().expect("4 bytes"));
    let started_at =
        u64::from_be_bytes(bytes[4..12].try_into().expect("8 bytes"));
    let generation =
        u64::from_be_bytes(bytes[12..].try_into().expect("8 bytes"));
    if pid == 0 || generation == 0 {
        return Err(
            "ownership claim requires a non-zero master PID and generation"
                .to_string(),
        );
    }
    Ok(MasterIdentity::new(
        claviron_sys::process::ProcessIdentity::new(pid, started_at),
        generation,
    ))
}

fn decode_worker(bytes: &[u8]) -> std::result::Result<WorkerIdentity, String> {
    WorkerIdentity::decode(
        bytes.try_into().map_err(|_| {
            "invalid ownership worker identity length".to_string()
        })?,
    )
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
                "FleetEventRegistry is unavailable for worker ownership events",
            )
        })?;
    registry
        .register_bridge::<WorkerOwnershipClaim>(state)
        .map_err(Error::Bootstrap)?;
    registry
        .register_bridge::<WorkerOwnershipAccepted>(state)
        .map_err(Error::Bootstrap)?;

    match state.run_mode() {
        RunMode::Worker(_) => install_worker_listener(state),
        RunMode::Master => install_master_listener(state),
        RunMode::Standalone => Ok(()),
    }
}

fn install_worker_listener(state: &AppState) -> Result<()> {
    let me = state.registry_ref().resolve::<WorkerIdentityProvider>().ok_or_else(|| {
        Error::bootstrap("WorkerIdentityProvider is unavailable for worker ownership claims")
    })?;
    state
        .events()
        .listen::<WorkerOwnershipClaim, _>(move |state, claim| {
            let me = me.clone();
            async move {
                if state.is_shutting_down() || me.accepted_drain().is_some() {
                    debug!(?claim, "ignoring ownership claim while worker is draining");
                    return Ok(());
                }
                let Some(fleet) = state.fleet() else {
                    warn!(?claim, "ignoring ownership claim without an Orbit fleet");
                    return Ok(());
                };
                if !master_identity_is_known(&fleet, claim.target.master())
                    || !master_identity_is_known(&fleet, claim.claimant)
                {
                    debug!(
                        ?claim,
                        "worker rejected ownership claim with an unknown master identity"
                    );
                    return Ok(());
                }
                match me.accept_master_claim(claim.target, claim.claimant) {
                    Ok(current) => {
                        state
                            .events()
                            .emit(WorkerOwnershipAccepted { previous: claim.target, current });
                        info!(
                            worker_id = %current.worker_id(),
                            pid = current.process().pid(),
                            master_pid = current.master().process().pid(),
                            master_generation = current.master().generation(),
                            "worker accepted ownership from a new master"
                        );
                    }
                    Err(reason) => {
                        debug!(?claim, reason, "worker rejected ownership claim");
                    }
                }
                Ok(())
            }
        })
        .map_err(Error::Bootstrap)
}

fn install_master_listener(state: &AppState) -> Result<()> {
    let supervisor =
        state.registry_ref().resolve::<WorkerControl>().ok_or_else(|| {
            Error::bootstrap(
                "WorkerControl is unavailable for ownership acknowledgements",
            )
        })?;
    state
        .events()
        .listen::<WorkerOwnershipAccepted, _>(move |state, accepted| {
            let supervisor = supervisor.clone();
            async move {
                if !crate::lifecycle::reconcile::ownership_accepted(&supervisor, accepted, &state).await {
                    warn!(
                        ?accepted,
                        "ignored an ownership acknowledgement without a matching claim"
                    );
                }
                Ok(())
            }
        })
        .map_err(Error::Bootstrap)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PresenceDecision {
    ReclaimDead,
    Claim,
    AdoptAccepted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefusalReason {
    Stale { age: Duration },
    Phase(PresencePhase),
    ActiveOperation(u64),
    UnknownMaster,
    MasterStillAlive,
    MasterGenerationNotOlder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReconciliationRefusal {
    presence: WorkerPresence,
    reason: RefusalReason,
}

impl ReconciliationRefusal {
    pub(crate) const fn presence(self) -> WorkerPresence {
        self.presence
    }

    /// The reason without its diagnostic payload, as the slot records it.
    pub(crate) const fn block_reason(self) -> WorkerBlockReason {
        match self.reason {
            RefusalReason::Stale { .. } => WorkerBlockReason::StalePresence,
            RefusalReason::Phase(_) => WorkerBlockReason::WorkerNotRunning,
            RefusalReason::ActiveOperation(_) => WorkerBlockReason::WorkerBusy,
            RefusalReason::UnknownMaster => WorkerBlockReason::UnknownMaster,
            RefusalReason::MasterStillAlive => {
                WorkerBlockReason::MasterStillAlive
            }
            RefusalReason::MasterGenerationNotOlder => {
                WorkerBlockReason::MasterNotOlder
            }
        }
    }
}

impl fmt::Display for ReconciliationRefusal {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let identity = self.presence.identity();
        write!(
            f,
            "worker #{} PID {} start_stamp={} incarnation={} master_pid={} master_start_stamp={} master_generation={} presence_counter={} captured_at={} cannot be reconciled: ",
            identity.worker_id(),
            identity.process().pid(),
            identity.process().started_at(),
            identity.incarnation(),
            identity.master().process().pid(),
            identity.master().process().started_at(),
            identity.master().generation(),
            self.presence.counter(),
            self.presence.captured_at()
        )?;
        match self.reason {
            RefusalReason::Stale { age } => write!(f, "presence is stale by {age:?}"),
            RefusalReason::Phase(phase) => write!(f, "worker phase is {phase:?}, not Running"),
            RefusalReason::ActiveOperation(operation) => {
                write!(f, "worker is already handling operation {operation}")
            }
            RefusalReason::UnknownMaster => {
                f.write_str("the original master identity is absent from the generation ring")
            }
            RefusalReason::MasterStillAlive => {
                f.write_str("the original master identity is still alive")
            }
            RefusalReason::MasterGenerationNotOlder => {
                f.write_str("the original master generation is not older than this master")
            }
        }
    }
}

pub(crate) fn assess_presence(
    presence: WorkerPresence,
    current_master: MasterIdentity,
    original_master_is_known: bool,
    now: OrbitEpoch,
    max_age: Duration,
) -> std::result::Result<PresenceDecision, ReconciliationRefusal> {
    if !presence.exact_process_alive() {
        return Ok(PresenceDecision::ReclaimDead);
    }
    let age = presence.captured_at().age_at(now);
    if age > max_age {
        return Err(ReconciliationRefusal {
            presence,
            reason: RefusalReason::Stale { age },
        });
    }
    if presence.phase() != PresencePhase::Running {
        return Err(ReconciliationRefusal {
            presence,
            reason: RefusalReason::Phase(presence.phase()),
        });
    }
    if let Some(operation) = presence.active_operation_id() {
        return Err(ReconciliationRefusal {
            presence,
            reason: RefusalReason::ActiveOperation(operation),
        });
    }

    let original_master = presence.identity().master();
    if original_master == current_master {
        return Ok(PresenceDecision::AdoptAccepted);
    }
    if !original_master_is_known {
        return Err(ReconciliationRefusal {
            presence,
            reason: RefusalReason::UnknownMaster,
        });
    }
    if original_master.process().is_alive() {
        return Err(ReconciliationRefusal {
            presence,
            reason: RefusalReason::MasterStillAlive,
        });
    }
    if original_master.generation() >= current_master.generation() {
        return Err(ReconciliationRefusal {
            presence,
            reason: RefusalReason::MasterGenerationNotOlder,
        });
    }
    Ok(PresenceDecision::Claim)
}

#[cfg(test)]
#[path = "../../tests/events/ownership.rs"]
mod tests;
