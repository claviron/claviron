//! Typed worker presence: who is publishing on a worker's lane.
//!
//! The heartbeat on kind `221` is the pulse — a counter and a time, nothing
//! else, and the app-side tooling reads it with that exact shape. Presence is
//! the name: the worker's complete [`WorkerIdentity`], the master that spawned
//! it, when its runtime began, what it believes its phase is and which
//! operation it is leaving for. Only the worker writes its own lane; the
//! master's view of the same slot is a separate projection (kind `227`).
//!
//! A presence record is evidence, never authority. Whoever reads one still
//! asks the kernel whether that exact process is alive before acting on it.

use std::mem::size_of;

use bytes::Bytes;
use claviron_orbit::lock::{LOCK_EVENT_RING_KIND, LOCK_STATE_KIND};
use claviron_orbit::{Fleet, NodeId, OrbitEpoch, OrbitTyped, RingSpec};
use claviron_runtime::registry;

use crate::lifecycle::{
    MASTER_GENERATION_RING_KIND, OperationId, WORKER_IDENTITY_BYTES,
    WorkerIdentity,
};

/// Orbit kind of the presence ring. Retained SHM segments outlive processes;
/// verify existing kind ownership and record shape before reuse.
pub(crate) const PRESENCE_RING_KIND: u8 = 239;
const PRESENCE_RING_CAPACITY: usize = 64;
const PRESENCE_FRAME_KIND: u8 = 1;
const PRESENCE_VERSION: u8 = 1;
const PRESENCE_BYTES: usize = size_of::<u8>()
    + WORKER_IDENTITY_BYTES
    + size_of::<u64>()
    + size_of::<u8>()
    + size_of::<u32>()
    + size_of::<u64>();
pub(crate) const PRESENCE_RING_SPEC: RingSpec =
    RingSpec::per_node(PRESENCE_RING_CAPACITY, 64);

const _: () = assert!(PRESENCE_RING_KIND != LOCK_STATE_KIND);
const _: () = assert!(PRESENCE_RING_KIND != LOCK_EVENT_RING_KIND);
const _: () = assert!(PRESENCE_RING_KIND != MASTER_GENERATION_RING_KIND);

/// Orbit ring marker for worker-written identity and lifecycle evidence.
/// Separate from the heartbeat pulse so presence can identify a surviving
/// process without changing the heartbeat wire contract.
#[derive(Clone, Debug)]
pub(crate) struct PresenceRecord;

impl OrbitTyped for PresenceRecord {
    const KIND: u8 = PRESENCE_RING_KIND;
    const RING_SPEC: RingSpec = PRESENCE_RING_SPEC;
}

/// The worker's own account of where it is. Observation, never intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PresencePhase {
    /// Providers are booting; the boot acknowledgement has not been emitted.
    Booting,
    Running,
    /// Shutdown is initiated in the worker; it is on its way out.
    Draining,
}

impl PresencePhase {
    const fn code(self) -> u8 {
        match self {
            Self::Booting => 1,
            Self::Running => 2,
            Self::Draining => 3,
        }
    }

    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Booting),
            2 => Some(Self::Running),
            3 => Some(Self::Draining),
            _ => None,
        }
    }
}

/// One presence record as published by a worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerPresence {
    identity: WorkerIdentity,
    /// Wall-clock start of the worker's Claviron runtime, written once by the
    /// worker. Display data for operators; never the kernel start stamp, which
    /// lives in `identity.process()` and is what liveness is judged by.
    runtime_started_at_unix_ms: u64,
    phase: PresencePhase,
    /// The parent PID as the worker sees it: `1` once its master has died.
    /// Diagnostic only; ownership is `identity.master()`.
    parent_pid: u32,
    /// The drain this worker accepted, if any.
    operation: Option<OperationId>,
    captured_at: OrbitEpoch,
    /// Position of this observation in the worker-owned presence lane. This
    /// is ring metadata, not part of the payload the worker prepares.
    counter: u64,
}

impl WorkerPresence {
    pub(crate) const fn new(
        identity: WorkerIdentity,
        runtime_started_at_unix_ms: u64,
        phase: PresencePhase,
        parent_pid: u32,
        operation: Option<OperationId>,
        captured_at: OrbitEpoch,
    ) -> Self {
        Self {
            identity,
            runtime_started_at_unix_ms,
            phase,
            parent_pid,
            operation,
            captured_at,
            counter: 0,
        }
    }

    pub const fn identity(self) -> WorkerIdentity {
        self.identity
    }

    pub const fn runtime_started_at_unix_ms(self) -> u64 {
        self.runtime_started_at_unix_ms
    }

    pub const fn phase(self) -> PresencePhase {
        self.phase
    }

    pub const fn parent_pid(self) -> u32 {
        self.parent_pid
    }

    pub const fn active_operation_id(self) -> Option<u64> {
        match self.operation {
            Some(operation) => Some(operation.raw()),
            None => None,
        }
    }

    pub const fn captured_at(self) -> OrbitEpoch {
        self.captured_at
    }

    pub const fn counter(self) -> u64 {
        self.counter
    }

    const fn with_counter(
        mut self,
        counter: u64,
    ) -> Self {
        self.counter = counter;
        self
    }

    /// Whether the exact process this record names is alive right now,
    /// according to the kernel — PID and start stamp both. A record whose PID
    /// is in use by another process is not that process.
    pub fn exact_process_alive(self) -> bool {
        self.identity.process().is_alive()
    }

    fn encode(self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(PRESENCE_BYTES);
        payload.push(PRESENCE_VERSION);
        self.identity.encode_into(&mut payload);
        payload
            .extend_from_slice(&self.runtime_started_at_unix_ms.to_be_bytes());
        payload.push(self.phase.code());
        payload.extend_from_slice(&self.parent_pid.to_be_bytes());
        payload.extend_from_slice(
            &self.operation.map_or(0, OperationId::raw).to_be_bytes(),
        );
        payload
    }

    fn decode(
        payload: &[u8],
        captured_at: OrbitEpoch,
        counter: u64,
    ) -> Option<Self> {
        if payload.len() != PRESENCE_BYTES || payload[0] != PRESENCE_VERSION {
            return None;
        }
        let mut at = 1;
        let identity = WorkerIdentity::decode(
            payload[at..at + WORKER_IDENTITY_BYTES].try_into().ok()?,
        )
        .ok()?;
        at += WORKER_IDENTITY_BYTES;
        let runtime_started_at_unix_ms =
            u64::from_be_bytes(payload[at..at + 8].try_into().ok()?);
        at += 8;
        let phase = PresencePhase::from_code(payload[at])?;
        at += 1;
        let parent_pid =
            u32::from_be_bytes(payload[at..at + 4].try_into().ok()?);
        at += 4;
        let operation = OperationId::from_raw(u64::from_be_bytes(
            payload[at..at + 8].try_into().ok()?,
        ));
        Some(Self {
            identity,
            runtime_started_at_unix_ms,
            phase,
            parent_pid,
            operation,
            captured_at,
            counter,
        })
    }
}

/// Publish this process's presence on its own lane.
pub(crate) fn publish_presence(
    fleet: &Fleet,
    presence: WorkerPresence,
) -> registry::Result<WorkerPresence> {
    let worker_node = u16::from(presence.identity.worker_id().raw());
    if fleet.node_id().get() != worker_node {
        return Err(registry::Error::msg(format!(
            "worker #{} cannot publish presence on Orbit node {}",
            presence.identity.worker_id(),
            fleet.node_id().get()
        )));
    }
    let id = fleet.publish::<PresenceRecord>(
        PRESENCE_FRAME_KIND,
        presence.captured_at.as_unix_ms(),
        Bytes::from(presence.encode()),
    );
    Ok(presence.with_counter(id.counter()))
}

/// The newest presence record on `worker_id`'s lane that this version
/// understands. A record that claims another worker identity is ignored.
pub fn latest_worker_presence(
    fleet: &Fleet,
    worker_id: claviron_runtime::WorkerId,
) -> Option<WorkerPresence> {
    let node_id = NodeId::new(u16::from(worker_id.raw()));
    let capacity = fleet.ring_capacity::<PresenceRecord>() as u64;
    let head = fleet.lane_head::<PresenceRecord>(node_id);
    (0..head.min(capacity)).find_map(|offset| {
        let frame =
            fleet.read_lane_at::<PresenceRecord>(node_id, head - 1 - offset)?;
        if frame.id.kind() != PRESENCE_RING_KIND
            || frame.kind != PRESENCE_FRAME_KIND
        {
            return None;
        }
        let presence = WorkerPresence::decode(
            &frame.payload,
            OrbitEpoch::from_unix_ms(frame.ver),
            frame.id.counter(),
        )?;
        (u16::from(presence.identity().worker_id().raw()) == node_id.get())
            .then_some(presence)
    })
}

#[cfg(test)]
#[path = "../../tests/observation/presence.rs"]
mod tests;
