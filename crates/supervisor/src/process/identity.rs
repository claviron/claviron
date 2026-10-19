//! Who the master is, in terms a worker can verify and a successor can read.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use claviron_orbit::lock::{LOCK_EVENT_RING_KIND, LOCK_STATE_KIND};
use claviron_orbit::{Fleet, NodeId, OrbitEpoch, OrbitTyped, RingSpec};
use claviron_runtime::{AppState, WorkerId};
use claviron_sys::process::{self, ProcessIdentity};

use crate::lifecycle::operation::OperationId;
use crate::presence::{PresencePhase, WorkerPresence};
use crate::{Error, Result, WorkerBootstrap};

/// Orbit kind of the master generation ring. Retained SHM segments outlive
/// processes; verify existing kind ownership and record shape before reuse.
pub(crate) const MASTER_GENERATION_RING_KIND: u8 = 238;
const MASTER_GENERATION_FRAME_KIND: u8 = 1;
/// Only master lane `0` is written; the ring is per node because that is the
/// shape every supervisor ring has, and the payload is one `ProcessIdentity`.
const MASTER_GENERATION_RING_SPEC: RingSpec =
    RingSpec::per_node(8, MASTER_GENERATION_PAYLOAD);
const MASTER_GENERATION_PAYLOAD: usize = size_of::<u32>() + size_of::<u64>();

const _: () = assert!(MASTER_GENERATION_RING_KIND != LOCK_STATE_KIND);
const _: () = assert!(MASTER_GENERATION_RING_KIND != LOCK_EVENT_RING_KIND);

/// The exact master process and its place in the sequence of masters this
/// instance has had.
///
/// `process` is the kernel identity (PID plus start stamp), so a PID that came
/// back around cannot pass for it. `generation` orders masters of one instance,
/// which two process identities alone cannot do: it is taken from the master
/// lane of the generation ring, whose head counts every master boot published
/// there since the SHM segment was created. The segment outlives the process,
/// so the count survives restarts; a cleared segment starts it again, which is
/// why the generation qualifies the process identity and never replaces it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MasterIdentity {
    process: ProcessIdentity,
    generation: u64,
}

impl MasterIdentity {
    pub const fn new(
        process: ProcessIdentity,
        generation: u64,
    ) -> Self {
        Self { process, generation }
    }

    pub const fn process(self) -> ProcessIdentity {
        self.process
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}

/// Orbit ring marker for persisted master identity/generation history.
/// Its lane counter distinguishes successive masters without resetting on boot.
#[derive(Clone)]
pub(crate) struct MasterGenerationRecord;

impl OrbitTyped for MasterGenerationRecord {
    const KIND: u8 = MASTER_GENERATION_RING_KIND;
    const RING_SPEC: RingSpec = MASTER_GENERATION_RING_SPEC;
}

/// Establish the identity of the current process as this instance's master.
///
/// Publishes one frame on the master lane and takes the generation from the
/// counter that frame was given, so two masters racing on the same segment
/// cannot both claim one number. Called once, at supervisor boot.
pub(crate) fn establish_master_identity(
    fleet: &Fleet
) -> Result<MasterIdentity> {
    let pid = std::process::id();
    let process = process::identity(pid).ok_or_else(|| {
        Error::service(format!(
            "master process {pid} cannot read its own kernel identity"
        ))
    })?;
    let id = fleet.publish::<MasterGenerationRecord>(
        MASTER_GENERATION_FRAME_KIND,
        process.started_at(),
        Bytes::from(encode(process)),
    );
    Ok(MasterIdentity::new(process, id.counter() + 1))
}

/// The most recent master this instance's segment remembers, whether or not it
/// is still alive. `None` on a fresh segment.
///
/// Boot reconciliation (checklist C6.2) is its consumer; until then only the
/// test reads it.
#[cfg(test)]
pub(crate) fn last_master_identity(fleet: &Fleet) -> Option<MasterIdentity> {
    let node = NodeId::ZERO;
    let counter =
        fleet.lane_head::<MasterGenerationRecord>(node).checked_sub(1)?;
    let frame = fleet.read_lane_at::<MasterGenerationRecord>(node, counter)?;
    if frame.kind != MASTER_GENERATION_FRAME_KIND {
        return None;
    }
    let process = decode(&frame.payload)?;
    (process.started_at() == frame.ver)
        .then(|| MasterIdentity::new(process, counter + 1))
}

/// Whether the generation ring contains this exact master identity. A
/// generation number alone is insufficient: clearing the segment can restart
/// its counter, and a PID alone can be reused.
pub(crate) fn master_identity_is_known(
    fleet: &Fleet,
    candidate: MasterIdentity,
) -> bool {
    let node = NodeId::ZERO;
    let head = fleet.lane_head::<MasterGenerationRecord>(node);
    let capacity = fleet.ring_capacity::<MasterGenerationRecord>() as u64;
    (0..head.min(capacity)).any(|offset| {
        let counter = head - 1 - offset;
        let Some(frame) =
            fleet.read_lane_at::<MasterGenerationRecord>(node, counter)
        else {
            return false;
        };
        if frame.kind != MASTER_GENERATION_FRAME_KIND {
            return false;
        }
        decode(&frame.payload).is_some_and(|process| {
            MasterIdentity::new(process, counter + 1) == candidate
                && process.started_at() == frame.ver
        })
    })
}

fn encode(process: ProcessIdentity) -> Vec<u8> {
    let mut payload = Vec::with_capacity(MASTER_GENERATION_PAYLOAD);
    payload.extend_from_slice(&process.pid().to_be_bytes());
    payload.extend_from_slice(&process.started_at().to_be_bytes());
    payload
}

fn decode(payload: &[u8]) -> Option<ProcessIdentity> {
    let payload: &[u8; MASTER_GENERATION_PAYLOAD] = payload.try_into().ok()?;
    let pid =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let started_at = u64::from_be_bytes(payload[4..].try_into().ok()?);
    (pid != 0).then(|| ProcessIdentity::new(pid, started_at))
}

#[cfg(test)]
#[path = "../../tests/process/identity/tests.rs"]
mod tests;

/// The exact process behind one worker slot, and the master it belongs to.
///
/// A PID alone is never accepted as a worker's identity: the kernel start
/// stamp defeats PID reuse, the incarnation tells two processes of one slot
/// apart, and the master identity says whose fleet the process was spawned
/// into. A record that names a worker must name all four; one that differs in
/// any of them is about some other process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WorkerIdentity {
    worker_id: WorkerId,
    process: ProcessIdentity,
    incarnation: u64,
    master: MasterIdentity,
}

pub(crate) const WORKER_IDENTITY_BYTES: usize = size_of::<u8>()
    + size_of::<u32>()
    + size_of::<u64>()
    + size_of::<u64>()
    + MASTER_IDENTITY_BYTES;
const MASTER_IDENTITY_BYTES: usize =
    size_of::<u32>() + size_of::<u64>() + size_of::<u64>();

impl WorkerIdentity {
    pub const fn new(
        worker_id: WorkerId,
        process: ProcessIdentity,
        incarnation: u64,
        master: MasterIdentity,
    ) -> Self {
        Self { worker_id, process, incarnation, master }
    }

    pub const fn worker_id(self) -> WorkerId {
        self.worker_id
    }

    pub const fn process(self) -> ProcessIdentity {
        self.process
    }

    pub const fn incarnation(self) -> u64 {
        self.incarnation
    }

    pub const fn master(self) -> MasterIdentity {
        self.master
    }

    pub(crate) const fn with_master(
        self,
        master: MasterIdentity,
    ) -> Self {
        Self { master, ..self }
    }

    /// The first field in which `other` names a different process than this
    /// one, for the diagnostic that says why a record was ignored.
    pub(crate) fn mismatch(
        self,
        other: Self,
    ) -> Option<&'static str> {
        if self.worker_id != other.worker_id {
            Some("worker id")
        } else if self.process.pid() != other.process.pid() {
            Some("pid")
        } else if self.process.started_at() != other.process.started_at() {
            Some("process start stamp")
        } else if self.incarnation != other.incarnation {
            Some("incarnation")
        } else if self.master.process() != other.master.process() {
            Some("master process")
        } else if self.master.generation() != other.master.generation() {
            Some("master generation")
        } else {
            None
        }
    }

    pub(crate) fn encode_into(
        self,
        out: &mut Vec<u8>,
    ) {
        out.push(self.worker_id.raw());
        out.extend_from_slice(&self.process.pid().to_be_bytes());
        out.extend_from_slice(&self.process.started_at().to_be_bytes());
        out.extend_from_slice(&self.incarnation.to_be_bytes());
        out.extend_from_slice(&self.master.process().pid().to_be_bytes());
        out.extend_from_slice(
            &self.master.process().started_at().to_be_bytes(),
        );
        out.extend_from_slice(&self.master.generation().to_be_bytes());
    }

    pub(crate) fn decode(
        bytes: &[u8; WORKER_IDENTITY_BYTES]
    ) -> std::result::Result<Self, String> {
        let u32_at = |at: usize| {
            u32::from_be_bytes(bytes[at..at + 4].try_into().expect("4 bytes"))
        };
        let u64_at = |at: usize| {
            u64::from_be_bytes(bytes[at..at + 8].try_into().expect("8 bytes"))
        };
        let worker_id = bytes[0];
        let pid = u32_at(1);
        let started_at = u64_at(5);
        let incarnation = u64_at(13);
        let master_pid = u32_at(21);
        let master_started_at = u64_at(25);
        let generation = u64_at(33);
        if worker_id == 0
            || pid == 0
            || incarnation == 0
            || master_pid == 0
            || generation == 0
        {
            return Err(
                "worker identity requires non-zero worker id, pids, incarnation and generation"
                    .to_string()
            );
        }
        Ok(Self::new(
            WorkerId::new(worker_id),
            ProcessIdentity::new(pid, started_at),
            incarnation,
            MasterIdentity::new(
                ProcessIdentity::new(master_pid, master_started_at),
                generation,
            ),
        ))
    }
}

/// The worker's own identity, held in the worker's registry: what it publishes
/// about itself and what a targeted request must name before the worker acts.
///
/// Built from the bootstrap the master handed this process and the process's
/// own kernel identity — never from anything a later record says about it.
///
/// This is a non-Runnable capability shared by presence publication and targeted
/// lifecycle handlers. Accepted ownership transfer updates the logical master;
/// restarting a publisher does not reset process identity or runtime start time.
pub struct WorkerIdentityProvider {
    identity: Mutex<WorkerIdentity>,
    /// Wall-clock time this worker's Claviron runtime began, recorded once
    /// here. Operator display data: it is not the kernel start stamp in
    /// `identity`, and nothing resets it.
    runtime_started_at_unix_ms: u64,
    /// Set once the boot acknowledgement has gone out.
    started: AtomicBool,
    /// The first drain this worker accepted. A worker leaves once; whatever
    /// operation asked first owns the teardown that follows.
    accepted_drain: Mutex<Option<OperationId>>,
}

impl WorkerIdentityProvider {
    pub fn from_bootstrap(bootstrap: &WorkerBootstrap) -> Result<Self> {
        let pid = std::process::id();
        let process = process::identity(pid).ok_or_else(|| {
            Error::bootstrap(format!(
                "worker process {pid} cannot read its own kernel identity"
            ))
        })?;
        let runtime_started_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                Error::bootstrap(format!(
                    "worker runtime clock is invalid: {error}"
                ))
            })?
            .as_millis()
            .try_into()
            .map_err(|_| {
                Error::bootstrap(
                    "worker runtime start exceeds the supported range",
                )
            })?;
        Ok(Self::new(
            WorkerIdentity::new(
                bootstrap.worker_id(),
                process,
                bootstrap.incarnation(),
                bootstrap.master(),
            ),
            runtime_started_at_unix_ms,
        ))
    }

    pub(crate) const fn new(
        identity: WorkerIdentity,
        runtime_started_at_unix_ms: u64,
    ) -> Self {
        Self {
            identity: Mutex::new(identity),
            runtime_started_at_unix_ms,
            started: AtomicBool::new(false),
            accepted_drain: Mutex::new(None),
        }
    }

    /// This process as worker `worker_id`, first incarnation, under a master
    /// that is also this process. For tests that need a worker to be somebody.
    #[cfg(test)]
    pub(crate) fn for_tests(worker_id: WorkerId) -> Self {
        let process =
            process::identity(std::process::id()).expect("own identity");
        Self::new(
            WorkerIdentity::new(
                worker_id,
                process,
                1,
                MasterIdentity::new(process, 1),
            ),
            OrbitEpoch::now().as_unix_ms(),
        )
    }

    pub fn identity(&self) -> WorkerIdentity {
        *self.identity.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn runtime_started_at_unix_ms(&self) -> u64 {
        self.runtime_started_at_unix_ms
    }

    /// Boot reached the acknowledgement milestone: providers booted and
    /// runnables submitted.
    pub(crate) fn mark_started(&self) {
        self.started.store(true, Ordering::Release);
    }

    /// What this worker publishes about itself right now.
    pub(crate) fn presence(
        &self,
        state: &AppState,
        captured_at: OrbitEpoch,
    ) -> WorkerPresence {
        let phase = if state.is_shutting_down() {
            PresencePhase::Draining
        } else if self.started.load(Ordering::Acquire) {
            PresencePhase::Running
        } else {
            PresencePhase::Booting
        };
        let parent_pid =
            u32::try_from(nix::unistd::getppid().as_raw()).unwrap_or(0);
        WorkerPresence::new(
            self.identity(),
            self.runtime_started_at_unix_ms,
            phase,
            parent_pid,
            self.accepted_drain(),
            captured_at,
        )
    }

    /// Record the operation whose drain request this worker acts on. Returns
    /// the operation that owns the teardown — the first one, whichever asked.
    pub(crate) fn accept_drain(
        &self,
        operation: OperationId,
    ) -> OperationId {
        let mut accepted = self
            .accepted_drain
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *accepted.get_or_insert(operation)
    }

    pub(crate) fn accepted_drain(&self) -> Option<OperationId> {
        *self
            .accepted_drain
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Accept a new master only when the claim names this exact worker, the
    /// original master is proven dead, and the claimant is a newer live master.
    /// Repeating an already accepted claim is idempotent so a lost ack can be
    /// emitted again.
    pub(crate) fn accept_master_claim(
        &self,
        target: WorkerIdentity,
        claimant: MasterIdentity,
    ) -> std::result::Result<WorkerIdentity, &'static str> {
        let mut current = self
            .identity
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let accepted = target.with_master(claimant);
        if *current == accepted {
            return Ok(accepted);
        }
        if let Some(field) = current.mismatch(target) {
            return Err(field);
        }
        if !current.process().is_alive() {
            return Err("worker process is no longer alive");
        }
        if current.master().process().is_alive() {
            return Err("current master is still alive");
        }
        if claimant.generation() <= current.master().generation() {
            return Err("claimant master generation is not newer");
        }
        if !claimant.process().is_alive() {
            return Err("claimant master is not alive");
        }
        *current = accepted;
        Ok(accepted)
    }
}

#[cfg(test)]
#[path = "../../tests/process/identity/worker_identity_tests.rs"]
mod worker_identity_tests;

/// Which source tree the running supervisor was built from.
///
/// `revision` is the first eight hex digits of the commit `build.rs` found
/// (`None` when it found no repository), `dirty` whether tracked files had
/// uncommitted changes, `crate_version` this crate's `CARGO_PKG_VERSION`. It
/// is stated once at master boot and carried in every worker snapshot, so a
/// binary older than the tree — the 2026-09-13 executable was two hours behind
/// its repository — is visible from the outside without reading log targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BuildIdentity {
    revision: Option<u32>,
    dirty: bool,
    crate_version: &'static str,
}

impl BuildIdentity {
    /// The identity compiled into this crate.
    pub fn current() -> Self {
        Self::parse(
            env!("CLAVIRON_SUPERVISOR_BUILD_REVISION"),
            env!("CARGO_PKG_VERSION"),
        )
    }

    pub(crate) const fn from_wire(
        revision: u32,
        dirty: bool,
    ) -> Self {
        Self {
            revision: if revision == 0 { None } else { Some(revision) },
            dirty,
            crate_version: "",
        }
    }

    fn parse(
        revision: &str,
        crate_version: &'static str,
    ) -> Self {
        let (hash, dirty) = match revision.strip_suffix("-dirty") {
            Some(hash) => (hash, true),
            None => (revision, false),
        };
        let revision = (hash.len() == 8)
            .then(|| u32::from_str_radix(hash, 16).ok())
            .flatten();
        Self { revision, dirty, crate_version }
    }

    /// The revision as eight hex digits, or `None` for a build without one.
    pub const fn revision(self) -> Option<u32> {
        self.revision
    }

    pub const fn dirty(self) -> bool {
        self.dirty
    }

    pub const fn crate_version(self) -> &'static str {
        self.crate_version
    }

    /// Two builds are the same source when their revisions and dirty flags
    /// agree; the crate version is informational and does not cross the wire.
    pub fn same_source(
        self,
        other: Self,
    ) -> bool {
        self.revision == other.revision && self.dirty == other.dirty
    }
}

impl std::fmt::Display for BuildIdentity {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self.revision {
            Some(revision) => write!(f, "{revision:08x}")?,
            None => f.write_str("unknown")?,
        }
        if self.dirty {
            f.write_str("-dirty")?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/process/identity/build_identity_tests.rs"]
mod build_identity_tests;
