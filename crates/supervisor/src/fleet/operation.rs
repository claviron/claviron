//! Who may act on the fleet, or on one slot, right now.
//!
//! One slot is touched by at most one operation at a time, and a fleet-wide
//! operation owns every slot while it runs. Ownership is taken by check-and-set
//! under a short lock — before the operation gets to await anything — so a
//! second command for the same target is answered at once: the same operation
//! again is joined, a different one is refused with the operation that owns
//! the target. Nothing queues behind a lock that a drain may hold for as long
//! as `shutdown_timeout: None` allows.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use claviron_runtime::WorkerId;

/// One accepted operation, numbered in submission order within this master.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OperationId(u64);

impl OperationId {
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Zero on the wire is "no operation"; every accepted operation is
    /// numbered from one.
    pub const fn from_raw(raw: u64) -> Option<Self> {
        if raw == 0 { None } else { Some(Self(raw)) }
    }
}

impl fmt::Display for OperationId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "op {}", self.0)
    }
}

/// The key a submitter attaches to a management command before the master has
/// numbered it, so the client that sent the command can find *its* operation
/// in the snapshot: the master records the key on the operation it accepts.
///
/// Minted where the command is submitted — in the worker hosting the control
/// plane, or in the master for a signal-driven command — from the submitter's
/// worker id, the wall-clock millisecond and a per-process sequence, so two
/// live submitters never mint the same key. It is a correlation key, not the
/// operation's identity: that is the master's [`OperationId`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CommandKey(u64);

static NEXT_COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(0);

impl CommandKey {
    const MILLIS_BITS: u32 = 44;
    const SEQUENCE_BITS: u32 = 12;

    /// A key no other live submitter can mint: `via_worker` (0 for the master)
    /// in the top byte, then the current millisecond and a sequence number.
    pub fn next(via_worker: Option<WorkerId>) -> Self {
        let millis =
            SystemTime::now().duration_since(UNIX_EPOCH).map_or(1, |since| {
                since.as_millis().min(u128::from(u64::MAX)) as u64
            }) & ((1 << Self::MILLIS_BITS) - 1);
        let sequence = NEXT_COMMAND_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            & ((1 << Self::SEQUENCE_BITS) - 1);
        let origin = u64::from(via_worker.map_or(0, WorkerId::raw));
        Self((origin << 56) | (millis.max(1) << Self::SEQUENCE_BITS) | sequence)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Zero on the wire is "no key".
    pub const fn from_raw(raw: u64) -> Option<Self> {
        if raw == 0 { None } else { Some(Self(raw)) }
    }
}

impl fmt::Display for CommandKey {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Who asked for an operation. Distinct from its kind: a rolling restart is a
/// rolling restart whether the GUI, `claviron-client` or `SIGUSR1` asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OperationOrigin {
    /// A management command, carrying the submitter's key. `via_worker` is the
    /// worker whose control plane relayed it; `None` when the master submitted
    /// it itself.
    Management { via_worker: Option<WorkerId>, command: CommandKey },
    /// The supervisor's own monitor: rotation, heartbeat recovery, crash recovery.
    Supervisor,
    /// A process signal to the master: `SIGHUP` config reload, `SIGTERM`/`SIGINT` shutdown.
    Signal,
}

impl fmt::Display for OperationOrigin {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Management { via_worker: Some(worker), command } => {
                write!(f, "management via worker #{worker}, command {command}")
            }
            Self::Management { via_worker: None, command } => {
                write!(f, "management in master, command {command}")
            }
            Self::Supervisor => f.write_str("supervisor"),
            Self::Signal => f.write_str("signal"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OperationKind {
    Start,
    Stop,
    Restart,
    Reload,
    Rotation,
    HeartbeatRecovery,
    CrashRecovery,
    ReloadAll,
    RestartAll,
    Resize { from: u8, to: u8 },
    ConfigReload,
    FleetShutdown,
}

impl fmt::Display for OperationKind {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Start => f.write_str("start"),
            Self::Stop => f.write_str("stop"),
            Self::Restart => f.write_str("restart"),
            Self::Reload => f.write_str("reload"),
            Self::Rotation => f.write_str("rotation"),
            Self::HeartbeatRecovery => f.write_str("heartbeat recovery"),
            Self::CrashRecovery => f.write_str("crash recovery"),
            Self::ReloadAll => f.write_str("reload-workers"),
            Self::RestartAll => f.write_str("restart-workers"),
            Self::Resize { from, to } => write!(f, "resize {from}->{to}"),
            Self::ConfigReload => f.write_str("config reload"),
            Self::FleetShutdown => f.write_str("fleet shutdown"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OperationScope {
    Fleet,
    Slot(WorkerId),
}

impl fmt::Display for OperationScope {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Fleet => f.write_str("the fleet"),
            Self::Slot(worker_id) => write!(f, "worker #{worker_id}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LifecycleOperation {
    pub(crate) id: OperationId,
    pub(crate) kind: OperationKind,
    pub(crate) scope: OperationScope,
    pub(crate) origin: OperationOrigin,
    pub(crate) started_at: Instant,
}

impl fmt::Display for LifecycleOperation {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "{} ({}) on {} by {}",
            self.kind, self.id, self.scope, self.origin
        )
    }
}

/// Why a submission did not start a new operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The same operation already owns the same target; the caller joins it.
    AlreadyInProgress(LifecycleOperation),
    /// A different operation owns the target.
    Conflict(LifecycleOperation),
    /// Nothing new starts once the fleet is shutting down.
    ShuttingDown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RejectionReason {
    OperationRunnerStopped,
    ShuttingDown,
}

/// What a caller learns from submitting a command. Acceptance means the
/// operation is recorded and running, not that it has completed; completion is
/// observed through the worker snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmitResult {
    Accepted(OperationId),
    AlreadyInProgress(LifecycleOperation),
    Conflict(LifecycleOperation),
    Rejected(RejectionReason),
}

impl From<Refusal> for SubmitResult {
    fn from(refusal: Refusal) -> Self {
        match refusal {
            Refusal::AlreadyInProgress(operation) => {
                Self::AlreadyInProgress(operation)
            }
            Refusal::Conflict(operation) => Self::Conflict(operation),
            Refusal::ShuttingDown => {
                Self::Rejected(RejectionReason::ShuttingDown)
            }
        }
    }
}

/// Lock-protected operation claims and sequence counter for one master lifetime.
/// Keeps fleet, per-slot and overriding shutdown claims in one conflict domain.
#[derive(Default)]
struct Ownership {
    fleet: Option<LifecycleOperation>,
    slots: BTreeMap<WorkerId, LifecycleOperation>,
    /// The fleet shutdown, once begun. It owns nothing in this table — it
    /// outranks everything in it — but it is an operation the workers are
    /// told about and answer to, so it has a number here.
    shutdown: Option<LifecycleOperation>,
    next: u64,
}

/// The live table of operations. Cheap to consult, never held across an await.
#[derive(Clone, Default)]
pub(crate) struct OperationTable {
    ownership: Arc<Mutex<Ownership>>,
}

impl OperationTable {
    fn lock(&self) -> MutexGuard<'_, Ownership> {
        self.ownership.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Take `scope` for `kind`, or say who has it. The guard releases the scope
    /// when dropped, so an operation that fails or panics cannot keep it.
    pub(crate) fn try_acquire(
        &self,
        scope: OperationScope,
        kind: OperationKind,
        origin: OperationOrigin,
    ) -> Result<OperationGuard, Refusal> {
        let mut ownership = self.lock();
        let blocking = match scope {
            OperationScope::Fleet => ownership
                .fleet
                .or_else(|| ownership.slots.values().next().copied()),
            OperationScope::Slot(worker_id) => ownership
                .fleet
                .or_else(|| ownership.slots.get(&worker_id).copied()),
        };
        if let Some(active) = blocking {
            return Err(if active.scope == scope && active.kind == kind {
                Refusal::AlreadyInProgress(active)
            } else {
                Refusal::Conflict(active)
            });
        }

        ownership.next += 1;
        let operation = LifecycleOperation {
            id: OperationId(ownership.next),
            kind,
            scope,
            origin,
            started_at: Instant::now(),
        };
        match scope {
            OperationScope::Fleet => ownership.fleet = Some(operation),
            OperationScope::Slot(worker_id) => {
                ownership.slots.insert(worker_id, operation);
            }
        }
        Ok(OperationGuard { ownership: Arc::clone(&self.ownership), operation })
    }

    pub(crate) fn fleet_operation(&self) -> Option<LifecycleOperation> {
        self.lock().fleet
    }

    /// Number the fleet shutdown. Never refused: shutdown has precedence over
    /// whatever owns the fleet or its slots, and those operations end under
    /// it. Idempotent — the first number stands.
    pub(crate) fn begin_fleet_shutdown(&self) -> LifecycleOperation {
        let mut ownership = self.lock();
        if let Some(shutdown) = ownership.shutdown {
            return shutdown;
        }
        ownership.next += 1;
        let shutdown = LifecycleOperation {
            id: OperationId(ownership.next),
            kind: OperationKind::FleetShutdown,
            scope: OperationScope::Fleet,
            origin: OperationOrigin::Signal,
            started_at: Instant::now(),
        };
        ownership.shutdown = Some(shutdown);
        shutdown
    }

    pub(crate) fn fleet_shutdown(&self) -> Option<LifecycleOperation> {
        self.lock().shutdown
    }

    pub(crate) fn slot_operation(
        &self,
        worker_id: WorkerId,
    ) -> Option<LifecycleOperation> {
        self.lock().slots.get(&worker_id).copied()
    }

    /// Every operation running now, fleet first.
    pub(crate) fn active(&self) -> Vec<LifecycleOperation> {
        let ownership = self.lock();
        ownership
            .fleet
            .into_iter()
            .chain(ownership.slots.values().copied())
            .collect()
    }
}

/// Ownership of one scope for the life of one operation.
pub(crate) struct OperationGuard {
    ownership: Arc<Mutex<Ownership>>,
    operation: LifecycleOperation,
}

impl OperationGuard {
    pub(crate) const fn operation(&self) -> LifecycleOperation {
        self.operation
    }
}

impl fmt::Debug for OperationGuard {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_tuple("OperationGuard").field(&self.operation).finish()
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        let mut ownership = self
            .ownership
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.operation.scope {
            OperationScope::Fleet => {
                if ownership
                    .fleet
                    .is_some_and(|active| active.id == self.operation.id)
                {
                    ownership.fleet = None;
                }
            }
            OperationScope::Slot(worker_id) => {
                if ownership
                    .slots
                    .get(&worker_id)
                    .is_some_and(|active| active.id == self.operation.id)
                {
                    ownership.slots.remove(&worker_id);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/fleet/operation/tests.rs"]
mod tests;
