//! Lifecycle facts that are not process handles: what the master wants of a
//! slot, what it has observed of the slot's process, and the projection of
//! the two that operators see.
//!
//! The two are kept apart on purpose. "Stop requested" is a wish; "the process
//! was reaped" is a fact; and whether that fact was the wished-for outcome is
//! decided from the wish that stood at the moment it happened — not from a
//! label that tried to carry both.

use std::time::Instant;

use crate::presence::WorkerPresence;
use crate::snapshot::WorkerLifecycle;

/// Where an unclaimed slot's node lane stood when the slot was last looked
/// at, and when that was.
///
/// The two fields are the whole watch: a head that has moved since means a
/// publisher is really there, and one that has not, for longer than a
/// heartbeat interval, means nobody is. It is its own clock on purpose — an
/// unclaimed slot has no process, so there is no spawn clock to borrow, and
/// borrowing one would make a single field mean two things.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LaneWatch {
    pub(crate) head: u64,
    pub(crate) since: Instant,
}

impl LaneWatch {
    pub(crate) fn starting_at(head: u64) -> Self {
        Self { head, since: Instant::now() }
    }
}

/// Whether the fleet is being ended. Latched once, atomically, before the
/// master awaits, publishes or locks anything on the way to shutdown, so no
/// operation can slip in between the decision and its visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FleetMode {
    Running,
    ShuttingDown,
}

/// What the master wants of one slot. Desired state, never an observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WorkerIntent {
    /// A process of this slot should be running. One that leaves on its own
    /// has failed and is recovered within the restart budget.
    EnsureRunning,
    /// The slot should hold no process. Nothing is spawned for it; a manually
    /// stopped slot and a slot retired by a shrink both rest here.
    Stopped,
    /// The current process should come down for `reason`. `after_exit` is the
    /// intent the slot takes once that process has been seen to exit.
    Drain { reason: DrainReason, after_exit: ExitDisposition },
}

impl WorkerIntent {
    pub(crate) const fn is_drain(self) -> bool {
        matches!(self, Self::Drain { .. })
    }

    /// The process of a slot with this intent has been reaped: how the exit is
    /// classified, and what the slot wants next.
    ///
    /// Only the intent decides. A drain for a planned reason produces a planned
    /// exit; a drain because the worker hung is the recovery of a failure and
    /// is counted as one; a process that left while it was meant to be running
    /// failed. A `Drain` then yields to its `after_exit`; the other intents
    /// stand.
    pub(crate) const fn on_exit(self) -> ExitTransition {
        match self {
            Self::EnsureRunning => ExitTransition {
                outcome: ExitOutcome::Failed,
                next: Self::EnsureRunning,
            },
            Self::Stopped => ExitTransition {
                outcome: ExitOutcome::Planned,
                next: Self::Stopped,
            },
            Self::Drain { reason, after_exit } => ExitTransition {
                outcome: if reason.is_failure() {
                    ExitOutcome::Failed
                } else {
                    ExitOutcome::Planned
                },
                next: after_exit.next_intent(),
            },
        }
    }
}

/// Pure result of classifying process exit against the slot's desired intent.
/// Pairs the exit outcome with the next intent so reaping cannot update one
/// without the corresponding lifecycle decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExitTransition {
    pub(crate) outcome: ExitOutcome,
    pub(crate) next: WorkerIntent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DrainReason {
    Shutdown,
    ScaleDown,
    Rotation,
    HeartbeatTimeout,
    ManualStop,
    RollingRestart,
}

impl DrainReason {
    /// A drain that exists because the worker stopped answering is the
    /// recovery of a failure, not a planned replacement.
    pub(crate) const fn is_failure(self) -> bool {
        matches!(self, Self::HeartbeatTimeout)
    }

    pub(crate) const fn code(self) -> u8 {
        match self {
            Self::Shutdown => 1,
            Self::ScaleDown => 2,
            Self::Rotation => 3,
            Self::HeartbeatTimeout => 4,
            Self::ManualStop => 5,
            Self::RollingRestart => 6,
        }
    }

    pub(crate) const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Shutdown),
            2 => Some(Self::ScaleDown),
            3 => Some(Self::Rotation),
            4 => Some(Self::HeartbeatTimeout),
            5 => Some(Self::ManualStop),
            6 => Some(Self::RollingRestart),
            _ => None,
        }
    }
}

/// How a worker's managed teardown ended. Neither value says the process has
/// exited; that is a separate observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TeardownOutcome {
    /// Every managed runnable drained and every finalizer succeeded.
    Completed,
    /// The teardown ran to its end, but this many finalizers reported errors.
    FinalizersFailed { count: u16 },
}

/// What a slot becomes once the process it was draining has exited.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExitDisposition {
    /// The slot rests as `Stopped`.
    StayStopped,
    /// A new incarnation of the slot is wanted.
    Replace,
    /// The slot leaves the configured fleet and rests as a stopped tombstone.
    RemoveSlot,
    /// The fleet is shutting down; nothing replaces the process.
    FinishFleetShutdown,
}

impl ExitDisposition {
    pub(crate) const fn next_intent(self) -> WorkerIntent {
        match self {
            Self::Replace => WorkerIntent::EnsureRunning,
            Self::StayStopped
            | Self::RemoveSlot
            | Self::FinishFleetShutdown => WorkerIntent::Stopped,
        }
    }
}

/// What the master has observed of the slot's process. Never desired state:
/// a draining worker is still `Running` here until it is seen to exit, and
/// only the intent says that it is on its way out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkerPhase {
    /// No process; the slot's lane is being watched before it is filled.
    Unclaimed {
        watch: LaneWatch,
    },
    /// A live worker of an earlier, dead master has been challenged to accept
    /// this master. No process handle is installed until its acknowledgement
    /// (or its worker-written presence naming this master) is observed.
    OwnershipPending {
        presence: WorkerPresence,
        requested_at: Instant,
    },
    /// Reconciliation could not prove safe adoption or safe reclamation. The
    /// exact evidence and the reason remain attached to the slot so shutdown
    /// and diagnostics cannot forget a process merely because no handle was
    /// installed.
    ReconciliationBlocked {
        presence: WorkerPresence,
        reason: WorkerBlockReason,
    },
    Starting,
    Running,
    Reloading,
    /// The worker's runtime has acknowledged a drain request. It is still
    /// running: its work is being brought down, not gone.
    DrainAcknowledged,
    /// The worker reported its managed teardown finished. The process still
    /// exists until the kernel says otherwise.
    TeardownCompleted,
    /// The process has been reaped. `outcome` records, at that moment, whether
    /// the exit was the one the intent asked for; the disposition that ran
    /// afterwards may since have changed the intent.
    Exited {
        outcome: ExitOutcome,
    },
}

impl WorkerPhase {
    /// A process is known to exist behind the slot.
    pub(crate) const fn is_live(self) -> bool {
        matches!(
            self,
            Self::Starting
                | Self::Running
                | Self::Reloading
                | Self::DrainAcknowledged
                | Self::TeardownCompleted
        )
    }

    /// A process may occupy the slot, but this master does not own a handle to
    /// it yet. Its absence from `WorkerProcess::process` is not an exit.
    pub(crate) const fn is_unowned(self) -> bool {
        matches!(
            self,
            Self::Unclaimed { .. }
                | Self::OwnershipPending { .. }
                | Self::ReconciliationBlocked { .. }
        )
    }

    /// The phase without its process-local evidence: what a reader outside
    /// this master can be told.
    pub(crate) const fn observed(self) -> WorkerObservedPhase {
        match self {
            Self::Unclaimed { .. } => WorkerObservedPhase::Unclaimed,
            Self::OwnershipPending { .. } => {
                WorkerObservedPhase::OwnershipPending
            }
            Self::ReconciliationBlocked { .. } => {
                WorkerObservedPhase::ReconciliationBlocked
            }
            Self::Starting => WorkerObservedPhase::Starting,
            Self::Running => WorkerObservedPhase::Running,
            Self::Reloading => WorkerObservedPhase::Reloading,
            Self::DrainAcknowledged => WorkerObservedPhase::DrainAcknowledged,
            Self::TeardownCompleted => WorkerObservedPhase::TeardownCompleted,
            Self::Exited { outcome } => WorkerObservedPhase::Exited(outcome),
        }
    }

    /// Why a reconciliation is blocked, when it is.
    pub(crate) const fn block_reason(self) -> Option<WorkerBlockReason> {
        match self {
            Self::ReconciliationBlocked { reason, .. } => Some(reason),
            _ => None,
        }
    }

    /// The worker-written presence a slot without a handle is judged by: the
    /// exact process that occupies the lane, whether this master is asking it
    /// to accept ownership or has refused to.
    pub(crate) const fn evidence(self) -> Option<WorkerPresence> {
        match self {
            Self::OwnershipPending { presence, .. }
            | Self::ReconciliationBlocked { presence, .. } => Some(presence),
            _ => None,
        }
    }

    /// Whether `other` is the same phase as far as anyone outside this master
    /// can tell: same published state, same block reason. The lane watch, the
    /// presence evidence and the claim clock are this master's working data
    /// and refresh without the phase having changed.
    pub(crate) fn same_observation(
        self,
        other: Self,
    ) -> bool {
        self.observed() == other.observed()
            && self.block_reason() == other.block_reason()
    }
}

/// Published worker phase: the internal phase's states, without the lane watch,
/// presence record or request clock that only this master can act on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WorkerObservedPhase {
    Unclaimed,
    OwnershipPending,
    ReconciliationBlocked,
    Starting,
    Running,
    Reloading,
    DrainAcknowledged,
    TeardownCompleted,
    Exited(ExitOutcome),
}

impl WorkerObservedPhase {
    /// A process is known to exist behind the slot.
    pub const fn is_live(self) -> bool {
        matches!(
            self,
            Self::Starting
                | Self::Running
                | Self::Reloading
                | Self::DrainAcknowledged
                | Self::TeardownCompleted
        )
    }
}

/// Why boot reconciliation refused to adopt or reclaim a slot whose lane
/// carried typed presence. The slot stays `ReconciliationBlocked` with this
/// reason until an operator or a later boot resolves it; nothing is spawned
/// over it and nothing is signalled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WorkerBlockReason {
    /// The presence record is older than the heartbeat timeout allows.
    StalePresence,
    /// The worker reports a phase other than `Running`.
    WorkerNotRunning,
    /// The worker is already acting on a lifecycle operation.
    WorkerBusy,
    /// The worker's original master is absent from the generation ring.
    UnknownMaster,
    /// The worker's original master process is still alive.
    MasterStillAlive,
    /// The worker's original master is not older than this master.
    MasterNotOlder,
    /// The process on the lane changed identity while this master's ownership
    /// claim was pending; whatever answers now is not the process it asked.
    IdentityChanged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExitOutcome {
    /// Asked for by an operator, a rotation, a resize or the fleet shutdown.
    Planned,
    /// The process left on its own, or had to be drained because it hung; the
    /// recovery budget applies.
    Failed,
}

impl WorkerLifecycle {
    /// The operator-facing label for one slot. A projection of the pair, owned
    /// by neither: the table keeps intent and phase, and this is derived every
    /// time it is asked for.
    pub(crate) const fn project(
        phase: WorkerPhase,
        intent: WorkerIntent,
    ) -> Self {
        Self::from_observation(phase.observed(), intent)
    }

    /// The same projection from the published pair, so a reader of the
    /// snapshot derives exactly the label the master would.
    pub const fn from_observation(
        phase: WorkerObservedPhase,
        intent: WorkerIntent,
    ) -> Self {
        match (phase, intent) {
            (
                WorkerObservedPhase::Unclaimed
                | WorkerObservedPhase::OwnershipPending
                | WorkerObservedPhase::ReconciliationBlocked,
                _,
            ) => Self::Unclaimed,
            (
                WorkerObservedPhase::Starting
                | WorkerObservedPhase::Running
                | WorkerObservedPhase::Reloading,
                WorkerIntent::Drain { .. },
            )
            | (
                WorkerObservedPhase::DrainAcknowledged
                | WorkerObservedPhase::TeardownCompleted,
                _,
            ) => Self::Stopping,
            (WorkerObservedPhase::Starting, _) => Self::Starting,
            (WorkerObservedPhase::Running, _) => Self::Running,
            (WorkerObservedPhase::Reloading, _) => Self::Reloading,
            // The operator said stop; however the process went, the slot is stopped.
            (WorkerObservedPhase::Exited(_), WorkerIntent::Stopped) => {
                Self::Stopped
            }
            (WorkerObservedPhase::Exited(ExitOutcome::Failed), _) => {
                Self::Crashed
            }
            (WorkerObservedPhase::Exited(ExitOutcome::Planned), _) => {
                Self::Exited
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/fleet/model/tests.rs"]
mod tests;
