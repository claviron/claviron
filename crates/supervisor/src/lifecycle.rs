//! Typed lifecycle model for the processes this master owns.
//!
//! The table used to hold a bare `Child` and a clock. A `Child` is a reapable
//! handle whose PID the kernel hands out again the moment it is reaped, so
//! [`OwnedProcess`] pairs the handle with the kernel start stamp while the
//! process is known to be the one just spawned, and every later observation is
//! matched against that pair rather than against a number.

pub use identity::{
    BuildIdentity, MasterIdentity, WorkerIdentity, WorkerIdentityProvider,
};
pub(crate) use identity::{
    MASTER_GENERATION_RING_KIND, MasterGenerationRecord, WORKER_IDENTITY_BYTES,
    establish_master_identity, master_identity_is_known,
};
pub use model::{
    DrainReason, ExitDisposition, ExitOutcome, FleetMode, WorkerBlockReason,
    WorkerIntent, WorkerObservedPhase,
};
pub(crate) use model::{LaneWatch, TeardownOutcome, WorkerPhase};
pub use operation::{CommandKey, OperationId, OperationKind, OperationOrigin};
pub(crate) use operation::{
    LifecycleOperation, OperationGuard, OperationScope, OperationTable,
    Refusal, RejectionReason, SubmitResult,
};
pub(crate) use process::{
    ExitObservation, OwnedProcess, ProcessExit, ProcessOrigin,
};

use crate::fleet::model;
pub(crate) use crate::fleet::operation;
use crate::process::{identity, owned as process};

pub(crate) mod drain;
pub(crate) mod reconcile;
pub(crate) mod reconciler;
pub(crate) mod recovery;
pub(crate) mod rotation;
pub(crate) mod startup;

pub(crate) mod master;

pub(crate) mod shutdown;
