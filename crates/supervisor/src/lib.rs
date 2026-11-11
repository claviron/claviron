mod bootstrap;
pub mod config;

mod error;

mod lifecycle;

mod registry;

pub mod signals;

mod fleet;
mod management;
mod monitor;
mod observation;
mod process;

pub use bootstrap::{ProcessEntry, WORKER_BOOTSTRAP_ENV, WorkerBootstrap};
pub use config::{
    SupervisorConfig, SupervisorConfigSource, configured_signals,
};
pub use control::{WorkerControl, WorkerControl as WorkerSupervisor};
pub use error::{Error, Result};
pub use fleet::WorkerFleet;
mod events;
use events::{ownership, reload, startup};
use lifecycle::shutdown;
use management::{control, manager, server};
#[cfg(feature = "metrics")]
use observation::metrics;
use observation::{
    heartbeat, heartbeat_observer as observer, membership, presence, publisher,
    snapshot,
};

#[cfg(test)]
#[path = "../tests/generations.rs"]
mod generation_tests;
pub use events::startup::WorkersBootConfirmed;
pub use lifecycle::{
    BuildIdentity, CommandKey, DrainReason, ExitDisposition, ExitOutcome,
    FleetMode, MasterIdentity, OperationId, OperationKind, OperationOrigin,
    WorkerBlockReason, WorkerIdentity, WorkerIdentityProvider, WorkerIntent,
    WorkerObservedPhase,
};
pub use manager::{
    WorkerManager, WorkerManagerError, WorkerManagerResult, WorkerResizePlan,
};
pub use presence::{PresencePhase, WorkerPresence, latest_worker_presence};
pub use registry::register;
pub use server::{
    ServerManager, ServerManagerError, ServerManagerResult, ServerReloaded,
};
pub use shutdown::{
    WorkersDraining, WorkersExited, publish_worker_teardown_completed,
};
pub use snapshot::{
    MasterRuntimeSnapshot, WorkerLifecycle, WorkerListSnapshot,
    WorkerOperation, WorkerOwnership, WorkerStatus, WorkerStatusSnapshot,
};
