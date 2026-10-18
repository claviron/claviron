//! # claviron-runtime
//!
//! Claviron runtime facade over `continuo`.
//!
//! Generic service-composition primitives are provided by `continuo` and
//! re-exported here. Local code owns only Claviron-specific state, run-mode
//! metadata, env snapshotting, and managed process-local event callbacks.
//!
//! The error model (`registry::Error`, `registry::BoxError`, `registry::Result`)
//! lives *inside* the [`registry`] module because it is a registry concern —
//! [`gate`] is an independent primitive with its own semantics and does not
//! share this error type.
pub mod env;
pub mod events;
mod server_name;
mod state;

pub mod activity;
mod application_build;
pub use application_build::ApplicationBuild;

/// Version of the Claviron runtime, distinct from its embedding application.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use congif;
pub use congif::{ConfigReader, HasConfigReader};
pub use continuo::events::{Event, ProcessEventBus, ShutdownInitiated};
pub use continuo::gate::{Gate, GateDrainOutcome, Permit};
pub use continuo::guard::{Guard, GuardGroup};
pub use continuo::registry::{
    AnyComponent, AnyRunnable, Bootable, BoxError, Component, ComponentOrder,
    Error, Finalizable, FinalizeFailure, FinalizeOutcome, MissingDependency,
    Registry, ReloadFailure, ReloadOutcome, ReloadState, Reloadable, Resolve,
    Result, Runnable, ValidationFailure, ValidationOutcome,
};
pub use continuo::{
    HasEvents, HasRegistry, Register, RunContext, ServiceManager,
    ServiceManagerError, ServiceSnapshot, ServiceStartPolicy, ServiceStatus,
    SharedState, gate, guard, registry,
};
pub use events::{EventDispatcher, ProcessStarted, ReopenLogs};
pub use orbitive::FleetName;
pub use server_name::{InvalidServerName, ServerName};
pub use state::{
    ConfigCheck, DEFAULT_FLEET_NAME, FleetCapacity, FleetIncarnation,
    FleetNodeId, LaunchBlob, RunMode, State as AppState, WorkerCount, WorkerId,
};

pub type Runtime = continuo::Runtime<AppState>;

#[cfg(feature = "metrics")]
pub mod metrics;
