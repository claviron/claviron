//! Fleet adapter for Continuo's process-local runnable service manager.
//!
//! Continuo remains the sole owner of service generations and lifecycle
//! state. This module only transports named commands through Orbit and joins
//! complete per-process observations for remote management surfaces.

mod manager;
mod membership;
mod publisher;
mod snapshot;

use std::sync::Arc;

pub use crate::config::ServiceSnapshotPolicy;
use claviron_runtime::registry;
use claviron_runtime::{AppState, Register};
pub use manager::{
    FleetServiceManager, FleetServiceManagerError, FleetServiceManagerResult,
};
pub use membership::current_members;
pub use publisher::ServiceSnapshotPublisher;
pub use snapshot::{
    FleetMember, FleetMemberRole, FleetServiceListSnapshot,
    FleetServicePlacement, FleetServiceStatus, FleetServiceStatusSnapshot,
};

use crate::FleetEventRegistry;

pub(crate) const SNAPSHOT_PUBLISHER_NAME: &str = "ServiceSnapshotPublisher";

/// Process-owner supplied membership for service observation. Service batches
/// prove registration and lifecycle, never whether a silent process should exist.
/// Bind this capability through the registry; an unavailable/stale source must
/// return an error rather than treating missing reporters as stopped processes.
pub trait FleetMembership: Send + Sync {
    fn members(
        &self,
        state: &AppState,
    ) -> Result<Vec<FleetMember>, String>;
}

/// Marks fleet service control as registered; a plain registry value.
pub(super) struct ServiceControlRegistration;

/// Register fleet service control before configuration and provider boot.
/// Continuo's local `ServiceManager` must be inserted before boot, but need
/// not exist during application-owned registration.
pub fn register(state: &AppState) -> registry::Result<()> {
    <ServiceSnapshotPublisher as Register<AppState>>::register(state)
}

/// Register fleet service control with an explicit absent-file policy.
/// A present `orbit.yaml` supplies the policy instead. First registration wins.
pub fn register_with_policy(
    state: &AppState,
    policy: ServiceSnapshotPolicy,
) -> registry::Result<()> {
    policy.validate()?;
    if state.registry_ref().resolve::<ServiceControlRegistration>().is_some() {
        return Ok(());
    }
    if !state.is_standalone()
        && state.registry_ref().resolve::<FleetEventRegistry>().is_none()
    {
        return Err(registry::Error::msg(
            "FleetEventRegistry must be registered before fleet service management",
        ));
    }

    manager::register_command_listener(state)?;
    if !state.is_standalone() {
        state.bind_config_environment::<ServiceSnapshotPublisher>()?;
        manager::register_command_bridge(state)?;
        crate::shm::register_reusable_ring::<snapshot::ServiceSnapshotRecord>(
            state,
            "ServiceSnapshotShm",
            "service-snapshot-ring",
        )?;
        state.registry_ref().register(Arc::new(
            ServiceSnapshotPublisher::new(
                state.config_dir().join("orbit.yaml"),
                policy,
            ),
        ))?;
    }
    state.registry_ref().insert(Arc::new(ServiceControlRegistration))?;
    Ok(())
}

#[cfg(test)]
#[path = "../tests/service.rs"]
mod tests;
