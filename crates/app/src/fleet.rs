mod command;
mod manager;
mod publisher;
mod snapshot;

use std::sync::Arc;

use claviron_runtime::AppState;
use claviron_runtime::registry;
pub use manager::{
    FleetApplicationManager, FleetApplicationManagerError,
    FleetApplicationManagerResult,
};
pub use publisher::ApplicationSnapshotPolicy;
pub use snapshot::{
    FleetApplicationListSnapshot, FleetApplicationPlacement,
    FleetApplicationStatus, FleetApplicationStatusSnapshot,
};

/// Encode this process's authoritative application snapshot for a Link lane.
/// Returns `None` when this runner has no application manager.
pub fn link_snapshot_rows(
    state: &AppState
) -> Result<Option<Vec<Vec<u8>>>, String> {
    let Some(manager) =
        state.registry_ref().resolve::<crate::ApplicationManager>()
    else {
        return Ok(None);
    };
    let member = local_member(state);
    let local = snapshot::local(state, manager.list());
    snapshot::encode_local_rows(&local, member).map(Some)
}

/// Encode an application snapshot for a Link lane, for a process that keeps
/// its own applications rather than an `ApplicationManager` — an application
/// joined to the Link from outside a Claviron runtime. `member` says which
/// node of the fleet reports it.
pub fn encode_link_snapshot_rows(
    snapshot: &FleetApplicationListSnapshot,
    member: claviron_orbit::service::FleetMember,
) -> Result<Vec<Vec<u8>>, String> {
    snapshot::encode_local_rows(snapshot, member)
}

/// Decode one complete runner-lane report using the same row format as the
/// ordinary application fleet snapshot ring.
pub fn decode_link_snapshot_rows(
    rows: &[Vec<u8>]
) -> Result<FleetApplicationListSnapshot, String> {
    snapshot::decode_link_rows(rows)
}

struct ApplicationFleetControlRegistration;

/// Bind application commands to the already-registered Orbit event transport.
///
/// Claviron remains responsible for initializing Orbit. This registration only
/// adds the application command listener and its supervised fleet bridge.
pub fn register(state: &AppState) -> registry::Result<()> {
    register_with_policy(state, ApplicationSnapshotPolicy::default())
}

pub fn register_with_policy(
    state: &AppState,
    policy: ApplicationSnapshotPolicy,
) -> registry::Result<()> {
    policy.validate()?;
    if state
        .registry_ref()
        .resolve::<ApplicationFleetControlRegistration>()
        .is_some()
    {
        return Ok(());
    }
    if state.registry_ref().resolve::<crate::ApplicationManager>().is_none() {
        return Err(registry::Error::msg(
            "ApplicationManager must be registered before fleet application control",
        ));
    }
    if !state.is_standalone()
        && state
            .registry_ref()
            .resolve::<claviron_orbit::FleetEventRegistry>()
            .is_none()
    {
        return Err(registry::Error::msg(
            "FleetEventRegistry must be registered before fleet application control",
        ));
    }

    command::register_listener(state)?;
    if !state.is_standalone() {
        command::register_bridge(state)?;
        claviron_orbit::shm::register_reusable_ring::<
            snapshot::ApplicationSnapshotRecord,
        >(state, "ApplicationSnapshotShm", "application-snapshot-ring")?;
        state.registry_ref().register(Arc::new(
            publisher::ApplicationSnapshotPublisher::new(state, policy),
        ))?;
    }
    state
        .registry_ref()
        .insert(Arc::new(ApplicationFleetControlRegistration))?;
    Ok(())
}

fn local_member(state: &AppState) -> claviron_orbit::service::FleetMember {
    use claviron_orbit::service::{FleetMember, FleetMemberRole};
    FleetMember {
        node_id: state.fleet_node_id().raw(),
        role: match state.run_mode() {
            claviron_runtime::RunMode::Master => FleetMemberRole::Master,
            claviron_runtime::RunMode::Worker(_) => FleetMemberRole::Worker,
            claviron_runtime::RunMode::Standalone => {
                FleetMemberRole::Standalone
            }
        },
    }
}

#[cfg(test)]
#[path = "../tests/fleet.rs"]
mod tests;
