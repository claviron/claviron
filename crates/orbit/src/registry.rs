use std::sync::Arc;

use claviron_runtime::AppState;
use orbitive::events::DefaultEventLayout;

use crate::{
    ApplicationEventLayout, ApplicationEventRegistry, ArenaBootPolicy,
    ArenaProvider, CounterProvider, FleetEventRegistry, FleetProvider,
    InvocationProvider, StreamsProvider,
};

/// Register the fleet and its common typed-cell infrastructure.
///
/// Call from `bootstrap::register` at the appropriate point. Explicitly
/// checks for an existing provider so repeated feature-level registration is
/// idempotent without relying on registry replacement semantics.
pub fn register_fleet(state: &AppState) -> claviron_runtime::Result<()> {
    if state.registry_ref().resolve::<FleetProvider>().is_none() {
        state.registry_ref().register(Arc::new(FleetProvider::new()))?;
    }
    crate::cell::register(state)
}

/// Register the optional keyed counter capability and its Fleet dependency.
/// The fleet's byte arena: one store of bodies, read in place by every process.
pub fn register_arena(state: &AppState) -> claviron_runtime::Result<()> {
    register_arena_with_policy(state, ArenaBootPolicy::Preserve)
}

/// Register the fleet arena with the owning domain's existing-segment policy.
/// The first registration wins, as there is one physical arena per fleet.
pub fn register_arena_with_policy(
    state: &AppState,
    policy: ArenaBootPolicy,
) -> claviron_runtime::Result<()> {
    register_fleet(state)?;
    if state.registry_ref().resolve::<ArenaProvider>().is_none() {
        crate::arena::register(state, policy)?;
    }
    Ok(())
}

/// Typed cells: one table per fleet, shared by every runtime on it.
pub fn register_cells(state: &AppState) -> claviron_runtime::Result<()> {
    register_fleet(state)
}

/// The fleet's byte streams: one table, kind `246`, every process creating in
/// its own lane and opening in anyone's.
pub fn register_streams(state: &AppState) -> claviron_runtime::Result<()> {
    register_fleet(state)?;
    if state.registry_ref().resolve::<StreamsProvider>().is_none() {
        crate::stream::register(state)?;
    }
    Ok(())
}

pub fn register_counter(state: &AppState) -> claviron_runtime::Result<()> {
    register_fleet(state)?;
    if state.registry_ref().resolve::<CounterProvider>().is_none() {
        crate::counter::register(state)?;
    }
    Ok(())
}

/// Register the optional invocation transport and its Fleet dependency.
pub fn register_invoke(state: &AppState) -> claviron_runtime::Result<()> {
    register_fleet(state)?;
    crate::invoke::register_shm(state)?;
    if state.registry_ref().resolve::<InvocationProvider>().is_none() {
        state.registry_ref().register(Arc::new(InvocationProvider::new()))?;
    }
    Ok(())
}

/// Register the default Orbit lock table and transition-ring lifecycle.
pub fn register_lock(state: &AppState) -> claviron_runtime::Result<()> {
    register_fleet(state)?;
    crate::lock::register_shm(state)
}

pub fn register(state: &AppState) -> claviron_runtime::Result<()> {
    register_fleet(state)?;
    if !state.is_standalone() {
        crate::event::register_shm::<DefaultEventLayout>(state)?;
        if state.registry_ref().resolve::<FleetEventRegistry>().is_none() {
            state
                .registry_ref()
                .register(Arc::new(FleetEventRegistry::new()))?;
        }
        // A log reopen asked of the master reaches its workers.
        if let Some(events) =
            state.registry_ref().resolve::<FleetEventRegistry>()
        {
            events.register_bridge::<claviron_runtime::ReopenLogs>(state)?;
        }
    }
    if state
        .registry_ref()
        .resolve::<crate::transport::LinkInterest>()
        .is_none()
    {
        state
            .registry_ref()
            .insert(Arc::new(crate::transport::LinkInterest::default()))?;
    }
    if !state.is_standalone() {
        crate::event::register_shm::<ApplicationEventLayout>(state)?;
        if state.registry_ref().resolve::<ApplicationEventRegistry>().is_none()
        {
            state
                .registry_ref()
                .register(Arc::new(ApplicationEventRegistry::new()))?;
        }
    }

    // Reaching an application process over shared memory. Registered always
    // and joined never, until a route asks for it: the fleet's segments are
    // sized for every participant, and a server with no application route
    // should not create them.
    if state
        .registry_ref()
        .resolve::<crate::transport::TransportService>()
        .is_none()
    {
        state.registry_ref().register(Arc::new(
            crate::transport::TransportService::default(),
        ))?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/registry.rs"]
mod tests;
