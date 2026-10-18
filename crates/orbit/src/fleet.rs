use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{
    self as runtime_registry, Bootable, Component, priority,
};
use orbitive::{Fleet, NodeId};

/// Holds the per-process [`Fleet`] handle for Claviron and joins it
/// during the runtime's `boot_all` phase.
/// Every member joins the explicit sharing domain selected by
/// [`AppState::fleet_name`]. Process/application instance identity is
/// independent; different process families may deliberately join one fleet.
pub struct FleetProvider {
    fleet: ArcSwapOption<Fleet>,
}

impl FleetProvider {
    pub fn new() -> Self {
        Self { fleet: ArcSwapOption::const_empty() }
    }

    /// The shared [`Fleet`] handle for this process. `None` until
    /// the provider's `boot` has run.
    pub fn handle(&self) -> Option<Arc<Fleet>> {
        self.fleet.load_full()
    }
}

impl Default for FleetProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl Component<AppState> for FleetProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "FleetProvider"
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState> for FleetProvider {
    async fn boot(
        &self,
        _deps: (),
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        let node_id = NodeId::new(state.fleet_node_id().raw());
        let fleet_capacity = state.fleet_capacity().raw();
        let fleet_name = state.fleet_name();

        // SHM-backed fleet: master and workers in the same process family
        // join the SAME segment namespace and see each
        // other's writes. This is the V1 promise.
        let fleet = Fleet::join_shm_as(fleet_name, fleet_capacity, node_id)
            .map_err(|e| {
                runtime_registry::Error::msg(format!(
                    "Fleet::join_shm failed: {e}"
                ))
            })?;

        let role = state.run_mode().as_env_value();
        let node = fleet.node_id();
        tracing::debug!(
            "🛰  Orbit fleet joined (SHM) — name={} role={role} {node} fleet_capacity={fleet_capacity}",
            fleet.name()
        );

        self.fleet.store(Some(Arc::new(fleet)));
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/fleet.rs"]
mod tests;
