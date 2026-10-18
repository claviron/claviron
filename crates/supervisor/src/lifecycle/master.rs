//! Master identity establishment and non-running ownership cleanup.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_orbit::{FleetEventRegistry, FleetProvider};
use claviron_runtime::AppState;
use claviron_runtime::registry::{
    self, Bootable, Component, ComponentOrder, Finalizable,
};
use tracing::{info, warn};

use crate::control::WorkerControl;
use crate::lifecycle::{BuildIdentity, establish_master_identity};

/// Non-Runnable provider for the master's process-lifetime identity and final
/// release of inactive fleet state.
///
/// Boot establishes the master generation and recovers the effective topology;
/// it does not wait for worker convergence. After runtime drain, Finalizable
/// releases exited handles and observations idempotently. It refuses cleanup
/// while spawned workers live and preserves state for surviving adopted workers.
/// It never signals workers, waits for live children, or unlinks their SHM.
pub(crate) struct MasterIdentityProvider {
    control: Arc<WorkerControl>,
}
impl MasterIdentityProvider {
    pub(crate) fn new(control: Arc<WorkerControl>) -> Self {
        Self { control }
    }
}
impl Scoped for MasterIdentityProvider {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}
impl Component<AppState> for MasterIdentityProvider {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "MasterIdentityProvider"
    }
    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<FleetEventRegistry>()
            .after::<crate::config::provider::SupervisorPolicy>()
            .after::<claviron_orbit::shm::ManagedShmProvider<
                claviron_orbit::shm::ReusableOrbitRing<
                    crate::lifecycle::MasterGenerationRecord,
                >,
            >>()
    }
    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<FleetProvider>,)> for MasterIdentityProvider {
    async fn boot(
        &self,
        (fleet_provider,): (Arc<FleetProvider>,),
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        // Registration is shared; only the master owns generations/topology.
        if !state.is_master() {
            return Ok(());
        }
        let fleet = fleet_provider.handle().ok_or_else(|| {
            claviron_runtime::registry::Error::msg(
                "Orbit fleet is unavailable for master identity",
            )
        })?;
        self.control.observer.attach_fleet(fleet.clone())?;
        let master = establish_master_identity(&fleet)?;
        let build = BuildIdentity::current();
        info!(
            server_name = state.name(),
            pid = master.process().pid(),
            start_stamp = master.process().started_at(),
            generation = master.generation(),
            build = %build,
            crate_version = build.crate_version(),
            "👑 Master initialization started"
        );
        // Boot runs once per provider; a second call would be a registry bug,
        // and the first identity stays authoritative either way.
        let _ = self.control.master.set(master);
        crate::lifecycle::reconcile::recover_worker_topology(
            &self.control,
            state,
            &fleet,
        )?;
        Ok(())
    }
}
#[async_trait]
impl Finalizable<AppState> for MasterIdentityProvider {
    async fn finalize(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        if !state.is_master() {
            return Ok(());
        }
        let mut rows = self.control.workers.write().await;
        let live = rows
            .iter()
            .filter_map(|row| row.process.as_ref())
            .filter(|process| process.identity().is_alive())
            .collect::<Vec<_>>();
        if live.iter().any(|process| !process.is_adopted()) {
            return Err(registry::Error::msg(format!(
                "master finalization refused: spawned workers are still alive: {:?}",
                live.iter().map(|process| process.pid()).collect::<Vec<_>>()
            )));
        }
        if !live.is_empty() {
            // Origin policy deliberately leaves these processes running.
            // Never unlink shared state still mapped by those writers.
            warn!(pids = ?live.iter().map(|process| process.pid()).collect::<Vec<_>>(),
                "master finalized without clearing adopted-worker state");
            return Ok(());
        }
        // Exact exited handles can now be reaped and released. No signal,
        // timeout or implicit shared-memory unlink belongs in Finalizable.
        for row in rows.iter_mut() {
            if let Some(process) = row.process.as_mut() {
                process.try_reap()?;
            }
        }
        rows.clear();
        drop(rows);
        self.control.observer.clear().await;
        Ok(())
    }
}
