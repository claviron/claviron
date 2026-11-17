//! Provider for the fleet's Orbitals table: `std::sync::atomic` between the
//! fleet's processes.
//!
//! One Orbit table, kind [`ATOMIC_KIND`]. Its life is the fleet's: the master
//! resets it when it boots onto a quiescent fleet, which starts a new epoch
//! and ends every earlier id, and reuses it unchanged while adopted workers
//! keep running, so their handles and ids stay valid. It is kept on exit.
//!
//! There is no Claviron handle type here. A value is declared with
//! [`Orbitals::allocate`] in one process and reached in another with
//! [`Orbitals::open`]; its [`OrbitalId`] travels serialized or through an
//! invocation.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{self as runtime_registry};
pub use orbitive::atomic::*;

use crate::OrbitStateExt;
use crate::shm::{
    self, ActiveBootPolicy, ExistingBootPolicy, FinalizePolicy,
    ManagedShmHandle, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

const PROVIDER_NAME: &str = "OrbitalsProvider";
const RESOURCE_NAME: &str = "orbit-atomic-table";

/// Domain resource set behind the mandatory SHM lifecycle wrapper.
pub struct OrbitalsResources {
    orbitals: ArcSwapOption<Orbitals>,
}

impl OrbitalsResources {
    fn new() -> Self {
        Self { orbitals: ArcSwapOption::const_empty() }
    }
}

/// Lifecycle owner and public capability for the fleet's Orbitals table.
pub type OrbitalsProvider = ManagedShmProvider<OrbitalsResources>;

impl ManagedShmHandle for OrbitalsResources {
    type Handle = Orbitals;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.orbitals.load_full()
    }
}

impl ManagedShmResources for OrbitalsResources {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl ManagedShmResource for OrbitalsResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(ATOMIC_KIND),
            // Slots are never returned within an epoch, so a table nobody
            // holds is emptied rather than inherited. Adopted workers hold
            // handles into it, and a reset does not invalidate a handle: it
            // is reused, same epoch, while any of them runs.
            existing_boot: ExistingBootPolicy::ResetWhenQuiescent {
                when_active: ActiveBootPolicy::Reuse,
            },
            finalize: FinalizePolicy::Preserve,
        }
    }

    fn validate_physical(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<PhysicalShmState> {
        let fleet = fleet(state)?;
        let name = orbitive::shm::ring_segment_name(fleet.name(), ATOMIC_KIND);
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        self.orbitals.store(Some(Arc::new(open(state)?)));
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.orbitals.store(Some(Arc::new(open(state)?)));
        Ok(())
    }

    async fn activity(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ResourceActivity> {
        if state.is_worker() {
            return Ok(ResourceActivity::Active);
        }
        shm::activity(state, self.descriptor()).await
    }

    fn reset(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        let orbitals = open(state)?;
        orbitals.reset_all().map_err(|error| {
            runtime_registry::Error::msg(format!(
                "Orbitals table reset failed: {error}"
            ))
        })?;
        self.orbitals.store(Some(Arc::new(orbitals)));
        Ok(())
    }
}

pub(crate) fn register(state: &AppState) -> runtime_registry::Result<()> {
    shm::register(state, OrbitalsResources::new())
}

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

fn open(state: &AppState) -> runtime_registry::Result<Orbitals> {
    Orbitals::new(fleet(state)?).map_err(|error| {
        runtime_registry::Error::msg(format!(
            "Orbitals table boot failed: {error}"
        ))
    })
}
