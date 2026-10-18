//! Provider for the fleet's byte arena: one index over one ring of bytes,
//! opened by every process and preserved across runtime generations by
//! default. What goes in it is the caller's business; the static file service
//! is the first. Its owner may explicitly select a quiescent boot reset, at
//! registration or — once its own configuration is prepared — before boot.

use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{self as runtime_registry};
pub use orbitive::arena::*;

use crate::OrbitStateExt;
use crate::shm::{
    self, ActiveBootPolicy, ExistingBootPolicy, FinalizePolicy,
    ManagedShmHandle, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

const PROVIDER_NAME: &str = "ArenaProvider";
const RESOURCE_NAME: &str = "orbit-arena";

pub struct ArenaResources {
    arena: ArcSwapOption<Arena>,
    boot: ArcSwap<ArenaBootPolicy>,
}

impl ArenaResources {
    fn new(boot: ArenaBootPolicy) -> Self {
        Self {
            arena: ArcSwapOption::const_empty(),
            boot: ArcSwap::from_pointee(boot),
        }
    }

    /// The owner's policy for an arena already on the fleet. Registration
    /// supplies the owner's default; the owner's `configure`, which precedes
    /// every provider boot, declares the configured one here.
    pub fn set_boot_policy(
        &self,
        boot: ArenaBootPolicy,
    ) {
        self.boot.store(Arc::new(boot));
    }
}

/// Runtime owner's policy for a physically valid arena already on the fleet.
/// Physical geometry remains compile-time because it is part of the SHM ABI.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ArenaBootPolicy {
    #[default]
    Preserve,
    ResetWhenQuiescent,
}

pub type ArenaProvider = ManagedShmProvider<ArenaResources>;

impl ManagedShmHandle for ArenaResources {
    type Handle = Arena;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.arena.load_full()
    }
}

impl ManagedShmResources for ArenaResources {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl ManagedShmResource for ArenaResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(ARENA_KIND),
            existing_boot: match **self.boot.load() {
                ArenaBootPolicy::Preserve => ExistingBootPolicy::Reuse,
                ArenaBootPolicy::ResetWhenQuiescent => {
                    ExistingBootPolicy::ResetWhenQuiescent {
                        when_active: ActiveBootPolicy::Reuse,
                    }
                }
            },
            finalize: FinalizePolicy::Preserve,
        }
    }

    fn validate_physical(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<PhysicalShmState> {
        let fleet = fleet(state)?;
        let name = orbitive::shm::ring_segment_name(fleet.name(), ARENA_KIND);
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        self.arena.store(Some(Arc::new(open(state)?)));
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.arena.store(Some(Arc::new(open(state)?)));
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
        let arena = open(state)?;
        arena.reset().map_err(|error| {
            runtime_registry::Error::msg(format!("arena reset failed: {error}"))
        })?;
        self.arena.store(Some(Arc::new(arena)));
        Ok(())
    }
}

pub(crate) fn register(
    state: &AppState,
    boot: ArenaBootPolicy,
) -> runtime_registry::Result<()> {
    shm::register(state, ArenaResources::new(boot))
}

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

fn open(state: &AppState) -> runtime_registry::Result<Arena> {
    Arena::new(fleet(state)?).map_err(|error| {
        runtime_registry::Error::msg(format!("arena boot failed: {error}"))
    })
}
