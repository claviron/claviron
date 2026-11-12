//! Provider for the fleet's byte arena: one index over one ring of bytes,
//! opened by every process and preserved across runtime generations by
//! default. What goes in it is the caller's business; the static file service
//! is the first. Its owner may explicitly select a quiescent boot reset, at
//! registration or — once its own configuration is prepared — before boot.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{self as runtime_registry};
pub use orbitive::arena::*;

use crate::OrbitStateExt;
use crate::cache::{CacheService, SHARED_CACHE_KIND};
use crate::shm::{
    self, ActiveBootPolicy, ExistingBootPolicy, FinalizePolicy,
    ManagedShmHandle, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

const PROVIDER_NAME: &str = "ArenaProvider";
const RESOURCE_NAME: &str = "orbit-arena";

pub struct ArenaResources<const KIND: u8 = ARENA_KIND> {
    explicitly_required: AtomicBool,
    arena: ArcSwapOption<Arena>,
    boot: ArcSwap<ArenaBootPolicy>,
}

impl<const KIND: u8> ArenaResources<KIND> {
    fn new(
        boot: ArenaBootPolicy,
        explicitly_required: bool,
    ) -> Self {
        Self {
            explicitly_required: AtomicBool::new(explicitly_required),
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

impl<const KIND: u8> ManagedShmHandle for ArenaResources<KIND> {
    type Handle = Arena;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.arena.load_full()
    }
}

impl<const KIND: u8> ManagedShmResources for ArenaResources<KIND> {
    fn name(&self) -> &'static str {
        if KIND == ARENA_KIND {
            PROVIDER_NAME
        } else {
            "SharedCacheArenaProvider"
        }
    }

    fn enabled(
        &self,
        state: &AppState,
    ) -> bool {
        self.explicitly_required.load(Ordering::Relaxed)
            || state.registry_ref().resolve::<CacheService>().is_some_and(
                |cache| {
                    if KIND == ARENA_KIND {
                        cache.has_replicated_stores()
                    } else {
                        cache.has_shm_stores()
                    }
                },
            )
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl<const KIND: u8> ManagedShmResource for ArenaResources<KIND> {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: self.name(),
            resource: if KIND == ARENA_KIND {
                RESOURCE_NAME
            } else {
                "shared-cache-arena"
            },
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(KIND),
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
        let name = orbitive::shm::ring_segment_name(fleet.name(), KIND);
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        self.arena.store(Some(Arc::new(open::<KIND>(state)?)));
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.arena.store(Some(Arc::new(open::<KIND>(state)?)));
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
        let arena = open::<KIND>(state)?;
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
    if let Some(provider) = state.registry_ref().resolve::<ArenaProvider>() {
        provider.resources().explicitly_required.store(true, Ordering::Relaxed);
        return Ok(());
    }
    shm::register(state, ArenaResources::<ARENA_KIND>::new(boot, true))
}

pub(crate) fn register_cache_arenas(
    state: &AppState
) -> runtime_registry::Result<()> {
    shm::register(
        state,
        ArenaResources::<ARENA_KIND>::new(ArenaBootPolicy::Preserve, false),
    )?;
    shm::register(
        state,
        ArenaResources::<SHARED_CACHE_KIND>::new(
            ArenaBootPolicy::Preserve,
            false,
        ),
    )
}

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

fn open<const KIND: u8>(state: &AppState) -> runtime_registry::Result<Arena> {
    Arena::open_kind(fleet(state)?, KIND).map_err(|error| {
        runtime_registry::Error::msg(format!("arena boot failed: {error}"))
    })
}
