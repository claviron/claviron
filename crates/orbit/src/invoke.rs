//! Claviron lifecycle binding for Orbit's invocation ring.
//!
//! The transport -- ring kind and geometry, `InvocationId`, the envelope, the
//! codec trait, cursors, lag reporting and the async subscription -- lives in
//! `orbit-invoke` and is re-exported here unchanged. This module adds only the
//! lifecycle: a managed SHM companion validates/creates/reuses the ring after
//! the fleet, then `InvocationProvider` opens the bus without resetting it.
//! Executors, placement and any claim or result protocol still belong to the
//! runtime adapter above.

use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{self, Bootable, Component, ComponentOrder};
pub use orbitive::invoke::*;

use crate::shm::{
    ExistingBootPolicy, FinalizePolicy, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, OrbitRingResource,
};
use crate::{FleetProvider, shm};

#[derive(Clone)]
struct ManagedInvocationRecord(PhantomData<()>);

impl orbitive::OrbitTyped for ManagedInvocationRecord {
    const KIND: u8 = INVOCATION_RING_KIND;
    const RING_SPEC: orbitive::RingSpec = INVOCATION_RING_SPEC;
}

pub(crate) struct InvocationShmResources {
    ring: OrbitRingResource<ManagedInvocationRecord>,
}

impl InvocationShmResources {
    fn new() -> Self {
        Self {
            ring: OrbitRingResource::new(
                "InvocationShmProvider",
                "invocation-ring",
                ExistingBootPolicy::Reuse,
                FinalizePolicy::Preserve,
            ),
        }
    }
}

impl ManagedShmResources for InvocationShmResources {
    fn name(&self) -> &'static str {
        "InvocationShmProvider"
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![&self.ring]
    }
}

pub(crate) fn register_shm(state: &AppState) -> registry::Result<()> {
    shm::register(state, InvocationShmResources::new())
}

/// Lifecycle owner for Claviron's optional invocation transport.
pub struct InvocationProvider {
    bus: ArcSwapOption<InvocationBus>,
}

impl InvocationProvider {
    pub fn new() -> Self {
        Self { bus: ArcSwapOption::const_empty() }
    }

    pub fn handle(&self) -> Option<Arc<InvocationBus>> {
        self.bus.load_full()
    }
}

impl Default for InvocationProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl Component<AppState> for InvocationProvider {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "InvocationProvider"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<ManagedShmProvider<InvocationShmResources>>()
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<FleetProvider>,)> for InvocationProvider {
    async fn boot(
        &self,
        (fleet_provider,): (Arc<FleetProvider>,),
        _state: &AppState,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg("Orbit fleet is unavailable")
        })?;
        let bus = InvocationBus::new(fleet);
        self.bus.store(Some(Arc::new(bus)));
        Ok(())
    }
}
