//! Claviron lifecycle binding for Orbit's keyed counter table.
//!
//! The table itself -- slots, hashing, the structural lock, the SHM layout and
//! its compile-time geometry -- lives in `orbit-counter` and is re-exported
//! here unchanged. Claviron adds the typed lifecycle decision: a compatible
//! table is reused at every boot and preserved at every exit, so values are
//! kept across a restart.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{self as runtime_registry};
pub use orbitive::counter::*;

use crate::OrbitStateExt;
use crate::shm::{
    self, ExistingBootPolicy, FinalizePolicy, ManagedShmHandle,
    ManagedShmProvider, ManagedShmResource, ManagedShmResources,
    PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

const PROVIDER_NAME: &str = "CounterProvider";
const RESOURCE_NAME: &str = "orbit-counter-table";

/// Domain resource set behind the mandatory SHM lifecycle wrapper.
pub struct CounterResources {
    counter: ArcSwapOption<Counter>,
}

impl CounterResources {
    fn new() -> Self {
        Self { counter: ArcSwapOption::const_empty() }
    }
}

/// Lifecycle owner and public capability for the optional keyed counter table.
pub type CounterProvider = ManagedShmProvider<CounterResources>;

impl ManagedShmHandle for CounterResources {
    type Handle = Counter;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.counter.load_full()
    }
}

impl ManagedShmResources for CounterResources {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl ManagedShmResource for CounterResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(COUNTER_STATE_KIND),
            // Preserve on the way out and reset on the way in cancelled
            // each other: what the finalize kept, the next boot wiped before
            // anything could read it. The table is a cache that outlives a
            // restart, and a released id cannot reach its slot again — the
            // generation stamp is bumped on every install — so reuse is safe
            // as well as cheaper.
            existing_boot: ExistingBootPolicy::Reuse,
            finalize: FinalizePolicy::Preserve,
        }
    }

    fn validate_physical(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<PhysicalShmState> {
        let fleet = fleet(state)?;
        let name =
            orbitive::shm::ring_segment_name(fleet.name(), COUNTER_STATE_KIND);
        // The public counter ABI validates its complete layout when `Counter::new`
        // attaches. Here the stable 64-byte header floor prevents mapping a
        // physically truncated object before that semantic validation.
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        let counter = open(state)?;
        self.counter.store(Some(Arc::new(counter)));
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.counter.store(Some(Arc::new(open(state)?)));
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
        let counter = open(state)?;
        counter.reset_all().map_err(|error| {
            runtime_registry::Error::msg(format!(
                "counter reset failed: {error}"
            ))
        })?;
        self.counter.store(Some(Arc::new(counter)));
        Ok(())
    }
}

pub(crate) fn register(state: &AppState) -> runtime_registry::Result<()> {
    shm::register(state, CounterResources::new())
}

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

fn open(state: &AppState) -> runtime_registry::Result<Counter> {
    Counter::new(fleet(state)?).map_err(|error| {
        runtime_registry::Error::msg(format!("counter boot failed: {error}"))
    })
}
