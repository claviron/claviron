//! Provider for the fleet's typed cells: two Orbit tables (scalar and text)
//! opened together, reused at every boot and preserved at every exit.
//!
//! First version: the text arena rides the scalar table's boot decision.
//! Both are created by one `Cells::new`, so one descriptor answers for both;
//! a separate descriptor per arena is a later refinement.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{self as runtime_registry};
pub use orbitive::cell::*;

pub(crate) mod orbital;

use crate::OrbitStateExt;
use crate::shm::{
    self, ExistingBootPolicy, FinalizePolicy, ManagedShmHandle,
    ManagedShmProvider, ManagedShmResource, ManagedShmResources,
    PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

const PROVIDER_NAME: &str = "CellsProvider";
const RESOURCE_NAME: &str = "orbit-cell-tables";

pub struct CellsResources {
    cells: Arc<ArcSwapOption<Cells>>,
}

impl CellsResources {
    fn new() -> Self {
        Self { cells: Arc::new(ArcSwapOption::const_empty()) }
    }

    fn install(
        &self,
        cells: Cells,
    ) -> runtime_registry::Result<()> {
        orbital::install(&self.cells, || Ok(cells)).map_err(|error| {
            runtime_registry::Error::msg(format!(
                "cell binding failed: {error}"
            ))
        })
    }
}

pub type CellsProvider = ManagedShmProvider<CellsResources>;

impl ManagedShmHandle for CellsResources {
    type Handle = Cells;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.cells.load_full()
    }
}

impl ManagedShmResources for CellsResources {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(runtime_registry::priority::EARLY)
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl ManagedShmResource for CellsResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(CELL_STATE_KIND),
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
            orbitive::shm::ring_segment_name(fleet.name(), CELL_STATE_KIND);
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        self.install(open(state)?)?;
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.install(open(state)?)
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
        let cells = open(state)?;
        orbital::install(&self.cells, || {
            cells.reset_all()?;
            Ok(cells)
        })
        .map_err(|error| {
            runtime_registry::Error::msg(format!("cell reset failed: {error}"))
        })
    }
}

pub(crate) fn register(state: &AppState) -> runtime_registry::Result<()> {
    shm::register(state, CellsResources::new())
}

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

fn open(state: &AppState) -> runtime_registry::Result<Cells> {
    Cells::new(fleet(state)?).map_err(|error| {
        runtime_registry::Error::msg(format!("cell boot failed: {error}"))
    })
}
