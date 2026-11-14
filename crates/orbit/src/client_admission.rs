//! Claviron lifecycle binding for the fleet's client admission table.
//!
//! One Orbit pool table, opted into fleet concurrency, holds a request budget
//! per key for the whole fleet. A process takes a permit before it sends with
//! its own client and returns it when the request ends; no client and no
//! byte crosses processes. See `docs/design/FLEET_CLIENT_ADMISSION.md`.
//!
//! The table is reset on a quiescent boot, reused while adopted workers keep
//! running, and kept on exit. A dead process's permits return when the
//! supervisor reports its confirmed exit ([`report_node_dead`]).

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::registry::{self as runtime_registry};
use claviron_runtime::{AppState, FleetIncarnation};
use orbitive::NodeId;
pub use orbitive::pool::{AdmissionPermit, Key, Pool};
use orbitive::pool::{Incarnation, PoolSpec};

use crate::OrbitStateExt;
use crate::shm::{
    self, ActiveBootPolicy, ExistingBootPolicy, FinalizePolicy,
    ManagedShmHandle, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

/// The SHM kind of the fleet's client admission table.
pub const CLIENT_ADMISSION_KIND: u8 = 175;

/// Budgets the table holds: one per key, such as one consumer and the remote
/// side whose limit it is.
pub const CLIENT_ADMISSION_KEY_CAPACITY: usize = 64;

const PROVIDER_NAME: &str = "ClientAdmissionProvider";
const RESOURCE_NAME: &str = "orbit-client-admission-table";

/// Domain resource set behind the mandatory SHM lifecycle wrapper.
pub struct ClientAdmissionResources {
    pool: ArcSwapOption<Pool>,
}

impl ClientAdmissionResources {
    fn new() -> Self {
        Self { pool: ArcSwapOption::const_empty() }
    }
}

/// Lifecycle owner and public capability for the fleet client admission
/// table.
pub type ClientAdmissionProvider = ManagedShmProvider<ClientAdmissionResources>;

impl ManagedShmHandle for ClientAdmissionResources {
    type Handle = Pool;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.pool.load_full()
    }
}

impl ManagedShmResources for ClientAdmissionResources {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl ManagedShmResource for ClientAdmissionResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(CLIENT_ADMISSION_KIND),
            // Permits a previous run left belong to processes that are gone
            // when no worker survived; adopted workers keep theirs.
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
        let name = orbitive::shm::ring_segment_name(
            fleet.name(),
            CLIENT_ADMISSION_KIND,
        );
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        self.pool.store(Some(Arc::new(open(state)?)));
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.pool.store(Some(Arc::new(open(state)?)));
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
        let pool = open(state)?;
        pool.reset_all();
        self.pool.store(Some(Arc::new(pool)));
        Ok(())
    }
}

pub(crate) fn register(state: &AppState) -> runtime_registry::Result<()> {
    shm::register(state, ClientAdmissionResources::new())
}

/// Return the permits `node` held in its life `incarnation`, after the
/// supervisor observed that process exit. Before the table opens there is
/// nothing to return.
pub fn report_node_dead(
    provider: &ClientAdmissionProvider,
    node: u16,
    incarnation: u64,
) {
    if let Some(pool) = provider.handle() {
        pool.node_dead(NodeId::new(node), Incarnation::new(incarnation));
    }
}

/// The table's geometry. Admission registers no resource, so a node's lane
/// holds the one slot the table's shape requires; the fleet's own capacity
/// sizes the per-node admission counts.
const SPEC: PoolSpec =
    PoolSpec::new(CLIENT_ADMISSION_KIND, CLIENT_ADMISSION_KEY_CAPACITY, 1)
        .with_fleet_concurrency();

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

/// Open the table as this life of the process, so a death report for an
/// earlier life of its node returns only that life's permits.
fn open(state: &AppState) -> runtime_registry::Result<Pool> {
    let incarnation = state
        .registry_ref()
        .resolve::<FleetIncarnation>()
        .map_or(FleetIncarnation::FIRST, |held| *held)
        .raw();
    Pool::with_spec(fleet(state)?, Incarnation::new(incarnation), SPEC).map_err(
        |error| {
            runtime_registry::Error::msg(format!(
                "client admission table boot failed: {error}"
            ))
        },
    )
}
