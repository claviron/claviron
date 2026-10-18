//! Provider for the fleet's byte streams: `orbit-stream`'s default table,
//! kind `246`, in the fleet's own segment.
//!
//! A stream is two bounded byte rings between two holders — whoever created
//! it and whoever opened its ticket. The table is per-node lanes of such
//! slots; a process creates in its own lane and opens in anyone's.
//!
//! A stream does not outlive its holders, so a quiescent boot resets the
//! table: what it finds there belongs to processes that are gone. With live
//! workers attached the table is reused as it is.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_runtime::registry::{self as runtime_registry};
use claviron_runtime::{AppState, FleetIncarnation};
pub use orbitive::stream::{
    Endpoint, Incarnation, ReadHalf, STREAM_BUFFER_BYTES, STREAM_KIND,
    StreamId, Streams, Ticket, WriteHalf,
};

use crate::OrbitStateExt;
use crate::shm::{
    self, ActiveBootPolicy, ExistingBootPolicy, FinalizePolicy,
    ManagedShmHandle, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ResourceActivity, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

const PROVIDER_NAME: &str = "StreamsProvider";
const RESOURCE_NAME: &str = "orbit-stream-table";

pub struct StreamsResources {
    streams: ArcSwapOption<Streams>,
}

impl StreamsResources {
    fn new() -> Self {
        Self { streams: ArcSwapOption::const_empty() }
    }
}

pub type StreamsProvider = ManagedShmProvider<StreamsResources>;

impl ManagedShmHandle for StreamsResources {
    type Handle = Streams;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>> {
        self.streams.load_full()
    }
}

impl ManagedShmResources for StreamsResources {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl ManagedShmResource for StreamsResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: PROVIDER_NAME,
            resource: RESOURCE_NAME,
            shape: ShmShape::OrbitTable,
            orbit_kind: Some(STREAM_KIND),
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
        let name = orbitive::shm::ring_segment_name(fleet.name(), STREAM_KIND);
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<ShmCreateOutcome> {
        self.streams.store(Some(Arc::new(open(state)?)));
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> runtime_registry::Result<()> {
        self.streams.store(Some(Arc::new(open(state)?)));
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
        let streams = open(state)?;
        streams.reset_all();
        self.streams.store(Some(Arc::new(streams)));
        Ok(())
    }
}

pub(crate) fn register(state: &AppState) -> runtime_registry::Result<()> {
    shm::register(state, StreamsResources::new())
}

fn fleet(state: &AppState) -> runtime_registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        runtime_registry::Error::msg("Orbit fleet is unavailable")
    })
}

/// Open the table as this life of the process. The incarnation stamps every
/// side it claims, so a death report for the process before this one cannot
/// end this one's streams.
///
/// A worker inherits its node id from the life before it and ends what that
/// life held: the supervisor spawns a replacement only after the slot's
/// process has exited, so every other incarnation on this node is gone. The
/// master's incarnation does not count lives, and a quiescent boot resets the
/// table instead.
fn open(state: &AppState) -> runtime_registry::Result<Streams> {
    let incarnation = state
        .registry_ref()
        .resolve::<FleetIncarnation>()
        .map_or(FleetIncarnation::FIRST, |held| *held)
        .raw();
    let streams = Streams::new(fleet(state)?, Incarnation::new(incarnation))
        .map_err(|error| {
            runtime_registry::Error::msg(format!(
                "stream table boot failed: {error}"
            ))
        })?;
    if state.is_worker() {
        streams.supersede();
    }
    Ok(streams)
}
