//! Claviron-facing Orbit lock types.
//!
//! [`OrbitLock`] is the framework-neutral primitive. [`Lock`] adds the Rust
//! RAII lifetime used by Claviron services: dropping it releases only the
//! exact lease it owns. Blocking, retries, and callback policy stay with the
//! caller.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{self};
use orbitive::lock::Result;
pub use orbitive::lock::{
    LOCK_EVENT_RING_KIND, LOCK_STATE_KIND, Lock as OrbitLock, LockAcquire,
    LockEvent, LockKey, LockLayout, LockLease, LockOwner, LockPoll,
    LockTransition, LockType,
};
use orbitive::{OrbitTyped, RingSpec};

use crate::OrbitStateExt;
use crate::shm::{
    ExistingBootPolicy, FinalizePolicy, ManagedShmResource,
    ManagedShmResources, OrbitRingResource, PhysicalShmState, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};

struct ManagedLockEventRecord<L>(PhantomData<L>);

impl<L> Clone for ManagedLockEventRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: LockLayout> OrbitTyped for ManagedLockEventRecord<L> {
    const KIND: u8 = L::EVENT_RING_KIND;
    const RING_SPEC: RingSpec = L::EVENT_RING_SPEC;
}

struct LockStateResource<L> {
    descriptor: ShmResourceDescriptor,
    _marker: PhantomData<fn() -> L>,
}

impl<L: LockLayout> LockStateResource<L> {
    const fn new(provider: &'static str) -> Self {
        Self {
            descriptor: ShmResourceDescriptor {
                provider,
                resource: "orbit-lock-state-table",
                shape: ShmShape::OrbitTable,
                orbit_kind: Some(L::STATE_KIND),
                existing_boot: ExistingBootPolicy::Reuse,
                finalize: FinalizePolicy::Preserve,
            },
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<L: LockLayout> ManagedShmResource for LockStateResource<L> {
    fn descriptor(&self) -> ShmResourceDescriptor {
        self.descriptor
    }

    fn validate_physical(
        &self,
        state: &AppState,
    ) -> registry::Result<PhysicalShmState> {
        let fleet = fleet(state)?;
        let name =
            orbitive::shm::ring_segment_name(fleet.name(), L::STATE_KIND);
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, 64)?.into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> registry::Result<ShmCreateOutcome> {
        open::<L>(state)?;
        Ok(ShmCreateOutcome::Opened)
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        open::<L>(state)
    }
}

struct LockShmResources<L> {
    state: LockStateResource<L>,
    events: OrbitRingResource<ManagedLockEventRecord<L>>,
}

impl<L: LockLayout> LockShmResources<L> {
    fn new() -> Self {
        const PROVIDER: &str = "OrbitLockShm";
        Self {
            state: LockStateResource::new(PROVIDER),
            events: OrbitRingResource::new(
                PROVIDER,
                "orbit-lock-transition-ring",
                ExistingBootPolicy::Reuse,
                FinalizePolicy::Preserve,
            ),
        }
    }
}

impl<L: LockLayout> ManagedShmResources for LockShmResources<L> {
    fn name(&self) -> &'static str {
        "OrbitLockShm"
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![&self.state, &self.events]
    }
}

pub(crate) fn register_shm(state: &AppState) -> registry::Result<()> {
    crate::shm::register(
        state,
        LockShmResources::<orbitive::lock::DefaultLockLayout>::new(),
    )
}

fn fleet(state: &AppState) -> registry::Result<Arc<orbitive::Fleet>> {
    state.fleet().ok_or_else(|| {
        registry::Error::msg("Orbit fleet is unavailable for lock SHM")
    })
}

fn open<L: LockLayout>(state: &AppState) -> registry::Result<()> {
    OrbitLock::<L>::with_layout(fleet(state)?).map(drop).map_err(|error| {
        registry::Error::msg(format!("Orbit lock SHM boot failed: {error}"))
    })
}

/// One held lock that releases its exact lease on drop.
pub struct Lock {
    orbit: OrbitLock,
    lease: Option<LockLease>,
}

impl Lock {
    pub fn new(
        orbit: OrbitLock,
        lease: LockLease,
    ) -> Self {
        Self { orbit, lease: Some(lease) }
    }

    pub fn lease(&self) -> &LockLease {
        self.lease.as_ref().expect("held lock is missing its lease")
    }

    /// Renew this exact tenure. Returns false if ownership was already lost.
    pub fn renew(
        &mut self,
        ttl: Duration,
    ) -> Result<bool> {
        let Some(renewed) = self.orbit.renew(self.lease(), ttl)? else {
            return Ok(false);
        };
        self.lease = Some(renewed);
        Ok(true)
    }

    /// Release this exact tenure before drop.
    pub fn release(mut self) -> Result<bool> {
        let lease = self.lease.take().expect("held lock is missing its lease");
        let released = self.orbit.release(&lease);
        if released.is_err() {
            self.lease = Some(lease);
        }
        released
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        if let Err(error) = self.orbit.release(&lease) {
            tracing::debug!(
                lock_id = %lease.lock_id,
                namespace = %String::from_utf8_lossy(lease.key.namespace()),
                error = %error,
                "Orbit lock release failed"
            );
        }
    }
}
