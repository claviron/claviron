//! Stable profile admission and native owners across membership views.
use std::sync::{Arc, Mutex, RwLock, Weak};

use claviron_runtime::gate::{Gate, GateDrainOutcome};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::ProtocolPools;
use super::transport::OriginTransport;
use crate::compile::profile::Profile;
use crate::config::profile::UpstreamProtocol;
use crate::origin::PoolGeneration;

pub(crate) struct ProfileLifetime {
    pub(crate) gate: Gate,
    pub(crate) force: CancellationToken,
    pub(crate) io: TaskTracker,
    pub(crate) breaker: Option<Arc<super::breaker::State>>,
    pub(crate) retry: Option<Arc<super::retry::Window>>,
    owners: Mutex<Vec<OwnedOrigin>>,
    current: RwLock<Weak<super::ServiceInner>>,
}
#[derive(Clone)]
pub(crate) struct OwnedOrigin {
    pub(crate) transport: Arc<OriginTransport>,
    pub(crate) pool: OriginPool,
}
#[derive(Clone)]
pub(crate) enum OriginPool {
    H1(Arc<Mutex<crate::proxy::h1::pool::TargetPool>>),
    H2(Arc<crate::proxy::h2::pool::H2TargetPool>),
}
impl ProfileLifetime {
    pub(crate) fn new(profile: &Profile) -> Arc<Self> {
        Arc::new(Self {
            // No capacity, entered only with `try_enter`: it never waits.
            gate: Gate::new(None, std::time::Duration::ZERO),
            force: CancellationToken::new(),
            io: TaskTracker::new(),
            breaker: super::breaker::State::new(&profile.breaker),
            retry: super::retry::Window::new(&profile.retry).map(Arc::new),
            owners: Mutex::new(Vec::new()),
            current: RwLock::new(Weak::new()),
        })
    }
    pub(crate) fn owners(&self) -> Vec<OwnedOrigin> {
        self.owners.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
    pub(crate) fn publish_current(
        &self,
        service: &super::UpstreamService,
    ) {
        *self.current.write().unwrap_or_else(|p| p.into_inner()) =
            Arc::downgrade(&service.inner);
    }
    pub(crate) fn warm_current(&self) {
        let current =
            self.current.read().unwrap_or_else(|p| p.into_inner()).upgrade();
        if let Some(inner) = current {
            super::UpstreamService { inner }.warm_pools();
        }
    }
    pub(crate) fn register(
        &self,
        owner: OwnedOrigin,
    ) {
        let mut owners = self.owners.lock().unwrap_or_else(|p| p.into_inner());
        if !owners
            .iter()
            .any(|old| Arc::ptr_eq(&old.transport, &owner.transport))
        {
            owners.push(owner);
        }
    }
    pub(crate) fn release(
        &self,
        owner: &OwnedOrigin,
    ) {
        self.owners
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|old| !Arc::ptr_eq(&old.transport, &owner.transport));
    }
}
impl OwnedOrigin {
    pub(crate) fn new(
        profile: &Arc<Profile>,
        generation: PoolGeneration,
        origin: &crate::origin::Origin,
        lifetime: &ProfileLifetime,
    ) -> Self {
        let pool = match profile.protocol {
            UpstreamProtocol::H1 => {
                let mut pool = crate::proxy::h1::pool::TargetPool::new(
                    profile.clone(),
                    generation,
                );
                pool.breaker = lifetime.breaker.clone();
                OriginPool::H1(Arc::new(Mutex::new(pool)))
            }
            UpstreamProtocol::H2 => {
                let mut pool = crate::proxy::h2::pool::H2TargetPool::new(
                    profile.clone(),
                    generation,
                    origin.id,
                );
                pool.breaker = lifetime.breaker.clone();
                OriginPool::H2(Arc::new(pool))
            }
        };
        let transport = OriginTransport::new(generation);
        let transport = transport.bind_assignment(origin.slot.get());
        Self { transport: Arc::new(transport), pool }
    }
    pub(crate) fn abort_prepared(self) {
        // The candidate has not escaped, joined the lifetime or attached driver
        // projections. Consume unique ownership; never abort a shared live pool.
        match &self.pool {
            OriginPool::H1(pool) => assert_eq!(Arc::strong_count(pool), 1),
            OriginPool::H2(pool) => assert_eq!(Arc::strong_count(pool), 1),
        }
        let Ok(transport) = Arc::try_unwrap(self.transport) else {
            panic!("prepared origin transport escaped before publication");
        };
        transport.abort_inert();
    }
    pub(crate) fn seal(
        &self,
        timeout: std::time::Duration,
    ) {
        self.transport.seal(timeout);
        match &self.pool {
            OriginPool::H1(pool) => {
                pool.lock().unwrap_or_else(|p| p.into_inner()).close()
            }
            OriginPool::H2(pool) => pool.seal(),
        }
    }
    pub(crate) fn close(&self) {
        self.transport.close();
        match &self.pool {
            OriginPool::H1(pool) => {
                pool.lock().unwrap_or_else(|p| p.into_inner()).close()
            }
            OriginPool::H2(pool) => pool.close(),
        }
    }
    pub(crate) async fn stop(&self) -> GateDrainOutcome {
        let outcome = tokio::select! { biased;
            _ = self.transport.force.cancelled() => GateDrainOutcome::Forced { remaining: self.transport.gate.count() },
            outcome = self.transport.gate.wait_all_done() => outcome
        };
        self.close();
        self.transport.wait().await;
        outcome
    }
}
pub(crate) fn pools(
    protocol: UpstreamProtocol,
    owners: &[OwnedOrigin],
) -> ProtocolPools {
    match protocol {
        UpstreamProtocol::H1 => ProtocolPools::H1(
            owners
                .iter()
                .map(|owner| {
                    let OriginPool::H1(pool) = &owner.pool else {
                        unreachable!("profile protocol immutable")
                    };
                    pool.clone()
                })
                .collect(),
        ),
        UpstreamProtocol::H2 => ProtocolPools::H2(
            owners
                .iter()
                .map(|owner| {
                    let OriginPool::H2(pool) = &owner.pool else {
                        unreachable!("profile protocol immutable")
                    };
                    pool.clone()
                })
                .collect(),
        ),
    }
}
