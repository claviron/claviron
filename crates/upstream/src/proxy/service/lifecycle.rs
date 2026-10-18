//! Profile admission, explicit bounded drain and isolated pool generations.
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use claviron_runtime::gate::{Gate, GateDrainOutcome};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::runtime::ServiceRuntime;
use super::{ProtocolPools, UpstreamService};
use crate::compile::profile::Profile;
use crate::origin::PoolGeneration;
use crate::proxy::failure::RequestFailure;
use crate::proxy::lease::{InputLease, RequestLease};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum UpstreamLifecycle {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}
pub(crate) struct Lifecycle {
    pub(crate) current: ArcSwap<ServiceEpoch>,
    pub(crate) admission: Mutex<()>,
    operation: tokio::sync::Mutex<()>,
    pub(super) status: AtomicU8,
    // Registry retirement is permanent; operator stop/start is reversible.
    pub(super) retired: AtomicBool,
    pub(super) membership_retired: Mutex<Option<super::membership::Retirement>>,
}
pub(crate) struct ServiceEpoch {
    pub(crate) pools: ProtocolPools,
    pub(crate) generation: PoolGeneration,
    pub(crate) background: CancellationToken,
    pub(crate) force: CancellationToken,
    pub(crate) tasks: TaskTracker,
    // Profile-owned tunnel futures; native connection/writer tasks live below.
    pub(crate) io: TaskTracker,
    pub(crate) transports: Box<[Arc<super::transport::OriginTransport>]>,
    pub(crate) gate: Gate,
    pub(crate) breaker: Option<Arc<super::breaker::State>>,
    pub(crate) outlier: Option<crate::health::outlier::State>,
    pub(crate) retry: Option<Arc<super::retry::Window>>,
    pub(crate) lifetime: Arc<super::control::ProfileLifetime>,
    pub(crate) owners: Box<[super::control::OwnedOrigin]>,
    pub(super) stopped: CancellationToken,
}
impl ServiceEpoch {
    pub(crate) fn admit_input(&self) -> Result<InputLease, RequestFailure> {
        Ok(InputLease {
            origin: None,
            breaker: None,
            _permit: self
                .gate
                .try_enter()
                .map_err(|_| RequestFailure::ProfileStopped)?,
        })
    }
    pub(crate) fn transport(
        &self,
        id: crate::origin::OriginId,
    ) -> &Arc<super::transport::OriginTransport> {
        &self.transports[id.index()]
    }
    async fn wait_outbound_io(&self) {
        self.io.wait().await;
        for owner in self.lifetime.owners() {
            owner.transport.wait().await;
        }
    }
    fn new(
        profile: &Arc<Profile>,
        runtime: &ServiceRuntime,
    ) -> Arc<Self> {
        let generation = PoolGeneration(
            runtime.generations.fetch_add(1, Ordering::Relaxed) + 1,
        );
        let lifetime = super::control::ProfileLifetime::new(profile);
        let owners = profile
            .origins
            .iter()
            .map(|origin| {
                super::control::OwnedOrigin::new(
                    profile, generation, origin, &lifetime,
                )
            })
            .collect::<Vec<_>>();
        Self::from_owners(profile, generation, lifetime, owners)
    }
    pub(crate) fn from_owners(
        profile: &Profile,
        generation: PoolGeneration,
        lifetime: Arc<super::control::ProfileLifetime>,
        owners: Vec<super::control::OwnedOrigin>,
    ) -> Arc<Self> {
        for owner in &owners {
            lifetime.register(owner.clone());
        }
        Arc::new(Self {
            transports: owners
                .iter()
                .map(|owner| owner.transport.clone())
                .collect(),
            pools: super::control::pools(profile.protocol, &owners),
            owners: owners.into_boxed_slice(),
            retry: lifetime.retry.clone(),
            breaker: lifetime.breaker.clone(),
            outlier: crate::health::outlier::State::new(
                &profile.outlier,
                profile.origins.len(),
            ),
            generation,
            gate: lifetime.gate.clone(),
            force: lifetime.force.clone(),
            io: lifetime.io.clone(),
            lifetime,
            background: CancellationToken::new(),
            tasks: TaskTracker::new(),
            stopped: CancellationToken::new(),
        })
    }
    fn close(&self) {
        self.force.cancel();
        for owner in self.lifetime.owners() {
            owner.close();
        }
    }
}

impl Lifecycle {
    pub(crate) fn new(
        profile: &Arc<Profile>,
        runtime: &ServiceRuntime,
    ) -> Self {
        Self::from_epoch(ServiceEpoch::new(profile, runtime))
    }
    pub(crate) fn from_epoch(epoch: Arc<ServiceEpoch>) -> Self {
        Self {
            current: ArcSwap::new(epoch),
            admission: Mutex::new(()),
            operation: tokio::sync::Mutex::new(()),
            status: AtomicU8::new(UpstreamLifecycle::Running as u8),
            retired: AtomicBool::new(false),
            membership_retired: Mutex::new(None),
        }
    }
}
enum SealDisposition {
    Profile,
    Membership(super::membership::Retirement),
}
impl UpstreamService {
    pub fn lifecycle(&self) -> UpstreamLifecycle {
        match self.inner.lifecycle.status.load(Ordering::Acquire) {
            0 => UpstreamLifecycle::Starting,
            1 => UpstreamLifecycle::Running,
            2 => UpstreamLifecycle::Stopping,
            3 => UpstreamLifecycle::Stopped,
            _ => UpstreamLifecycle::Failed,
        }
    }
    pub fn generation(&self) -> PoolGeneration {
        self.inner.lifecycle.current.load().generation
    }
    pub(crate) fn epoch(&self) -> Arc<ServiceEpoch> {
        self.inner.lifecycle.current.load_full()
    }
    pub(crate) fn admit(
        &self,
        request_body: bool,
    ) -> Result<RequestLease, RequestFailure> {
        let _admission = self
            .inner
            .lifecycle
            .admission
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if self.lifecycle() != UpstreamLifecycle::Running {
            return Err(RequestFailure::ProfileStopped);
        }
        let epoch = self.epoch();
        let permit = epoch
            .gate
            .try_enter()
            .map_err(|_| RequestFailure::ProfileStopped)?;
        // Track both IO legs independently: H2 may return a response before
        // a streaming request writer has finished. Neither needs an Arc<Permit>.
        let request =
            if request_body { Some(epoch.admit_input()?) } else { None };
        Ok(RequestLease {
            epoch,
            _permit: permit,
            request,
            breaker: None,
            origin: None,
        })
    }
    /// Stop the current profile and join all native owners. On an explicitly
    /// replaced membership handle, join its removed origins/maintenance only;
    /// unchanged accepted IO belongs to the stable current profile lifetime.
    pub async fn stop(&self) -> GateDrainOutcome {
        let _operation = self.inner.lifecycle.operation.lock().await;
        let epoch = self.epoch();
        if self.lifecycle() == UpstreamLifecycle::Stopped {
            return GateDrainOutcome::Drained;
        }
        let disposition = self.seal(false);
        if let SealDisposition::Membership(retirement) = disposition {
            epoch.tasks.wait().await;
            // Another view may have replaced our immediate successor while
            // this old maintenance still held a shared replenishing flag.
            epoch.lifetime.warm_current();
            let outcomes = futures_util::future::join_all(
                retirement.removed.iter().map(|owner| owner.stop()),
            )
            .await;
            for owner in &retirement.removed {
                epoch.lifetime.release(owner);
            }
            self.inner
                .lifecycle
                .status
                .store(UpstreamLifecycle::Stopped as u8, Ordering::Release);
            epoch.stopped.cancel();
            return super::membership::combine(outcomes);
        }
        let (_, outcome) =
            tokio::join!(epoch.tasks.wait(), epoch.gate.wait_all_done());
        epoch.close();
        epoch.io.close();
        epoch.wait_outbound_io().await;
        self.inner
            .lifecycle
            .status
            .store(UpstreamLifecycle::Stopped as u8, Ordering::Release);
        epoch.stopped.cancel();
        outcome
    }
    /// Registry-retired handles never reopen. A reversible operator stop
    /// still creates fresh pools; a retired handle returns its sealed generation.
    pub async fn start(&self) -> PoolGeneration {
        // A sealed registry handle is a no-op even while its drain owns the
        // operation mutex and a caller retains an accepted response body.
        if self.inner.lifecycle.retired.load(Ordering::Acquire) {
            return self.generation();
        }
        let _operation = self.inner.lifecycle.operation.lock().await;
        let generation = {
            let _admission = self
                .inner
                .lifecycle
                .admission
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if self.inner.lifecycle.retired.load(Ordering::Acquire)
                || self.lifecycle() == UpstreamLifecycle::Running
            {
                return self.generation();
            }
            let epoch =
                ServiceEpoch::new(&self.inner.profile, &self.inner.runtime);
            let generation = epoch.generation;
            self.inner.balancer.reset();
            // Retirement and restart share admission serialization. A retiring
            // registry cannot miss a fresh epoch published by a racing start.
            self.inner.lifecycle.current.store(epoch);
            self.inner
                .lifecycle
                .status
                .store(UpstreamLifecycle::Running as u8, Ordering::Release);
            generation
        };
        self.attach_origin_projections();
        self.attach_warm_pools();
        self.warm_pools();
        self.inner.runtime.changed.notify_waiters();
        generation
    }

    /// Synchronously reject new accepted work before publishing a replacement.
    /// This does not wait, force accepted IO, spawn tasks, or prove joined drain.
    pub(crate) fn retire(&self) {
        let _ = self.seal(true);
    }

    fn seal(
        &self,
        retired: bool,
    ) -> SealDisposition {
        let _admission = self
            .inner
            .lifecycle
            .admission
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        self.seal_locked(retired)
    }
    /// Caller already holds this service's admission mutex. Boot publication
    /// takes registry -> all retiring admissions -> resolution table exclusion.
    pub(crate) fn retire_locked(&self) {
        let _ = self.seal_locked(true);
    }
    fn seal_locked(
        &self,
        retired: bool,
    ) -> SealDisposition {
        if retired {
            self.inner.lifecycle.retired.store(true, Ordering::Release);
        }
        if let Some(retirement) = self
            .inner
            .lifecycle
            .membership_retired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            return SealDisposition::Membership(retirement);
        }
        if self.lifecycle() == UpstreamLifecycle::Stopped {
            return SealDisposition::Profile;
        }
        let epoch = self.epoch();
        self.inner
            .lifecycle
            .status
            .store(UpstreamLifecycle::Stopping as u8, Ordering::Release);
        epoch.gate.graceful_shutdown(Some(
            self.inner.profile.lifecycle.drain_timeout,
        ));
        epoch.background.cancel();
        epoch.tasks.close();
        for owner in epoch.lifetime.owners() {
            owner.seal(self.inner.profile.lifecycle.drain_timeout);
        }
        self.inner.runtime.changed.notify_waiters();
        SealDisposition::Profile
    }

    pub(crate) async fn wait_stopped(&self) {
        self.epoch().stopped.cancelled().await;
    }
    /// Track a cold maintenance future under its captured epoch without
    /// spawning it. Admission serializes attachment with profile stop; dropping
    /// the Runnable generation releases this token and its probe leases.
    pub(crate) fn background_future_for(
        &self,
        epoch: &Arc<ServiceEpoch>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Option<crate::health::tasks::ProbeFuture> {
        let _admission = self
            .inner
            .lifecycle
            .admission
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !Arc::ptr_eq(epoch, &self.epoch())
            || epoch.background.is_cancelled()
            || self.inner.runtime.shutdown.is_cancelled()
        {
            return None;
        }
        let cancelled = epoch.background.clone();
        let shutdown = self.inner.runtime.shutdown.clone();
        Some(Box::pin(epoch.tasks.track_future(async move {
            tokio::select! { biased; _ = cancelled.cancelled() => {}, _ = shutdown.cancelled() => {}, _ = future => {} }
        })))
    }
    pub(crate) fn spawn_background_for(
        &self,
        epoch: &Arc<ServiceEpoch>,
        future: impl Future<Output = ()> + Send + 'static,
    ) {
        let _admission = self
            .inner
            .lifecycle
            .admission
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if Arc::ptr_eq(epoch, &self.epoch())
            && !epoch.background.is_cancelled()
            && !self.inner.runtime.shutdown.is_cancelled()
        {
            epoch.tasks.spawn(future);
        }
    }
}
