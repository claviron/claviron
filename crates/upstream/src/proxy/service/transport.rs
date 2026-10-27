//! Native outbound IO belongs to one concrete origin, independently of request
//! admission and profile maintenance. Construction is inert and starts no tasks.
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use claviron_runtime::gate::{Gate, Permit};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::origin::PoolGeneration;
use crate::proxy::failure::RequestFailure;

pub(crate) struct OriginTransport {
    pub(crate) generation: PoolGeneration,
    pub(crate) force: CancellationToken,
    pub(crate) io: TaskTracker,
    pub(crate) gate: Gate,
    projection: RwLock<Option<Projection>>,
    health_owner: Option<crate::state::StateOwner>,
}
impl OriginTransport {
    /// Register the whole dial/handshake before its first poll, so joined stop
    /// also covers sockets that have not yet reached driver publication.
    pub(crate) async fn native<T>(
        &self,
        future: impl Future<
            Output = Result<T, crate::health::probe::OriginReachabilityFailure>,
        >,
    ) -> Result<T, crate::proxy::connect::AcquireConnectionError> {
        let _assignment = self
            .assignment_guard()
            .map_err(crate::proxy::connect::AcquireConnectionError::Pool)?;
        self.io.track_future(async {
            tokio::select! { biased;
                _ = self.force.cancelled() => Err(crate::proxy::connect::AcquireConnectionError::Pool(RequestFailure::Closed)),
                result = future => result.map_err(crate::proxy::connect::AcquireConnectionError::OriginReachabilityFailure)
            }
        }).await
    }
    /// The HTTP send/upgrade phase may own upgraded IO between driver EOF and
    /// session submission. Join it before claiming the origin's IO is closed.
    pub(crate) fn native_phase(
        &self
    ) -> Result<tokio_util::task::task_tracker::TaskTrackerToken, RequestFailure>
    {
        let _assignment = self.assignment_guard()?;
        let token = self.io.token();
        if self.force.is_cancelled() {
            return Err(RequestFailure::Closed);
        }
        Ok(token)
    }

    pub(crate) fn new(generation: PoolGeneration) -> Self {
        Self {
            generation,
            force: CancellationToken::new(),
            io: TaskTracker::new(),
            // No capacity, entered only with `try_enter`: it never waits, so
            // its wait budget is never read. A request's wait for capacity is
            // its `queue_timeout`, taken where the pools make it wait.
            gate: Gate::new(None, Duration::ZERO),
            projection: RwLock::new(None),
            health_owner: None,
        }
    }
    pub(crate) fn seal(
        &self,
        drain_timeout: Duration,
    ) {
        self.gate.graceful_shutdown(Some(drain_timeout));
    }
    pub(crate) fn close(&self) {
        self.gate.force_shutdown();
        self.force.cancel();
        self.io.close();
    }
    pub(crate) async fn wait(&self) {
        self.io.wait().await;
        if self.force.is_cancelled()
            && let Some(owner) = &self.health_owner
        {
            owner.joined();
        }
    }
}

/// Both HTTP IO legs retain the selected origin independently. In particular,
/// an H2 response EOF is not evidence that its request writer has ended.
pub(crate) struct OriginAdmission {
    response: OriginLease,
    input: Option<Permit>,
}
pub(crate) struct OriginLease {
    pub(crate) transport: Arc<OriginTransport>,
    _permit: Permit,
}
impl OriginTransport {
    pub(crate) fn admit(
        self: &Arc<Self>,
        request_body: bool,
    ) -> Result<OriginAdmission, RequestFailure> {
        let _assignment = self.assignment_guard()?;
        if self.force.is_cancelled() {
            return Err(RequestFailure::Closed);
        }
        let response =
            self.gate.try_enter().map_err(|_| RequestFailure::Closed)?;
        let input = if request_body {
            Some(self.gate.try_enter().map_err(|_| RequestFailure::Closed)?)
        } else {
            None
        };
        Ok(OriginAdmission {
            response: OriginLease {
                transport: self.clone(),
                _permit: response,
            },
            input,
        })
    }
}
impl OriginAdmission {
    pub(crate) fn attach(
        self,
        lease: &mut crate::proxy::lease::RequestLease,
    ) {
        if let Some(input) = &mut lease.request {
            input.origin = self.input;
        }
        lease.origin = Some(self.response);
    }
}

#[derive(Clone)]
struct Projection {
    service: Weak<super::ServiceInner>,
    epoch: Weak<super::lifecycle::ServiceEpoch>,
    index: usize,
}
impl super::UpstreamService {
    pub(crate) fn attach_origin_projections(&self) {
        let epoch = self.epoch();
        epoch.lifetime.publish_current(self);
        for (index, transport) in epoch.transports.iter().enumerate() {
            *transport.projection.write().unwrap_or_else(|p| p.into_inner()) =
                Some(Projection {
                    service: Arc::downgrade(&self.inner),
                    epoch: Arc::downgrade(&epoch),
                    index,
                });
        }
    }
}
impl OriginTransport {
    pub(crate) fn projection(
        &self
    ) -> Option<(
        super::UpstreamService,
        Arc<super::lifecycle::ServiceEpoch>,
        usize,
    )> {
        let projection = self
            .projection
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()?;
        let service =
            super::UpstreamService { inner: projection.service.upgrade()? };
        let epoch = projection.epoch.upgrade()?;
        if epoch.background.is_cancelled() || self.gate.is_shutting_down() {
            return None;
        }
        Some((service, epoch, projection.index))
    }
}

impl OriginTransport {
    pub(crate) fn bind_assignment(
        mut self,
        slot: &crate::state::SlotRef,
    ) -> Self {
        self.health_owner = slot.owner();
        if slot.is_assignment() && self.health_owner.is_none() {
            // Registration lost a concurrent retirement. An inert constructor
            // must not create a native owner outside the assignment fence.
            self.close();
        }
        self
    }
    pub(super) fn abort_inert(self) {
        // Consumed unique construction has never admitted or published native
        // IO. A closed empty tracker proves the join synchronously; this is not
        // an acknowledgement from Drop, or a shortcut for a serving owner.
        assert!(self.io.is_empty());
        assert_eq!(self.gate.count(), 0);
        assert!(
            self.projection.read().unwrap_or_else(|p| p.into_inner()).is_none()
        );
        self.close();
        if let Some(owner) = &self.health_owner {
            owner.joined();
        }
    }
    fn assignment_guard(
        &self
    ) -> Result<Option<crate::state::StateGuard>, RequestFailure> {
        self.health_owner
            .as_ref()
            .map(|owner| owner.guard().ok_or(RequestFailure::Closed))
            .transpose()
    }
}
