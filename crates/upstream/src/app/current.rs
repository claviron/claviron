//! The live service behind one provider-bound app, swapped atomically on
//! reload. A route selection clones the current one; accepted IO retains
//! that service's epoch. This is not `crate::binding`, which is the door a
//! host comes in through.
use std::fmt;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::proxy::service::{ServiceInner, UpstreamService};

pub(crate) struct CurrentService {
    current: ArcSwap<ServiceInner>,
}
impl CurrentService {
    pub(crate) fn new(service: &UpstreamService) -> Arc<Self> {
        Arc::new(Self { current: ArcSwap::new(service.inner.clone()) })
    }
    pub(crate) fn load(&self) -> UpstreamService {
        UpstreamService { inner: self.current.load_full() }
    }
    /// Caller seals the previous changed service before publishing. An unchanged
    /// service keeps its pools and does not need another atomic publication.
    pub(crate) fn publish(
        &self,
        service: &UpstreamService,
    ) {
        let previous = self.current.load();
        if !Arc::ptr_eq(&previous, &service.inner) {
            debug_assert_ne!(
                UpstreamService { inner: Arc::clone(&previous) }.lifecycle(),
                crate::proxy::service::lifecycle::UpstreamLifecycle::Running,
                "old service must be sealed before binding publication"
            );
            self.current.store(service.inner.clone());
        }
    }
}
impl fmt::Debug for CurrentService {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let service = self.load();
        formatter
            .debug_struct("CurrentService")
            .field("profile", &service.inner.profile.name)
            .field("generation", &service.generation())
            .finish_non_exhaustive()
    }
}
