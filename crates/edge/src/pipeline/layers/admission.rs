//! Prepared queue/head timing around the selected application's capacity.

use std::future::Future;
use std::time::Duration;

use claviron_http::admission::{Admission, Place};
use claviron_http::policy::RequestPolicy;
use super::*;

/// Immutable timing policy prepared after listener/host narrowing.
///
/// Capacity still belongs to the current application. This stage borrows that
/// authority rather than retaining it in a connection's metadata cache.
/// Waiting and reply-head timing use HTTP's existing futures and RAII permits.
/// They do not become synchronous HostLayer callbacks or erased layer futures.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionPolicy {
    queue_timeout: Option<Duration>,
    head_timeout: Option<Duration>,
}

impl AdmissionPolicy {
    pub fn new(policy: RequestPolicy) -> Self {
        Self {
            queue_timeout: policy.queue_timeout,
            head_timeout: policy.head_timeout,
        }
    }

    /// Return the existing admission future without an async wrapper or task.
    /// The future borrows only the supplied capacity; timing values are copied.
    pub fn enter<'a>(
        &self,
        admission: &'a Admission,
    ) -> impl Future<Output = Option<Place>> + Send + 'a + use<'a> {
        admission.enter(self.queue_timeout)
    }

    /// Ask the backend about timing ownership only when a limit is declared.
    /// A backend enforcing that limit itself must not be timed a second time.
    pub fn head_timeout_for(
        &self,
        owns_timeout: impl FnOnce() -> bool,
    ) -> Option<Duration> {
        self.head_timeout.filter(|_| !owns_timeout())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/layers/admission.rs"]
mod tests;

/// A configured concurrency authority, prepared once and shared by connections.
/// With no capacity declaration, the builder must not construct this layer.
pub struct CapacityLayer {
    admission: Admission,
    queue_timeout: Option<Duration>,
}

impl CapacityLayer {
    pub fn new(max_concurrent: usize, queue_timeout: Option<Duration>) -> Self {
        Self { admission: Admission::limited(Some(max_concurrent)), queue_timeout }
    }
}

/// Exists only in an execution state that selected capacity admission.
#[derive(Default)]
pub struct CapacityState {
    place: Option<Place>,
}

impl AsMut<CapacityState> for CapacityState {
    fn as_mut(&mut self) -> &mut Self { self }
}

impl<S: AsMut<CapacityState> + Send> AsyncHeadAction<S> for CapacityLayer {
    fn call<'a>(
        &'a self,
        _: &'a mut RequestParts,
        state: &'a mut S,
    ) -> std::pin::Pin<Box<dyn Future<Output = ControlFlow<RouteResponse>> + Send + 'a>> {
        Box::pin(async move {
            match self.admission.enter(self.queue_timeout).await {
                Some(place) => {
                    state.as_mut().place = Some(place);
                    ControlFlow::Continue(())
                }
                None => ControlFlow::Break(
                    claviron_http::err_status(http::StatusCode::SERVICE_UNAVAILABLE),
                ),
            }
        })
    }
}

impl<S: AsMut<CapacityState> + Send> HostLayer<S> for CapacityLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.before_async(self);
        layer.after(|_, state| { state.as_mut().place.take(); });
        Ok(())
    }
}
