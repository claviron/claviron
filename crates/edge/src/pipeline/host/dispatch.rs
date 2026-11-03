//! Request ownership calls registered by concrete HostLayer implementations.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use http::Request;

use super::HostHandlerFuture;

pub type DispatchResult = ServiceResult<RouteResponse, Request<RouteBody>>;

/// A synchronous decision, consumed before the caller awaits pending work.
/// Ready requests and responses stay inline without allocating a future.
#[must_use]
#[allow(
    clippy::large_enum_variant,
    reason = "Ready decisions are consumed before await; boxing them would allocate on synchronous fallthrough"
)]
pub enum DispatchCall {
    Ready(DispatchResult),
    Pending(DispatchFuture),
}

/// Only pending work enters the caller's suspended state. Application futures
/// retain their existing erasure without a second future allocation.
pub struct DispatchFuture {
    inner: Work,
}

enum Work {
    Request(Pin<Box<dyn Future<Output = DispatchResult> + Send>>),
    Application(HostHandlerFuture),
}

impl DispatchCall {
    pub fn ready(result: DispatchResult) -> Self {
        Self::Ready(result)
    }

    pub fn new(
        work: impl Future<Output = DispatchResult> + Send + 'static
    ) -> Self {
        Self::Pending(DispatchFuture { inner: Work::Request(Box::pin(work)) })
    }

    pub(in crate::pipeline) fn application(work: HostHandlerFuture) -> Self {
        Self::Pending(DispatchFuture { inner: Work::Application(work) })
    }
}

impl Future for DispatchFuture {
    type Output = DispatchResult;

    fn poll(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        match &mut self.get_mut().inner {
            Work::Request(work) => work.as_mut().poll(context),
            Work::Application(work) => {
                work.as_mut().poll(context).map(|result| match result {
                    ServiceResult::Handled(response) => {
                        ServiceResult::Handled(response)
                    }
                    ServiceResult::Unhandled(request) => {
                        ServiceResult::Unhandled(request.into_request())
                    }
                })
            }
        }
    }
}

/// A body-owning operation. Registration in a host plan uses HostLayer; this
/// call contract also serves PHP's selected runner without another plan.
pub trait DispatchStep: Send + Sync + 'static {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall;
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/dispatch.rs"]
mod tests;
