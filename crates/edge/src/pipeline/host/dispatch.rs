//! Request ownership calls registered by concrete HostLayer implementations.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use http::Request;

use super::{HostContext, HostHandlerFuture};

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
    Response(claviron_http::RouteFuture),
}

impl DispatchCall {
    /// Reuse an existing terminal HTTP future without adding another future box.
    /// Its response is Handled, including error statuses such as 404.
    pub fn response(work: claviron_http::RouteFuture) -> Self {
        Self::Pending(DispatchFuture { inner: Work::Response(work) })
    }
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
            Work::Response(work) => work.as_mut().poll(context).map(|reply| {
                let response = match reply {
                    Ok(response) => response,
                    Err(never) => match never {},
                };
                ServiceResult::Handled(response)
            }),
        }
    }
}

/// A body-owning operation. Registration in a host plan uses HostLayer; this
/// call contract also serves PHP's selected runner without another plan.
///
/// `context` lends the accepted connection, the selected host name and the
/// scope's policy for the duration of the call. Pending work is `'static`, so
/// an owner copies what that work needs before it returns. It is absent when
/// the plan runs with no accepted transport behind it.
pub trait DispatchStep: Send + Sync + 'static {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<HostContext<'_>>,
    ) -> DispatchCall;
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/dispatch.rs"]
mod tests;
