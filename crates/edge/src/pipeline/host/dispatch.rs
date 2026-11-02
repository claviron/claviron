//! Request ownership calls registered by concrete HostLayer implementations.

use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::task::{Context, Poll};

use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::Request;

use super::HostHandlerFuture;

pub type DispatchResult = ServiceResult<RouteResponse, Request<RouteBody>>;

/// A ready decision stays inline. Existing application futures retain their
/// own erasure; adapting their output adds no second future allocation.
pub struct DispatchFuture {
    inner: Work,
}

enum Work {
    Ready(Ready<DispatchResult>),
    Request(Pin<Box<dyn Future<Output = DispatchResult> + Send>>),
    Application(HostHandlerFuture),
}

impl DispatchFuture {
    pub fn ready(result: DispatchResult) -> Self {
        Self { inner: Work::Ready(ready(result)) }
    }

    pub fn new(work: impl Future<Output = DispatchResult> + Send + 'static) -> Self {
        Self { inner: Work::Request(Box::pin(work)) }
    }

    pub(in crate::pipeline) fn application(work: HostHandlerFuture) -> Self {
        Self { inner: Work::Application(work) }
    }

}

impl Future for DispatchFuture {
    type Output = DispatchResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().inner {
            Work::Ready(work) => work.poll_unpin(context),
            Work::Request(work) => work.as_mut().poll(context),
            Work::Application(work) => work.as_mut().poll(context).map(|result| {
                match result {
                    ServiceResult::Handled(response) => ServiceResult::Handled(response),
                    ServiceResult::Unhandled(request) => {
                        ServiceResult::Unhandled(request.into_request())
                    }
                }
            }),
        }
    }
}

/// A body-owning operation. Registration in a host plan uses HostLayer; this
/// call contract also serves PHP's selected runner without another plan.
pub trait DispatchStep: Send + Sync + 'static {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture;
}
