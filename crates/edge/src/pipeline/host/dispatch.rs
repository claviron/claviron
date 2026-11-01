//! Request ownership calls registered by concrete HostLayer implementations.

use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use claviron_http::{RouteBody, RouteFuture, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::Request;

use super::{HostHandlerFuture, HostLayer, HostLayerBuilder};
use crate::pipeline::RustRouter;

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
    Response(RouteFuture),
}

impl DispatchFuture {
    pub fn ready(result: DispatchResult) -> Self {
        Self { inner: Work::Ready(ready(result)) }
    }

    pub fn new(work: impl Future<Output = DispatchResult> + Send + 'static) -> Self {
        Self { inner: Work::Request(Box::pin(work)) }
    }

    pub(super) fn application(work: HostHandlerFuture) -> Self {
        Self { inner: Work::Application(work) }
    }

    pub(super) fn response(work: RouteFuture) -> Self {
        Self { inner: Work::Response(work) }
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
            Work::Response(work) => work.as_mut().poll(context).map(|result| {
                match result {
                    Ok(response) => ServiceResult::Handled(response),
                    Err(never) => match never {},
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

impl<S> HostLayer<S> for Arc<dyn DispatchStep> {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.request(move |request| self.call(request));
        Ok(())
    }
}

impl<S> HostLayer<S> for RustRouter {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        match self {
            Self::Other(owner) => {
                layer.request(move |request| DispatchFuture::response(owner.handle(request)));
            }
            #[cfg(feature = "axum")]
            Self::Axum(router) => {
                use super::ApplicationHandler;
                layer.request(move |request| {
                    DispatchFuture::response(Box::pin(router.clone().call(request).map(Ok)))
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/dispatch.rs"]
mod tests;
