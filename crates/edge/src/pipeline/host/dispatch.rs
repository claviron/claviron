//! Native dispatch, independent of Rust application interception.

use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use claviron_http::{RouteBody, RouteFuture, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::Request;

use super::{ApplicationHandler, HostHandler, ApplicationStage};
use crate::pipeline::other::{OtherHandler, OtherService};

pub type DispatchResult = ServiceResult<RouteResponse, Request<RouteBody>>;

/// A synchronous decision stays inline; only actual asynchronous work is boxed.
pub struct DispatchFuture {
    inner: futures_util::future::Either<
        Ready<DispatchResult>,
        Pin<Box<dyn Future<Output = DispatchResult> + Send>>,
    >,
}

impl DispatchFuture {
    pub fn ready(result: DispatchResult) -> Self {
        Self { inner: futures_util::future::Either::Left(ready(result)) }
    }

    pub fn new(work: impl Future<Output = DispatchResult> + Send + 'static) -> Self {
        Self { inner: futures_util::future::Either::Right(Box::pin(work)) }
    }
}

impl Future for DispatchFuture {
    type Output = DispatchResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.poll_unpin(context)
    }
}

/// One prepared native dispatch step. Unhandled transfers the same request
/// and its unread body to the next owner. A handled status, including 404,
/// never means fallthrough. This is not a Rust application interceptor.
pub trait DispatchStep: Send + Sync + 'static {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture;
}

struct Plan<H> {
    steps: Box<[Arc<dyn DispatchStep>]>,
    terminal: H,
}

/// Active native steps and a terminal owner, prepared once at boot/reload.
///
/// The chain transfers Request<RouteBody> directly. It does not split, clone,
/// collect, decode or re-box the body. Cancellation drops the pending owner
/// and whatever request it holds. Only each active heterogeneous step retains
/// its existing future erasure; the surrounding call stays concrete.
pub struct DispatchChain<H> {
    plan: Arc<Plan<H>>,
}

impl<H> Clone for DispatchChain<H> {
    fn clone(&self) -> Self { Self { plan: self.plan.clone() } }
}

impl<H> DispatchChain<H> {
    pub fn new(terminal: H, steps: Vec<Arc<dyn DispatchStep>>) -> Self {
        Self { plan: Arc::new(Plan { steps: steps.into_boxed_slice(), terminal }) }
    }
}

impl<H: ApplicationHandler + Clone + Sync + 'static> DispatchChain<H> {
    pub async fn handle(self, mut request: Request<RouteBody>) -> RouteResponse {
        for step in &self.plan.steps {
            request = match step.call(request).await {
                ServiceResult::Handled(response) => return response,
                ServiceResult::Unhandled(request) => request,
            };
        }
        self.plan.terminal.clone().call(request).await
    }
}

impl<H: ApplicationHandler + Clone + Sync + 'static> ApplicationHandler for DispatchChain<H> {
    fn call(self, request: Request<RouteBody>) -> impl Future<Output = RouteResponse> + Send {
        self.handle(request)
    }
}

impl<H: ApplicationHandler + Clone + Sync + 'static> OtherHandler for DispatchChain<H> {
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        Box::pin(self.clone().handle(request).map(Ok))
    }
}

/// Keep Rust interception outside native dispatch without another erased call
/// boundary. Empty optional plans retain their original direct owner.
pub(crate) fn compose_dispatch_other(
    terminal: OtherService,
    steps: Vec<Arc<dyn DispatchStep>>,
    applications: &[Arc<dyn HostHandler>],
) -> OtherService {
    if steps.is_empty() {
        return super::compose_other(terminal, applications);
    }
    let dispatch = DispatchChain::new(terminal, steps);
    if applications.is_empty() {
        OtherService::new(dispatch)
    } else {
        OtherService::new(ApplicationStage::new(dispatch, applications.to_vec()))
    }
}

#[cfg(feature = "axum")]
mod axum;
#[cfg(feature = "axum")]
pub(crate) use self::axum::dispatch_router;

#[cfg(test)]
#[path = "../../../tests/pipeline/host/dispatch.rs"]
mod tests;
