//! Prepared Rust host handlers before the configured files/backend chain.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use claviron_http::{RouteBody, RouteFuture, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::{Request, request::Parts};

use super::ApplicationHandler;
use crate::pipeline::other::{OtherHandler, OtherService};

/// An unclaimed request with its original body still inaccessible.
///
/// Head inspection/changes do not expose or poll the body. To handle the body,
/// consume this value with `into_request`; it cannot then be returned as
/// Unhandled. Only the pipeline constructs this capability.
pub struct HostRequest {
    head: Parts,
    body: RouteBody,
}

impl HostRequest {
    fn new(request: Request<RouteBody>) -> Self {
        let (head, body) = request.into_parts();
        Self { head, body }
    }

    pub fn head(&self) -> &Parts {
        &self.head
    }

    pub fn head_mut(&mut self) -> &mut Parts {
        &mut self.head
    }

    /// Claim the request, transferring its existing body without reading it.
    pub fn into_request(self) -> Request<RouteBody> {
        Request::from_parts(self.head, self.body)
    }
}

pub type HostHandlerResult = ServiceResult<RouteResponse, HostRequest>;
pub type HostHandlerFuture = Pin<Box<dyn Future<Output = HostHandlerResult> + Send>>;

/// A prepared Rust host handler.
///
/// Return Handled even for an error response. Return Unhandled with the supplied
/// capability to continue with the same unread body. Status codes never select
/// another application. Runtime heterogeneous registration erases each active
/// application's future here; absent contributions install no stage.
pub trait HostHandler: Send + Sync + 'static {
    fn call(&self, request: HostRequest) -> HostHandlerFuture;
}

struct Composition<H> {
    applications: Box<[Arc<dyn HostHandler>]>,
    backend: H,
}

/// Immutable Rust contributions followed by one configured request owner.
///
/// Prepare at boot/reload. Execution visits only those contributions, in their
/// registration order, without a recursive Next or per-request registry lookup.
/// A handled response stops the chain; otherwise the final owner receives the
/// original body once. Cancellation drops the pending future and its capability.
/// Active heterogeneous contributions each return an erased future; this is an
/// explicit application cost, not a cost imposed on unconfigured hosts.
/// The combined call stays under Edge's declared total head deadline. A backend
/// may additionally enforce its own narrower execution policy, as in Axum
/// composition. An empty contribution set keeps the original native owner and
/// its timing contract at cold composition rather than installing this stage.
pub struct ApplicationStage<H> {
    inner: Arc<Composition<H>>,
}

impl<H> Clone for ApplicationStage<H> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<H> ApplicationStage<H> {
    pub fn new(backend: H, applications: Vec<Arc<dyn HostHandler>>) -> Self {
        Self { inner: Arc::new(Composition {
            applications: applications.into_boxed_slice(), backend,
        }) }
    }
}

impl<H> ApplicationStage<H>
where
    H: ApplicationHandler + Clone + Sync + 'static,
{
    pub fn handle(self, request: Request<RouteBody>) -> impl Future<Output = RouteResponse> + Send {
        // The async traversal owns a concrete call; it clones the terminal
        // only after every host handler has returned the original request.
        self.execute(request, H::call)
    }
}

impl<H: Clone + Send + Sync + 'static> ApplicationStage<H> {
    async fn execute<C, F>(self, request: Request<RouteBody>, call: C) -> RouteResponse
    where
        C: FnOnce(H, Request<RouteBody>) -> F + Send,
        F: Future<Output = RouteResponse> + Send,
    {
        let mut request = HostRequest::new(request);
        for application in &self.inner.applications {
            request = match application.call(request).await {
                ServiceResult::Handled(response) => return response,
                ServiceResult::Unhandled(request) => request,
            };
        }
        call(self.inner.backend.clone(), request.into_request()).await
    }
}

impl<H> ApplicationHandler for ApplicationStage<H>
where
    H: ApplicationHandler + Clone + Sync + 'static,
{
    fn call(self, request: Request<RouteBody>) -> impl Future<Output = RouteResponse> + Send {
        self.handle(request)
    }
}

impl<H> OtherHandler for ApplicationStage<H>
where
    H: ApplicationHandler + Clone + Sync + 'static,
{
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        Box::pin(self.clone().handle(request).map(Ok))
    }
}

pub(crate) fn compose_other(
    backend: OtherService,
    applications: &[Arc<dyn HostHandler>],
) -> OtherService {
    if applications.is_empty() {
        backend
    } else {
        OtherService::new(ApplicationStage::new(backend, applications.to_vec()))
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/handler.rs"]
mod tests;
