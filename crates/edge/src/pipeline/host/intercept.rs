//! Prepared Rust application interception before one configured owner.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use claviron_http::{RouteBody, RouteFuture, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::{Request, request::Parts};

use super::ApplicationHandler;
use crate::pipeline::bypass::{BypassHandler, BypassService};

/// An unclaimed request with its original body still inaccessible.
///
/// Head inspection/changes do not expose or poll the body. To handle the body,
/// consume this value with `into_request`; it cannot then be returned as
/// Unhandled. Only the pipeline constructs this capability.
pub struct InterceptRequest {
    head: Parts,
    body: RouteBody,
}

impl InterceptRequest {
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

pub type InterceptResult = ServiceResult<RouteResponse, InterceptRequest>;
pub type InterceptFuture = Pin<Box<dyn Future<Output = InterceptResult> + Send>>;

/// A prepared Rust application's request entry point.
///
/// Return Handled even for an error response. Return Unhandled with the supplied
/// capability to continue with the same unread body. Status codes never select
/// another application. Runtime heterogeneous registration erases each active
/// application's future here; absent contributions install no stage.
pub trait ApplicationInterceptor: Send + Sync + 'static {
    fn call(&self, request: InterceptRequest) -> InterceptFuture;
}

struct Composition<H> {
    applications: Box<[Arc<dyn ApplicationInterceptor>]>,
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
    pub fn new(backend: H, applications: Vec<Arc<dyn ApplicationInterceptor>>) -> Self {
        Self { inner: Arc::new(Composition {
            applications: applications.into_boxed_slice(), backend,
        }) }
    }
}

impl<H> ApplicationStage<H>
where
    H: ApplicationHandler + Clone + Sync + 'static,
{
    pub async fn handle(self, request: Request<RouteBody>) -> RouteResponse {
        let mut request = InterceptRequest::new(request);
        for application in &self.inner.applications {
            request = match application.call(request).await {
                ServiceResult::Handled(response) => return response,
                ServiceResult::Unhandled(request) => request,
            };
        }
        self.inner.backend.clone().call(request.into_request()).await
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

impl<H> BypassHandler for ApplicationStage<H>
where
    H: ApplicationHandler + Clone + Sync + 'static,
{
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        Box::pin(self.clone().handle(request).map(Ok))
    }
}

pub(crate) fn compose_bypass(
    backend: BypassService,
    applications: &[Arc<dyn ApplicationInterceptor>],
) -> BypassService {
    if applications.is_empty() {
        backend
    } else {
        BypassService::new(ApplicationStage::new(backend, applications.to_vec()))
    }
}

#[cfg(feature = "axum")]
mod axum;
#[cfg(feature = "axum")]
pub(crate) use self::axum::compose_router;

#[cfg(test)]
#[path = "../../../tests/pipeline/host/intercept.rs"]
mod tests;
