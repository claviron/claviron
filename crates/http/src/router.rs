use std::sync::Arc;

use crate::{RouteBody, RouteFuture};
use http::Request;

/// Low-level HTTP extension point without owning a listener.
///
/// The transport and host boundary hand over a request with an erased body
/// and typed extensions. A Rust handler or alternative router owns the response
/// and can use this contract
/// without depending on the Axum router or middleware stack.
///
/// # Hot-path contract
///
/// An unselected backend or inactive configuration must not impose its own
/// lookup, allocation, copying, synchronization, timer, task or polling work
/// on this path. Backend-specific future state belongs behind its own
/// `RouteFuture` boundary, never in a shared async match. Capabilities that
/// add no request-path cost need not be removed or hidden behind new features.
/// Active transport and host-policy contracts still apply.
pub trait OtherHandler: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        "other_handler"
    }

    /// Whether this handler enforces the request policy's head timeout itself.
    ///
    /// The default leaves head timing to the caller. A handler returning `true`
    /// owns enforcement of `HttpPolicy.requests.head_timeout`, including the
    /// timeout response and cancellation; the caller does not start a second
    /// head timer. This does not change admission or request/response limits.
    fn times_its_head(&self) -> bool {
        false
    }

    fn call(
        &self,
        req: Request<RouteBody>,
    ) -> RouteFuture;
}

/// Shared ownership of an application-supplied, non-Axum Rust handler/router.
///
/// This service is one Rust router choice in a prepared application or host.
/// It neither owns the host pipeline nor selects or skips its layers.
#[derive(Clone)]
pub struct OtherService {
    handler: Arc<dyn OtherHandler>,
}

impl OtherService {
    pub fn new(handler: impl OtherHandler) -> Self {
        Self { handler: Arc::new(handler) }
    }

    pub fn times_its_head(&self) -> bool {
        self.handler.times_its_head()
    }

    /// Transfer the request to the Rust handler without wrapping its future.
    pub fn handle(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        self.handler.call(request)
    }
}

impl From<Arc<dyn OtherHandler>> for OtherService {
    fn from(handler: Arc<dyn OtherHandler>) -> Self {
        Self { handler }
    }
}

/// A host-free Rust router used by both runner and Edge composition.
#[derive(Clone)]
pub enum RustRouter {
    #[cfg(feature = "axum")]
    Axum(axum::Router),
    #[cfg(feature = "warp")]
    Warp(crate::warp::Router),
    Other(OtherService),
}

impl RustRouter {
    pub fn kind(&self) -> &'static str {
        match self {
            #[cfg(feature = "axum")]
            Self::Axum(_) => "axum",
            #[cfg(feature = "warp")]
            Self::Warp(_) => "warp",
            Self::Other(_) => "other",
        }
    }

    /// Whether the router is handed the accepted connection, host name and
    /// policy as request extensions. Another handler receives the request as
    /// it arrived, with nothing written into it. A framework router reads
    /// request extensions through its own extractors and always receives them.
    pub fn reads_identity(&self) -> bool {
        match self {
            Self::Other(_) => false,
            #[cfg(feature = "warp")]
            Self::Warp(_) => true,
            #[cfg(feature = "axum")]
            Self::Axum(_) => true,
        }
    }

    /// Transfer one request to the selected router without transport state.
    pub fn handle(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        match self {
            Self::Other(service) => service.handle(request),
            #[cfg(feature = "warp")]
            Self::Warp(router) => router.handle(request),
            #[cfg(feature = "axum")]
            Self::Axum(router) => {
                use tower::ServiceExt;
                let router = router.clone();
                Box::pin(async move {
                    let Ok(response) = router
                        .oneshot(request.map(axum::body::Body::new))
                        .await;
                    Ok(crate::from_axum_response(response))
                })
            }
        }
    }
}

#[cfg(feature = "warp")]
impl From<crate::warp::Router> for RustRouter {
    fn from(router: crate::warp::Router) -> Self {
        Self::Warp(router)
    }
}

#[cfg(feature = "axum")]
impl From<axum::Router> for RustRouter {
    fn from(router: axum::Router) -> Self {
        Self::Axum(router)
    }
}

impl From<OtherService> for RustRouter {
    fn from(service: OtherService) -> Self {
        Self::Other(service)
    }
}

#[cfg(test)]
#[path = "../tests/router.rs"]
mod tests;
