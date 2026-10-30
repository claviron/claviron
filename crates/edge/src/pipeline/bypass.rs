#[cfg(feature = "server")]
pub mod factories;
#[cfg(feature = "upstream")]
pub mod upstream;

#[cfg(any(feature = "php_fcgi", feature = "php_embed", feature = "broadcast"))]
mod adapters;

use std::sync::Arc;

use claviron_http::{RouteBody, RouteFuture};
use http::{Request, Response};

/// Low-level HTTP extension point without owning a listener.
///
/// The transport and host boundary hand over a request with an erased body
/// and typed context. An implementation owns the response: a Rust handler,
/// another runtime's adapter, or an alternative router can use this contract
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
pub trait BypassHandler: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        "bypass_handler"
    }

    /// Whether this handler enforces the request policy's head timeout itself.
    ///
    /// The default leaves head timing to the edge. A handler returning `true`
    /// owns enforcement of `HttpPolicy.requests.head_timeout`, including the
    /// timeout response and cancellation; the edge does not start a second
    /// head timer. This does not change admission or request/response limits.
    fn times_its_head(&self) -> bool {
        false
    }

    fn call(
        &self,
        req: Request<RouteBody>,
    ) -> RouteFuture;
}

/// Direct request ownership outside the Axum router and middleware stack.
///
/// Every backend implements [`BypassHandler`] and returns its own erased
/// [`RouteFuture`]. Dispatch returns that future directly; a large backend's
/// state never becomes part of another backend's concrete dispatch future.
/// Use [`Self::new`] for built-in adapters or application-owned handlers.
/// Native TCP carries the surrounding host future inline, so the handler's
/// erasure is the only future boxing boundary on an ordinary Bypass request.
/// Admission and head timing still own and cancel that same handler future.
///
/// There is no implicit static-file or `try_files` pass. A handler may compose
/// the public `claviron_files` APIs itself. The upstream adapter owns its
/// explicitly configured file fallback independently of this dispatcher.
///
/// Transport boundaries box their native body into `RouteBody` before
/// calling `handle`:
///
/// * h1/h2: `claviron_edge::tcp::service` boxes `Incoming` after host admission.
/// * h3: `claviron_edge::quic` boxes `Full`
///   (buffered) or `StreamBody` (streaming).
#[derive(Clone)]
pub enum BypassService {
    Handler(Arc<dyn BypassHandler>),
    /// An empty response without an application handler.
    Noop,
}

impl BypassService {
    pub fn new(handler: impl BypassHandler) -> Self {
        Self::Handler(Arc::new(handler))
    }

    pub(crate) fn times_its_head(&self) -> bool {
        match self {
            Self::Handler(handler) => handler.times_its_head(),
            Self::Noop => false,
        }
    }

    /// Dispatch a fully-erased request to the chosen adapter.
    ///
    /// `req` already carries [`RouteBody`]; transports must box their
    /// native body into `RouteBody` (via `boxed_body`) before reaching
    /// this point. See type docs on `BypassService` for the boundary
    /// list.
    pub fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> RouteFuture {
        match self {
            Self::Handler(handler) => handler.call(req),
            Self::Noop => Box::pin(async move {
                drop(req);
                Ok(Response::new(claviron_http::empty_body()))
            }),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/pipeline/bypass.rs"]
mod tests;
