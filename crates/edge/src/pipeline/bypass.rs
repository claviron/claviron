#[cfg(feature = "server")]
pub mod factories;
#[cfg(feature = "upstream")]
pub mod upstream;

use std::convert::Infallible;
use std::sync::Arc;

use claviron_files::ResourceAdapter;
use claviron_http::{RouteBody, RouteFuture, RouteResponse};
#[cfg(feature = "php_embed")]
use claviron_php_embed::{PhpApplicationAdapter, PhpScriptAdapter};
#[cfg(feature = "php_fcgi")]
use claviron_php_fcgi::PhpFpmAdapter;
#[cfg(feature = "broadcast")]
use claviron_pusher::PusherAdapter;
use http::{Request, Response};

#[cfg(feature = "upstream")]
use self::upstream::UpstreamBypass;

/// Application-owned bypass endpoint.
///
/// This is the generic escape hatch for code that wants the vhost
/// dispatcher and connection context, but not the Axum router stack.
/// Implementations receive the request after transport bodies are erased
/// to [`RouteBody`] and return the canonical bypass response type.
pub trait BypassHandler: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        "bypass_handler"
    }

    fn call(
        &self,
        req: Request<RouteBody>,
    ) -> RouteFuture;
}

/// Variants that bypass the Axum router. Each variant owns an adapter
/// that handles a `Request<RouteBody>` directly — no Axum, no Tower,
/// no generics. Hyper-stack only: the request body comes in already
/// erased to [`RouteBody`] (the bypass-pipeline body type), so adapters
/// stay non-generic and the dispatch chain has zero `axum::*` types.
///
/// Transport boundaries box their native body into `RouteBody` before
/// calling `handle`:
///
/// * h1/h2: `claviron_hyper::service` boxes `Incoming` after host admission.
/// * h3: `claviron_h3` boxes `Full`
///   (buffered) or `StreamBody` (streaming).
#[derive(Clone)]
pub enum BypassService {
    #[cfg(feature = "php_fcgi")]
    Fpm(PhpFpmAdapter),
    #[cfg(feature = "php_embed")]
    PhpScript(PhpScriptAdapter),
    #[cfg(feature = "php_embed")]
    PhpApplication(PhpApplicationAdapter),
    #[cfg(feature = "broadcast")]
    Pusher(PusherAdapter),
    #[cfg(feature = "upstream")]
    Upstream(UpstreamBypass),
    Asset(ResourceAdapter),
    Handler(Arc<dyn BypassHandler>),
    /// Default no-upstream variant kept so the enum compiles even when no
    /// upstream features are enabled.
    Noop,
}

impl BypassService {
    /// Whether this backend times the wait for the reply's head itself,
    /// against the same `requests.head_timeout` the edge hands it — the
    /// listener's, narrowed by the host and by the backend's own block. The
    /// edge then sets no clock of its own for that wait: the limit still
    /// holds, measured once.
    ///
    /// Only a backend of this crate says so. Bypass leaves the router's
    /// layers behind, not the limits a server needs to stay well, so a
    /// handler an application wrote ([`Self::Handler`]) is timed by the edge
    /// whatever it does inside.
    pub(crate) fn times_its_head(&self) -> bool {
        match self {
            #[cfg(feature = "upstream")]
            Self::Upstream(_) => true,
            _ => false,
        }
    }

    /// Dispatch a fully-erased request to the chosen adapter.
    ///
    /// `req` already carries [`RouteBody`]; transports must box their
    /// native body into `RouteBody` (via `boxed_body`) before reaching
    /// this point. See type docs on `BypassService` for the boundary
    /// list.
    pub async fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, Infallible> {
        match self {
            #[cfg(feature = "php_fcgi")]
            Self::Fpm(svc) => svc.handle(req).await,
            #[cfg(feature = "broadcast")]
            Self::Pusher(svc) => svc.handle(req).await,
            #[cfg(feature = "php_embed")]
            Self::PhpScript(svc) => svc.handle(req).await,
            #[cfg(feature = "php_embed")]
            Self::PhpApplication(svc) => svc.handle(req).await,
            #[cfg(feature = "upstream")]
            Self::Upstream(svc) => svc.handle(req).await,
            Self::Asset(svc) => svc.handle(req).await,
            Self::Handler(handler) => handler.call(req).await,
            Self::Noop => {
                let _ = req;
                Ok(Response::new(claviron_http::empty_body()))
            }
        }
    }
}
