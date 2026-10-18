//! How a caller reaches this crate: pick a service from an application and
//! send it a request. Binding it to a host — the merged router, the bypass
//! adapter, the file fallback — is the edge's, which owns hosts.
use claviron_http::{RouteBody, RouteResponse};
use http::Request;

pub use crate::app::UpstreamApp;
use crate::proxy::service::request::RequestScope;
use crate::proxy::service::{UpstreamRequestPolicy, UpstreamService};
/// Send `req` to `service`, allowing an HTTP/1 upgrade or an HTTP/2
/// extended CONNECT only where the caller's host permits it.
pub async fn call_upstream_service(
    service: UpstreamService,
    req: Request<RouteBody>,
    allow_h1_upgrade: bool,
    allow_connect_protocol: bool,
) -> RouteResponse {
    let mut scope = RequestScope::from_request(&req);
    scope.policy =
        UpstreamRequestPolicy { allow_h1_upgrade, allow_connect_protocol };
    let shutdown = service.inner.runtime.shutdown.clone().cancelled_owned();
    claviron_http::body::ends_at_shutdown(
        service.handle_scoped(req, scope).await,
        shutdown,
    )
}
