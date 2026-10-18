use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Extension;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::any;
use claviron_http::{boxed_body, into_axum_response};
use http::Request;

use crate::http::handler::handle_pusher_request;
use crate::{PusherConfig, PusherTarget};

/// Build the per-host Pusher router. The `Arc<PusherConfig>` is pinned as a
/// router Extension so the handler can extract it without going through
/// `HostConfigExt` — that would introduce a cycle (host-config knows
/// `PusherConfig`; the router would re-import host-config to read it back).
pub fn http_pusher_router(cfg: Arc<PusherConfig>) -> Router {
    Router::new()
        .route("/app/{key}", any(http_pusher_handler))
        .route("/apps/{id}", any(http_pusher_handler))
        .route("/apps/{id}/{*rest}", any(http_pusher_handler))
        .layer(Extension(cfg))
}

async fn http_pusher_handler(
    Extension(cfg): Extension<Arc<PusherConfig>>,
    req: Request<Body>,
) -> Result<Response, StatusCode> {
    let PusherTarget::Http(http) = &cfg.target else {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };
    tracing::debug!(address = %http.address, "handling external Pusher request");
    // Axum binding boundary: erase axum::Body into RouteBody for the
    // bypass-style pusher handler so the dispatch chain stays
    // axum-free at the handler level.
    let (parts, body) = req.into_parts();
    let req = Request::from_parts(parts, boxed_body(body));
    Ok(into_axum_response(handle_pusher_request(http, req).await))
}
