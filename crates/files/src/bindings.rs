use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use claviron_http::ext::RealIpExt;
use claviron_http::{RequestContext, ServiceResult, into_axum_response};
use http::StatusCode;

use crate::service::try_handle_static_mounts;
use crate::site::FileSite;

/// Serve only explicitly configured static mounts before dynamic runners.
/// General filesystem, directory-index and SPA fallback remain downstream.
pub async fn axum_static_mount_handler(
    State(site): State<Arc<dyn FileSite>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let real_ip = req.extensions().get::<RealIpExt>().map(|ext| ext.0);
    let ctx =
        RequestContext { uri: req.uri(), headers: req.headers(), real_ip };
    match try_handle_static_mounts(&*site, ctx).await? {
        ServiceResult::Handled(resp) => Ok(into_axum_response(resp)),
        ServiceResult::Unhandled(_) => Ok(next.run(req).await),
    }
}
