use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use claviron_http::ext::RealIpExt;
use claviron_http::{RequestContext, ServiceResult, into_axum_response};
use claviron_runtime::AppState;
use http::StatusCode;

use crate::service::{try_handle_files, try_handle_static_mounts};
use crate::site::FileSite;

/// Dispatch one already-selected mount without scanning a site's mount list.
pub async fn axum_selected_static_mount_handler(
    State(mount): State<Arc<crate::service::StaticMount>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let real_ip = req.extensions().get::<RealIpExt>().map(|ext| ext.0);
    let ctx =
        RequestContext { uri: req.uri(), headers: req.headers(), real_ip };
    match mount.try_handle(ctx).await {
        ServiceResult::Handled(response) => Ok(into_axum_response(response)),
        ServiceResult::Unhandled(_) => Ok(next.run(req).await),
    }
}

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

/// What [`axum_file_handler`] is built with: the runtime and the site it
/// serves, both given when the router is built rather than read per request.
#[derive(Clone)]
pub struct FileHandlerState {
    pub state: AppState,
    pub site: Arc<dyn FileSite>,
}

/// Axum middleware fallback that serves static files for the site it was
/// built for.
pub async fn axum_file_handler(
    State(FileHandlerState { state, site }): State<FileHandlerState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let real_ip = req.extensions().get::<RealIpExt>().map(|ext| ext.0);
    let ctx =
        RequestContext { uri: req.uri(), headers: req.headers(), real_ip };

    match try_handle_files(&state, &*site, ctx).await? {
        ServiceResult::Handled(resp) => Ok(into_axum_response(resp)),
        ServiceResult::Unhandled(_) => Ok(next.run(req).await),
    }
}
