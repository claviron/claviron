//! File dispatch preparation consumes HTTP declarations, never HostConfig.

use crate::{
    FileHandlerState, FileSite, axum_file_handler, axum_static_mount_handler,
};
use axum::response::IntoResponse;
use axum::{Router, middleware};
use claviron_http::config::files::FilesConfig;
use claviron_http::config::statics::StaticConfig;
use claviron_runtime::AppState;
use std::sync::Arc;

/// General file/index/SPA fallback stays behind application owners.
pub fn build_router(
    state: AppState,
    config: Arc<FilesConfig>,
) -> Router {
    let site: Arc<dyn FileSite> = config;
    Router::new()
        .fallback(|| async { http::StatusCode::NOT_FOUND.into_response() })
        .layer(middleware::from_fn_with_state(
            FileHandlerState { state, site },
            axum_file_handler,
        ))
}

/// Only explicit mounts intercept the owner. No general filesystem probing
/// moves in front of a pinned PHP entry or another dynamic application.
pub fn mount(
    router: Router,
    config: Arc<FilesConfig>,
) -> Router {
    if config.statics.as_ref().is_none_or(Vec::is_empty) {
        return router;
    }
    let site: Arc<dyn FileSite> = config;
    router
        .layer(middleware::from_fn_with_state(site, axum_static_mount_handler))
}

/// RouteTable already chose this static owner. Files only maps the URI under
/// that mount and serves it, without another mount-list selection.
pub fn mount_selected(
    router: Router,
    root: std::path::PathBuf,
    config: StaticConfig,
) -> Router {
    let mount = Arc::new(crate::StaticMount::new(root, config));
    router.layer(middleware::from_fn_with_state(
        mount,
        crate::axum_selected_static_mount_handler,
    ))
}
