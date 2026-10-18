use std::sync::Arc;

use axum::body::Body as AxumBody;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Router, middleware};
use claviron_http::{ServiceResult, boxed_body, into_axum_response};
use claviron_php::PhpRuntime;
use claviron_runtime::AppState;

use crate::error::{ExecuteError, GatewayError};
use crate::proxy;
use crate::service::PhpFcgiService;

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        // Keep the body text for now, mainly for debugging. Upstream-only callers
        // (non-Axum) can choose to hide details.
        (self.status_code(), self.to_string()).into_response()
    }
}

impl IntoResponse for ExecuteError {
    fn into_response(self) -> Response {
        (self.status_code(), self.to_string()).into_response()
    }
}

/// Per-runtime FCGI dispatch — single `PhpRuntime` bound to one
/// middleware layer. Path-prefix mismatch falls through to the next
/// layer (longest-prefix-wins ordering is owned by the vhost router
/// when stacking layers). Same shape and semantics as the embed
/// runner's `php_sapi_dispatch` so a host can mix `runner: fcgi` and
/// `runner: script` routes; whichever layer's prefix matches first
/// (= outermost layer) gets the request.
///
/// Static-probing is delegated to the next layer (file_handler) — when
/// `try_php_proxy` returns `Unhandled` (e.g. `runtime.try_files=true`,
/// a static file exists, a non-`.php` extension is not listed in
/// `entry_extensions`, or `validate_script` rejected a missing file),
/// `next.run(req)` lets the host's `statics:` / redirect / ACL / SPA
/// pipeline own static asset routing.
#[allow(clippy::type_complexity)]
async fn fcgi_dispatch(
    State((svc, runtime)): State<(Arc<PhpFcgiService>, Arc<PhpRuntime>)>,
    req: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let path = req.uri().path();

    if let Some(prefix) = &runtime.path
        && !prefix.has_path_segment(path)
    {
        return Ok(next.run(req).await);
    }

    // Axum middleware boundary: erase axum::Body into RouteBody for the
    // bypass dispatch chain; on Unhandled, re-wrap into axum::Body so
    // `next.run` can drive the rest of the Axum/Tower pipeline.
    let (parts, body) = req.into_parts();
    let req = http::Request::from_parts(parts, boxed_body(body));

    match proxy::try_fcgi_proxy(svc, runtime, req, None).await? {
        ServiceResult::Handled(resp) => Ok(into_axum_response(resp)),
        ServiceResult::Unhandled(req) => {
            let (parts, body) = req.into_parts();
            let req = http::Request::from_parts(parts, AxumBody::new(body));
            Ok(next.run(req).await)
        }
    }
}

/// Apply ONE FCGI runtime as an axum middleware layer. The runner
/// resolves its own `PhpFcgiService` from the registry — caller
/// (vhost router) only knows about runners, not their service types.
/// Each call adds its own layer; longest-prefix routes (added LAST
/// per tower's `.layer()` LIFO) sit outermost and intercept their
/// prefix before shallower routes see the request.
///
/// Caller's router MUST already have its fallback set: axum 0.8
/// records layers against the current fallback at the moment
/// `.layer()` is called, so a fallback added AFTER `.layer()` is NOT
/// wrapped by the layer and the gateway never fires.
pub fn build_fcgi_router(
    base: Router,
    state: &AppState,
    runtime: PhpRuntime,
) -> Router {
    let fcgi_service = match state.registry_ref().resolve::<PhpFcgiService>() {
        Some(svc) => svc,
        None => {
            tracing::error!("PhpFcgiService not registered");
            return base;
        }
    };

    let layer = middleware::from_fn_with_state(
        (fcgi_service, Arc::new(runtime)),
        fcgi_dispatch,
    );
    base.layer(layer)
}
