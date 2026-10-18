use std::sync::Arc;

use axum::body::Body as AxumBody;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Router, middleware};
use claviron_http::{ServiceResult, boxed_body, into_axum_response};
use claviron_php::PhpRuntime;
use claviron_runtime::AppState;

use crate::error::SapiGatewayError;
use crate::routing::proxy;
use crate::service::PhpScriptService;
use crate::{PhpApplicationAdapter, PhpApplicationService};

#[derive(Clone)]
struct ApplicationRoute {
    path: Option<claviron_http::ext::PathPrefix>,
    adapter: Option<PhpApplicationAdapter>,
}

async fn php_application_dispatch(
    State(route): State<ApplicationRoute>,
    req: Request,
    next: Next,
) -> Response {
    if route
        .path
        .as_ref()
        .is_some_and(|prefix| !prefix.has_path_segment(req.uri().path()))
    {
        return next.run(req).await;
    }
    // A matched application owns the whole prefix, including dotted paths.
    // Missing/stopped capacity must never fall through into another PHP script.
    let Some(adapter) = route.adapter else {
        return (
            http::StatusCode::SERVICE_UNAVAILABLE,
            [(http::header::RETRY_AFTER, "1")],
        )
            .into_response();
    };
    let (parts, body) = req.into_parts();
    let response = adapter
        .handle(http::Request::from_parts(parts, boxed_body(body)))
        .await;
    into_axum_response(response.unwrap())
}

/// Mount a named persistent application at a segment-safe URL prefix. The
/// original URI is preserved, and all methods/suffixes under the prefix belong
/// to the application. This does not allocate a pool or run an entry script.
pub fn build_application_router(
    base: Router,
    state: &AppState,
    runtime: PhpRuntime,
) -> Router {
    let adapter = runtime.application.as_ref().and_then(|name| {
        let service = state.registry_ref().resolve::<PhpApplicationService>()?;
        let adapter = match PhpApplicationAdapter::new(service, name)
            .environment(state, &runtime) {
            Ok(adapter) => adapter,
            Err(error) => {
                tracing::error!(application = %name, %error, "PHP application request environment could not be prepared");
                return None;
            }
        };
        Some(adapter.route_policy(claviron_http::policy::HttpPolicyOverride {
            requests: runtime.requests,
            responses: runtime.responses,
        }))
    });

    if adapter.is_none() {
        tracing::error!(application = ?runtime.application, "PHP application route unavailable; matched requests will return 503");
    }
    base.layer(middleware::from_fn_with_state(
        ApplicationRoute { path: runtime.path, adapter },
        php_application_dispatch,
    ))
}

impl IntoResponse for SapiGatewayError {
    fn into_response(self) -> Response {
        (self.status_code(), self.to_string()).into_response()
    }
}

#[allow(clippy::type_complexity)]
async fn php_sapi_dispatch(
    State((svc, runtime)): State<(Arc<PhpScriptService>, Arc<PhpRuntime>)>,
    req: Request,
    next: Next,
) -> Result<Response, SapiGatewayError> {
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

    match proxy::try_php_proxy(svc, runtime, req, None).await? {
        ServiceResult::Handled(resp) => Ok(into_axum_response(resp)),
        ServiceResult::Unhandled(req) => {
            let (parts, body) = req.into_parts();
            let req = http::Request::from_parts(parts, AxumBody::new(body));
            Ok(next.run(req).await)
        }
    }
}

/// Apply ONE SAPI runtime as an axum middleware layer. The runner
/// resolves its own `PhpSapiService` from the registry — caller
/// (vhost router) only knows about runners, not their service types.
/// Same contract as `claviron-php-fcgi::build_axum_router` so the
/// vhost router dispatches to either runner with the same call shape.
pub fn build_script_router(
    base: Router,
    state: &AppState,
    runtime: PhpRuntime,
) -> Router {
    let sapi_service = match state.registry_ref().resolve::<PhpScriptService>()
    {
        Some(svc) => svc,
        None => {
            tracing::error!("PhpSapiService not registered");
            return base;
        }
    };

    let layer = middleware::from_fn_with_state(
        (sapi_service, Arc::new(runtime)),
        php_sapi_dispatch,
    );
    base.layer(layer)
}
