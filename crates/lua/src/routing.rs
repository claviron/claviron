//! Host-independent preparation of configured Lua application routes.
//!
//! Edge and standalone consumers supply typed HTTP declarations; this module
//! neither reads HostConfig nor owns host selection or application lifecycle.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Router, middleware};
use claviron_http::config::routes::LuaRouteConfig;
use claviron_http::ext::PathPrefix;
use claviron_http::policy::HttpPolicyOverride;
use claviron_http::{boxed_body, into_axum_response};
use claviron_runtime::AppState;

use crate::serve::LuaApplications;

/// Mount configured applications with the most specific prefix outermost.
/// Preparation runs once when the caller builds its router, not per request.
pub fn build_router(
    router: Router,
    state: &AppState,
    routes: &[LuaRouteConfig],
) -> Router {
    let mut applications = routes.to_vec();
    applications.sort_by(|a, b| match (&a.path, &b.path) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
    });
    let mut router = router;
    for application in applications {
        router = build_application_router(
            router,
            state,
            application.path,
            application.application,
            claviron_http::policy::HttpPolicyOverride {
                requests: application.requests,
                responses: application.responses,
            },
        );
    }
    router
}

#[derive(Clone)]
struct ApplicationRoute {
    path: Option<PathPrefix>,
    application: Arc<str>,
    service: Option<Arc<LuaApplications>>,
    route: HttpPolicyOverride,
}

async fn dispatch(
    State(route): State<ApplicationRoute>,
    request: Request,
    next: Next,
) -> Response {
    if route
        .path
        .as_ref()
        .is_some_and(|prefix| !prefix.has_path_segment(request.uri().path()))
    {
        return next.run(request).await;
    }
    let Some(application) = route
        .service
        .as_ref()
        .and_then(|service| service.get(&route.application))
    else {
        return (
            http::StatusCode::SERVICE_UNAVAILABLE,
            [(http::header::RETRY_AFTER, "1")],
        )
            .into_response();
    };
    let (parts, body) = request.into_parts();
    let request = http::Request::from_parts(parts, boxed_body(body));
    let policy = crate::app::route_policy(&request, &route.route);
    into_axum_response(application.handle(request, policy).await)
}

/// Mount one Lua application at a segment-safe prefix; a request outside it
/// falls through.
pub fn build_application_router(
    base: Router,
    state: &AppState,
    path: Option<PathPrefix>,
    application: impl Into<Arc<str>>,
    route: HttpPolicyOverride,
) -> Router {
    let application = application.into();
    // A reload may declare the application later without rebuilding this mount.
    let service = state.registry_ref().resolve::<LuaApplications>();
    if service
        .as_ref()
        .is_some_and(|service| !service.is_declared(&application))
    {
        tracing::warn!(application = %application, "Lua route names an application absent from lua.yaml; it answers 503 until one is declared");
    }
    base.layer(middleware::from_fn_with_state(
        ApplicationRoute { path, application, service, route },
        dispatch,
    ))
}
