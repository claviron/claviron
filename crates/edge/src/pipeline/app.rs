//! Optional Axum adaptation for explicitly supplied Rust applications.

use std::sync::Arc;

use axum::Router;
use claviron_http::{RouteBody, RouteFuture, RouteResponse, ServiceResult};
use claviron_http::routing::AxumPipeline;
use claviron_runtime::AppState;
use claviron_types::Host;
use futures_util::FutureExt;
use http::Request;
use std::future::Future;
use tower::ServiceExt;

use super::apps::HostAppRegistry;
use super::other::{OtherHandler, OtherService};
use super::host::{
    ApplicationHandler, HostHandler, ApplicationStage,
    HostHandlerFuture, HostRequest, dispatch_router,
};
use crate::HostConfig;

struct AxumApplication(Router);

impl OtherHandler for AxumApplication {
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        Box::pin(ApplicationHandler::call(self.0.clone(), request).map(Ok))
    }
}

pub(crate) fn owner(router: Router) -> OtherService {
    OtherService::new(AxumApplication(router))
}

/// Run registered Rust contributions before an application-owned Axum router.
/// An empty contribution set retains the original router without a wrapper.
pub(crate) fn compose_router(
    backend: Router,
    applications: &[Arc<dyn HostHandler>],
) -> Router {
    if applications.is_empty() {
        return backend;
    }
    let pipeline = ApplicationStage::new(backend, applications.to_vec());
    Router::new().fallback(move |request: Request<axum::body::Body>| {
        pipeline.clone()
            .handle(request.map(claviron_http::boxed_body))
            .map(claviron_http::into_axum_response)
    })
}

/// Native backends enter Axum only if an application or layer was supplied.
pub(crate) fn compose(
    state: &AppState,
    host: &Host,
    config: Arc<HostConfig>,
    applications: &[Router<AppState>],
    registry: Option<&HostAppRegistry>,
    native: OtherService,
) -> OtherService {
    if applications.is_empty() && registry.is_none() {
        return native;
    }
    let downstream = dispatch_router(native.clone());
    let router = if applications.is_empty() {
        downstream.clone()
    } else {
        applications.iter().cloned().fold(Router::new(), Router::merge)
            .layer(axum::Extension(AxumPipeline::new(downstream.clone())))
            .with_state(state.clone())
            .merge(downstream.clone())
    };
    if let Some(layered) = registry.and_then(|registry| registry.layer_routable_routes(
        state, host, config, AxumPipeline::new(downstream), router.clone(),
    )) {
        owner(layered)
    } else if applications.is_empty() {
        native
    } else {
        owner(router)
    }
}

impl ApplicationHandler for Router {
    fn call(self, request: Request<RouteBody>) -> impl Future<Output = RouteResponse> + Send {
        self.oneshot(request.map(axum::body::Body::new)).map(|reply| match reply {
            Ok(response) => claviron_http::from_axum_response(response),
            Err(never) => match never {},
        })
    }
}

/// Adapt an application-owned Axum router to a native Rust contribution.
/// The router owns its response, including a 404. Status never means Unhandled.
pub fn application(router: Router) -> Arc<dyn HostHandler> {
    Arc::new(AxumApplication(router))
}

impl HostHandler for AxumApplication {
    fn call(&self, request: HostRequest) -> HostHandlerFuture {
        Box::pin(ApplicationHandler::call(self.0.clone(), request.into_request())
            .map(ServiceResult::Handled))
    }
}

/// Configured REST endpoints are a Rust application, before native files and
/// backends. Their WebSocket sessions retain the separate native boundary.
#[cfg(feature = "broadcast")]
pub(crate) fn configured(
    state: &AppState,
    config: &HostConfig,
) -> Option<Router> {
    use claviron_pusher::PusherTarget;
    config.broadcast_cfg().and_then(|config| match &config.target {
        PusherTarget::Fleet => state.registry_ref().resolve::<claviron_broadcast::BroadcastService>()
            .map(claviron_broadcast::pusher_routes),
        PusherTarget::Http(_) => Some(claviron_pusher::pusher_routes(config)),
    })
}

#[cfg(feature = "broadcast")]
pub(crate) fn with_downstream(routes: Router, native: OtherService) -> OtherService {
    owner(routes.merge(dispatch_router(native)))
}

#[cfg(all(test, feature = "php_embed"))]
#[path = "../../tests/unit/application.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/unit/file_routes.rs"]
mod file_route_tests;
