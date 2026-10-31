//! Optional Axum adaptation for explicitly supplied Rust applications.

use std::sync::Arc;

use axum::Router;
use claviron_http::{RouteBody, RouteFuture};
use claviron_http::routing::{AxumPipeline, RouteLayer};
use claviron_runtime::{AppState, registry::Result as RegistryResult};
use claviron_types::Host;
use futures_util::FutureExt;
use http::Request;

use super::apps::HostAppRegistry;
use super::bypass::{BypassHandler, BypassService};
use super::host::{ApplicationHandler, dispatch_router};
use crate::HostConfig;

struct AxumApplication(Router);

impl BypassHandler for AxumApplication {
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        Box::pin(self.0.clone().call(request).map(Ok))
    }
}

pub(crate) fn owner(router: Router) -> BypassService {
    BypassService::new(AxumApplication(router))
}

/// Native backends enter Axum only if an application or layer was supplied.
pub(crate) fn compose(
    state: &AppState,
    host: &Host,
    config: Arc<HostConfig>,
    applications: &[Router<AppState>],
    registry: Option<&HostAppRegistry>,
    native: BypassService,
) -> BypassService {
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

pub(crate) fn layers(native: BypassService, layers: &[Arc<dyn RouteLayer>]) -> BypassService {
    if layers.is_empty() { return native; }
    owner(layers.iter().fold(dispatch_router(native), |router, layer| layer.apply(router)))
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
pub(crate) fn with_downstream(routes: Router, native: BypassService) -> BypassService {
    owner(routes.merge(dispatch_router(native)))
}

/// Explicit Axum view of a prepared native host, for application composition.
/// Config-only host publication does not call this adapter.
pub fn create_router(
    state: AppState,
    host: &Host,
    config: Arc<HostConfig>,
    applications: Vec<Router<AppState>>,
    #[cfg(feature = "websocket")] websocket: Option<Arc<super::H1UpgradeRegistry>>,
) -> RegistryResult<Router> {
    super::host::compile::prepare(
        &state, host, config, &[], applications,
        #[cfg(feature = "websocket")]
        websocket,
    ).map(dispatch_router)
}

/// Explicit Axum view without additional Rust route contributions.
pub fn create_default_router(
    state: AppState,
    host: &Host,
    config: Arc<HostConfig>,
    #[cfg(feature = "websocket")] websocket: Option<Arc<super::H1UpgradeRegistry>>,
) -> RegistryResult<Router> {
    create_router(state, host, config, Vec::new(),
        #[cfg(feature = "websocket")]
        websocket,
    )
}

#[cfg(all(test, feature = "php_embed"))]
#[path = "../../tests/unit/application.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/unit/file_routes.rs"]
mod file_route_tests;
