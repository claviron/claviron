use std::sync::Arc;

#[cfg(feature = "axum")]
use axum::Router;
#[cfg(feature = "websocket")]
use claviron_http::upgrade::H1UpgradeRoute;
use claviron_runtime::AppState;
use claviron_runtime::registry::{AnyComponent, Result};
use claviron_types::Host;

use super::apps::BypassFactory;
use super::host::ApplicationInterceptor;
#[cfg(feature = "axum")]
use super::apps::StandaloneRouterFactory;
use crate::config::HostConfig;

/// Boot-time inputs for an optional Rust application layer.
/// `downstream` calls the already-selected native owner without re-entering
/// the Axum route contributions. It requires no second prefix lookup.
#[cfg(feature = "axum")]
pub struct RouteLayerContext<'a> {
    pub state: &'a AppState,
    pub host: &'a Host,
    pub config: Arc<HostConfig>,
    pub downstream: claviron_http::routing::AxumPipeline,
}

/// Provider-owned Rust applications bound to selected edge hosts at preparation.
/// Native `application` contributions require no Axum. The optional `routes`
/// and `layer_routes` hooks explicitly opt into Axum application composition.
pub trait Routable: AnyComponent<AppState> {
    /// Directive codecs and compilers owned by this provider. Enumerated only
    /// while preparing a host snapshot; never consulted during dispatch.
    #[cfg(feature = "axum")]
    fn route_extensions(
        &self
    ) -> Vec<Arc<dyn claviron_http::routing::RouteExtension<HostConfig>>> {
        Vec::new()
    }

    /// Select the canonical vhost key while preparing the routing snapshot.
    /// Config selectors use `vhost`/`vhosts`, not `host`/`hosts` or public aliases.
    /// Edge resolves request hostnames once; selection is not repeated per request.
    fn match_host(
        &self,
        key: &Host,
        host: &HostConfig,
    ) -> bool;

    /// Prepare this provider's native Rust request contribution at boot/reload.
    /// Unhandled returns the supplied request capability to the next contribution
    /// and ultimately the configured owner. Only changed host preparation calls
    /// this factory; request dispatch calls the returned capability directly.
    fn application(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> Result<Option<Arc<dyn ApplicationInterceptor>>> {
        Ok(None)
    }

    /// Build routes from the registered shared instance at host preparation.
    /// Handlers may capture this Arc directly; host selection stays here,
    /// rather than being repeated on each request.
    #[cfg(feature = "axum")]
    fn routes(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> Result<Option<Router<AppState>>> {
        Ok(None)
    }

    /// Prepare an optional Axum layer around the Rust application and selected
    /// native downstream. Return None when inactive: no Axum invocation remains.
    /// Host security, CORS and response policy are outside this application.
    /// Preparation runs per selected scope at boot/reload. Exclusive standalone
    /// and Bypass bindings retain their own application composition.
    #[cfg(feature = "axum")]
    fn layer_routes(
        &self,
        _context: RouteLayerContext<'_>,
        _router: Router,
    ) -> Option<Router> {
        None
    }

    #[cfg(feature = "websocket")]
    fn h1_upgrades(
        &self,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> Result<Vec<H1UpgradeRoute>> {
        Ok(Vec::new())
    }
}

/// Provider-owned exclusive binding for one selected host.
pub trait HostBindingProvider: AnyComponent<AppState> {
    /// Match only canonical vhost keys at boot/reload. Public hostname aliases
    /// belong to Edge resolution, not the provider's `vhost`/`vhosts` selectors.
    fn match_host(
        &self,
        key: &Host,
        host: &HostConfig,
    ) -> bool;

    fn binding(
        &self,
        key: &Host,
        host: Arc<HostConfig>,
    ) -> Result<Option<RouteBinding>>;
}

#[derive(Clone)]
pub enum RouteBinding {
    #[cfg(feature = "axum")]
    Standalone(Arc<StandaloneRouterFactory>),
    Bypass(Arc<BypassFactory>),
}
