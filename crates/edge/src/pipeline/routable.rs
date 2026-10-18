use std::sync::Arc;

#[cfg(feature = "axum")]
use axum::Router;
#[cfg(feature = "websocket")]
use claviron_http::upgrade::H1UpgradeRoute;
use claviron_runtime::AppState;
use claviron_runtime::registry::{AnyComponent, Result};
use claviron_types::Host;

use super::apps::BypassFactory;
#[cfg(feature = "axum")]
use super::apps::StandaloneRouterFactory;
use crate::config::HostConfig;

/// Boot-time inputs for a provider's host-wide route layer.
///
/// The router passed alongside this context contains the merged application
/// routes. `downstream` calls only the configured owner (the host fallback on
/// legacy hosts, the selected prefix on hosts with inherited policy layers),
/// allowing delegation without entering merged Rust application routes.
#[cfg(feature = "axum")]
pub struct RouteLayerContext<'a> {
    pub state: &'a AppState,
    pub host: &'a Host,
    pub config: Arc<HostConfig>,
    pub downstream: claviron_http::routing::AxumPipeline,
}

/// Provider-owned Rust routes bound to selected edge hosts.
#[cfg(feature = "axum")]
pub trait Routable: AnyComponent<AppState> {
    /// Directive codecs and compilers owned by this provider. Enumerated only
    /// while preparing a host snapshot; never consulted during dispatch.
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

    /// Build routes from the registered shared instance at host preparation.
    /// Handlers may capture this Arc directly; host selection stays here,
    /// rather than being repeated on each request.
    fn routes(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> Result<Option<Router<AppState>>> {
        Ok(None)
    }

    /// Wrap the assembled host router, including sibling routables and its
    /// config-driven fallback. Built at boot/reload, before Claviron's
    /// response headers, CORS, metrics and tracing layers are attached. Hosts
    /// with inherited route policy layers prepare this stack per selected
    /// prefix, inside those layers; other hosts prepare it once per host.
    /// Exclusive standalone/bypass bindings do not use this hook.
    /// `downstream` bypasses merged Rust contributions and calls the configured
    /// owner directly. Capture it at boot for navigation delegation;
    /// unlike request extensions, it is available before inner routes run.
    fn layer_routes(
        &self,
        _context: RouteLayerContext<'_>,
        router: Router,
    ) -> Router {
        router
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
