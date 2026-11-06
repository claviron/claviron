use std::sync::Arc;

#[cfg(feature = "websocket")]
use claviron_http::upgrade::H1UpgradeRoute;
use claviron_runtime::AppState;
use claviron_runtime::registry::{AnyComponent, Result};
use claviron_types::Host;

#[cfg(feature = "axum")]
use super::apps::axum::Factory as AxumRouterFactory;
use super::apps::other::Factory as OtherFactory;
use claviron_pipeline::HostHandler;
use crate::config::HostConfig;

pub(crate) use claviron_pipeline::factory::RouteProtocol;
pub(crate) use claviron_pipeline::factory::matches_protocol;

/// Provider contributions prepared for selected hosts and listener protocols.
/// Handlers, layers, directives and native upgrades enter the host plan before
/// custom redirects and the application's terminal RustRouter.
pub trait Routable: AnyComponent<AppState> {
    /// Select listener protocols at boot/reload. The default accepts all.
    /// Unselected contributions leave no request-time action or protocol check.
    /// Host matching and application/layer factories retain their signatures;
    /// they run only for selected protocols. Unbound lookups retain only
    /// contributions that accept every protocol family.
    fn match_protocol(
        &self,
        _protocol: RouteProtocol,
    ) -> bool {
        true
    }

    /// Directive codecs and compilers owned by this provider. Enumerated only
    /// while preparing a host snapshot; never consulted during dispatch.
    /// Native host extensions do not require the Axum application capability.
    fn route_extensions(
        &self
    ) -> Vec<Arc<dyn super::extension::RouteExtension>> {
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

    /// Prepare protocol-native session bindings for this selected host.
    /// Called at boot/reload, before application/layer preparation. Session
    /// admission and stream ownership remain with the native registries.
    fn prepare_sessions(
        &self,
        _state: &AppState,
        _key: &Host,
        _host: &HostConfig,
    ) -> Result<()> {
        Ok(())
    }

    /// Prepare this provider's native Rust request contribution at boot/reload.
    /// Unhandled returns the supplied request capability to the next contribution
    /// and ultimately the configured owner. Only changed host preparation calls
    /// this factory; request dispatch calls the returned capability directly.
    fn application(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> Result<Option<Arc<dyn HostHandler>>> {
        Ok(None)
    }

    /// Prepare native layers surrounding this host's selected request owner.
    /// Called only at boot/reload, including for explicit Axum/Other bindings.
    /// An empty or inactive layer set leaves no request-time wrapper.
    fn layers(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> Result<Vec<Arc<dyn super::extension::RouteLayer>>> {
        Ok(Vec::new())
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
    /// Select listener protocols at boot/reload. The default accepts all.
    fn match_protocol(
        &self,
        _protocol: RouteProtocol,
    ) -> bool {
        true
    }

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
    Axum(Arc<AxumRouterFactory>),
    #[cfg(feature = "warp")]
    Warp(Arc<super::apps::warp::Factory>),
    Other(Arc<OtherFactory>),
}
