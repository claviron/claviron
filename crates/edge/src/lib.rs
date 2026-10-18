//! Protocol-agnostic HTTP edge composition.
//!
//! Transport drivers hand accepted connection and request facts to this crate.
//! It owns host admission and selects the response owner: a merged application
//! router, an axum-free bypass such as upstream/FCGI, or a native session
//! binding. HTTP/1.1, HTTP/2, and HTTP/3 wire mechanics remain in their drivers.

// The Axum upstream handler proves Send through nested native Hyper futures.
// Match upstream's finite proof depth without boxing the request path.
#![recursion_limit = "256"]

#[cfg(feature = "access-log")]
mod access;
pub mod config;

#[cfg(all(feature = "server", feature = "db"))]
mod listener;
// `pipeline` owns both request paths: merged routers and direct bypass
// adapters. `transport` is what a transport driver resolves a host with.
// `loader` combines host declarations; `service` is the machinery behind them.
// neither is anyone else's business, so neither is public. See `transport`.
#[cfg(feature = "server")]
pub mod deny;
pub(crate) mod loader;
#[cfg(feature = "server")]
pub mod pipeline;
#[cfg(feature = "axum")]
pub use pipeline::merged::extensions as extension;
#[cfg(feature = "server")]
mod provider;
#[cfg(feature = "server")]
pub(crate) mod service;
#[cfg(feature = "server")]
pub mod transport;

#[cfg(feature = "server")]
use std::sync::Arc;

#[cfg(feature = "server")]
use claviron_runtime::AppState;
#[cfg(feature = "server")]
use claviron_runtime::registry::{
    Component, Error as RegistryError, Result as RegistryResult,
};
#[cfg(feature = "server")]
pub use loader::config::VhostDocument;
pub use loader::error::{Error, Result};
#[cfg(feature = "server")]
pub use loader::resolver::{HostBinding, VhostResolver};
#[cfg(feature = "server")]
pub use provider::{
    HttpConfigProvider, http_config_path, register_http_config,
    register_http_config_with_capabilities,
};

#[cfg(feature = "server")]
use crate::pipeline::HostAppResolver;

#[cfg(feature = "server")]
pub fn register(state: &AppState) -> RegistryResult<()> {
    use claviron_runtime::Register;
    let registry = state.registry_ref();
    VhostResolver::register(state)?;
    registry.insert(Arc::new(HostAppRegistry::default()))?;
    registry.insert(Arc::new(H2ConnectRegistry::default()))?;
    #[cfg(feature = "websocket")]
    registry.insert(Arc::new(H1UpgradeRegistry::default()))?;
    #[cfg(feature = "webtransport")]
    registry.insert(Arc::new(WebTransportRegistry::default()))?;

    #[cfg(feature = "access-log")]
    registry.register(Arc::new(access::AccessLogProvider))?;
    #[cfg(feature = "db")]
    {
        // Registered everywhere; `Scoped` keeps it running in workers.
        use crate::listener::VhostAliasListener;
        registry.register(Arc::new(VhostAliasListener::default()))?;
    }

    // Boot compiles every host's router from the services a serving process
    // registers. The master serves nothing and registers none of them, so
    // this is the one edge provider a master does not carry; see
    // ARCHITECTURE.md §3.5.
    if state.is_master() {
        #[cfg(feature = "orbit")]
        state.events().listen::<claviron_runtime::ShutdownInitiated, _>(|state, _| async move {
            use std::collections::BTreeSet;

            use claviron_orbit::transport::TransportSegment;

            let Some(vhosts) = state.registry_ref().resolve::<VhostResolver>() else {
                return Ok(());
            };
            let fleets: BTreeSet<_> = vhosts.snapshot().canonical_entries()
                .flat_map(|(_, record)| record.config.orbit().iter().map(|route| route.fleet_name))
                .collect();
            for fleet in fleets {
                match TransportSegment::existing_for_fleet(fleet.as_str()) {
                    Ok(Some(segment)) => {
                        if let Err(error) = segment.announce_edge_shutdown(state.name()) {
                            tracing::warn!(%fleet, "could not announce edge shutdown over Link: {error}");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => tracing::warn!(%fleet, "could not inspect Link for edge shutdown: {error}"),
                }
            }
            Ok(())
        })?;
        return Ok(());
    }
    registry.register(Arc::new(HostAppResolver::default()))?;
    Ok(())
}

/// Declare a host during application registration, after [`register`].
///
/// This declaration survives boot and reload. File and code declarations
/// share one namespace: duplicate host keys or conflicting aliases fail
/// preparation rather than silently replacing another host. Listener and TLS
/// preparation use the ordinary Edge lifecycle; this does not open a listener.
/// Routes are registered separately with `register_routable`.
#[cfg(feature = "server")]
pub fn register_host(
    state: &AppState,
    host: Host,
    config: impl Into<Arc<HostConfig>>,
) -> RegistryResult<()> {
    let config = config.into();
    register_hosts(state, move |_| {
        Ok(vec![(host.clone(), Arc::clone(&config))])
    })
}

/// Declare hosts from a consumer's prepared configuration snapshot.
///
/// Register the source once, after [`register`] and before boot. It is called
/// only while rebuilding the host table, including on reload and vhost source
/// changes, never for a request. Return an empty list to declare no hosts.
/// Errors retain the previously published table.
///
/// Read the consumer's prepared `state.config_reader().snapshot::<Owner>()`,
/// not its boot-published resources, and do not perform file IO here. A full
/// reload prepares configuration owners before rebuilding hosts; a consumer's
/// runtime snapshot might still belong to the previous generation at that
/// point. A consumer-specific observer alone does not reload listeners or
/// routers; use the normal full reload to apply its hosting changes.
/// [`VhostDocument`] can be nested in the owner's config without duplicating
/// the host schema or its declaration equality; return its `declarations()`.
#[cfg(feature = "server")]
pub fn register_hosts(
    state: &AppState,
    source: impl Fn(&AppState) -> Result<Vec<(Host, Arc<HostConfig>)>>
    + Send
    + Sync
    + 'static,
) -> RegistryResult<()> {
    let resolver = state
        .registry_ref()
        .resolve::<VhostResolver>()
        .ok_or_else(|| RegistryError::msg("VhostResolver not registered"))?;
    resolver
        .register_hosts(source)
        .map_err(|error| RegistryError::msg(error.to_string()))
}

/// Register a provider that contributes routes to selected edge hosts.
///
/// The provider is inserted into the normal runtime registry so its lifecycle
/// hooks run like any other provider. The same `Arc` is also added to the
/// `HostAppRegistry`, which is only consulted while building a host snapshot.
#[cfg(all(feature = "server", feature = "axum"))]
pub fn register_routable<R>(
    state: &AppState,
    provider: Arc<R>,
) -> RegistryResult<()>
where
    R: Routable + Component<AppState> + 'static,
{
    let registry = state.registry_ref();
    registry.register(provider.clone())?;
    let host_apps = registry
        .resolve::<pipeline::HostAppRegistry>()
        .ok_or_else(|| RegistryError::msg("HostAppRegistry not registered"))?;
    host_apps.register_routable(provider);
    Ok(())
}

/// Register a provider that exclusively owns selected edge hosts.
#[cfg(feature = "server")]
pub fn register_host_binding_provider<R>(
    state: &AppState,
    provider: Arc<R>,
) -> RegistryResult<()>
where
    R: HostBindingProvider + Component<AppState> + 'static,
{
    let registry = state.registry_ref();
    registry.register(provider.clone())?;
    let host_apps = registry
        .resolve::<pipeline::HostAppRegistry>()
        .ok_or_else(|| RegistryError::msg("HostAppRegistry not registered"))?;
    host_apps.register_binding_provider(provider);
    Ok(())
}

pub use claviron_types::Host;
pub use config::HostConfig;
#[cfg(feature = "server")]
pub use pipeline::bypass::{BypassHandler, BypassService};
#[cfg(feature = "server")]
pub use pipeline::{
    AnyAuthenticated, AuthContext, H2ConnectAccept, H2ConnectBinding,
    H2ConnectHandler, H2ConnectRegistry, H2ConnectReject, H2ConnectResult,
    HostAppRegistry, HostBindingProvider, RouteBinding,
};
#[cfg(all(feature = "server", feature = "websocket"))]
pub use pipeline::{
    H1UpgradeAccept, H1UpgradeAuthenticated, H1UpgradeBinding,
    H1UpgradeContext, H1UpgradeHandler, H1UpgradeRegistry, H1UpgradeReject,
    H1UpgradeResult, H1UpgradeRoute, H1UpgradeState, is_h1_header_upgrade,
    websocket_accept_key, websocket_response_headers,
};
#[cfg(all(feature = "server", feature = "webtransport"))]
pub use pipeline::{
    H3WebTransportSession, WebTransportAccept, WebTransportBinding,
    WebTransportHandler, WebTransportRegistry, WebTransportReject,
    WebTransportResult,
};
#[cfg(all(feature = "server", feature = "axum"))]
pub use pipeline::{Routable, RouteLayerContext};
