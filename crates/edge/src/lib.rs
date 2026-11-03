//! HTTP edge composition with independently selected native transport drivers.
//!
//! Transport drivers hand accepted connection and request facts to this crate.
//! It owns host admission and prepares native file/backend dispatch, with
//! optional Rust application adapters and native session bindings. The `tcp`
//! and `quic` modules retain separate HTTP/1.1, HTTP/2,
//! and HTTP/3 wire mechanics and connection ownership.

// Native upstream dispatch proves Send through nested Hyper futures.
// Match upstream's finite proof depth without boxing the request path.
#![recursion_limit = "256"]

pub mod config;
#[cfg(feature = "access-log")]
mod log;

#[cfg(feature = "db")]
mod listener;
// `pipeline` prepares native host plans and optional Rust application adapters.
// `transport` is what a transport driver resolves a host with.
// `loader` combines host declarations; `service` is the machinery behind them.
// neither is anyone else's business, so neither is public. See `transport`.
pub(crate) mod loader;
pub mod pipeline;
pub use pipeline::extension;
mod provider;
pub(crate) mod service;
#[cfg(feature = "rate-limit")]
pub use pipeline::layers::HostQuotas;
pub mod transport;

use std::sync::Arc;

use claviron_runtime::AppState;
use claviron_runtime::registry::{
    Component, Error as RegistryError, Result as RegistryResult,
};
pub use loader::config::VhostDocument;
pub use loader::error::{Error, Result};
pub use loader::resolver::{HostBinding, VhostResolver};
pub use provider::{
    HttpConfigProvider, http_config_path, register_http_config,
    register_http_config_with_capabilities,
};

use crate::pipeline::HostAppResolver;

/// Code-owned alternative to a host's `rate_limit` configuration. The supplied
/// limiter is already prepared and owns its KeyProvider and store lifetime.
#[cfg(feature = "rate-limit")]
pub fn register_quota<K: claviron_http::rate_limit::KeyProvider + 'static>(
    state: &AppState,
    host: Host,
    prefix: claviron_http::ext::PathPrefix,
    limiter: claviron_http::rate_limit::RateLimiter<K>,
) -> RegistryResult<()> {
    let hosts = state
        .registry_ref()
        .resolve::<HostAppRegistry>()
        .ok_or_else(|| RegistryError::msg("HostAppRegistry not registered"))?;
    hosts.register_quota(host, prefix, limiter)
}

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
    registry.register(Arc::new(log::AccessLogProvider))?;
    #[cfg(feature = "db")]
    {
        // Registered everywhere; `Scoped` keeps it running in workers.
        use crate::listener::VhostAliasListener;
        registry.register(Arc::new(VhostAliasListener::default()))?;
    }

    // Only serving processes compile application plans; the master keeps
    // host declarations and lifecycle coordination.
    if state.is_master() {
        #[cfg(feature = "orbit")]
        state.events().listen::<claviron_runtime::ShutdownInitiated, _>(|state, _| async move {
            use std::collections::BTreeSet;

            use claviron_orbit::transport::TransportSegment;

            let Some(vhosts) = state.registry_ref().resolve::<VhostResolver>() else {
                return Ok(());
            };
            let fleets: BTreeSet<_> = vhosts.snapshot().canonical_entries()
                .flat_map(|(_, record)| record.config.link_routes().map(|(_, route)| route.fleet_name))
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
pub use pipeline::Routable;
pub use pipeline::apps::other::{OtherHandler, OtherService};
pub use pipeline::{
    AnyAuthenticated, AuthContext, H2ConnectAccept, H2ConnectBinding,
    H2ConnectHandler, H2ConnectRegistry, H2ConnectReject, H2ConnectResult,
    HostAppRegistry, HostBindingProvider, InterceptsNonServing,
    NonServingContext, RouteBinding, RouteProtocol, RustRouter,
};
#[cfg(feature = "websocket")]
pub use pipeline::{
    H1UpgradeAccept, H1UpgradeAuthenticated, H1UpgradeBinding,
    H1UpgradeContext, H1UpgradeHandler, H1UpgradeRegistry, H1UpgradeReject,
    H1UpgradeResult, H1UpgradeRoute, H1UpgradeState, is_h1_header_upgrade,
    websocket_accept_key, websocket_response_headers,
};
#[cfg(feature = "webtransport")]
pub use pipeline::{
    H3WebTransportBiStream, H3WebTransportSession, WebTransportAccept,
    WebTransportBinding, WebTransportHandler, WebTransportRegistry,
    WebTransportReject, WebTransportResult, webtransport_session_ended,
};

#[cfg(feature = "quic")]
pub mod quic;
#[cfg(feature = "tcp")]
pub mod tcp;
