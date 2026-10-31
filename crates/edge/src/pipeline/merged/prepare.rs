//! Prepare configured owners before host interception is composed.
//!
//! Host compatibility is confined to this cold boundary. File, upstream and
//! language adapters consume typed declarations, not HostConfig.

use crate::config::HostConfig;
use axum::Router;
use claviron_runtime::AppState;
use claviron_types::Host;
use std::sync::Arc;

/// Compile the configured fallback. Rust interception and host-wide policy
/// are composed by the caller, not by a language adapter.
pub(crate) fn prepare(
    state: AppState,
    _host_key: &Host,
    host_cfg: Arc<HostConfig>,
) -> Router {
    // vhost/file/php pipeline. Fallback MUST be set before any layer is
    // stacked on top — axum 0.8 only wraps a fallback with `.layer()`
    // when the fallback was already present at the time the layer is added.
    //
    // The caller composes Routable → Static → TryFiles ahead of this owner.
    // Only directory-index/SPA fallback remains behind script/backend owners.
    //
    use axum::Extension;
    let files = Arc::new(crate::pipeline::host::fallback_config(&host_cfg));
    let router =
        claviron_files::routing::build_router(state.clone(), files.clone())
            .layer(Extension(host_cfg.clone()));

    // Native steps retain the former backend precedence. The host compiler
    // supplies one generic application bridge, never a backend-specific Axum
    // binding, body conversion, middleware or request extension.
    let steps = std::iter::empty::<Arc<dyn crate::pipeline::host::DispatchStep>>();
    #[cfg(feature = "orbit")]
    let steps = steps.chain(crate::pipeline::link::LinkBackend::new(&state, host_cfg.orbit())
        .map(|backend| Arc::new(backend) as Arc<dyn crate::pipeline::host::DispatchStep>));
    #[cfg(feature = "lua")]
    let steps = steps.chain(crate::pipeline::embed::lua::LuaBackend::new(
        state.registry_ref().resolve::<claviron_lua::serve::LuaApplications>(),
        host_cfg.lua(),
    ).map(|backend| Arc::new(backend) as Arc<dyn crate::pipeline::host::DispatchStep>));
    #[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
    let steps = steps.chain(crate::pipeline::embed::php::PhpBackend::new(
        &state, host_cfg.build_runtimes(&state),
    ).map(|backend| Arc::new(backend) as Arc<dyn crate::pipeline::host::DispatchStep>));
    #[cfg(feature = "upstream")]
    let steps = steps.chain(crate::pipeline::upstream::UpstreamStage::prepare(
        &state, _host_key, host_cfg.upstream.as_ref(),
        claviron_upstream::proxy::service::UpstreamRequestPolicy {
            allow_h1_upgrade: host_cfg.transport.h1.upgrade,
            allow_connect_protocol: host_cfg.transport.h2.connect_protocol,
        },
    ).map(|backend| Arc::new(backend) as Arc<dyn crate::pipeline::host::DispatchStep>));
    let router = crate::pipeline::host::compose_dispatch_router(router, steps.collect(), &[]);

    // The fleet target publishes directly into the worker-local Broadcast
    // service. WebSocket upgrade routes are compiled separately, but REST
    // publishing remains an ordinary host route and shares the host policy.
    #[cfg(feature = "broadcast")]
    let router = {
        let local = host_cfg.broadcast_cfg().is_some_and(|config| {
            matches!(config.target, claviron_pusher::PusherTarget::Fleet)
        });
        if local {
            if let Some(service) = state
                .registry_ref()
                .resolve::<claviron_broadcast::BroadcastService>(
            ) {
                router.merge(claviron_broadcast::pusher_routes(service))
            } else {
                router
            }
        } else {
            router
        }
    };

    // External Pusher routes stay above the upstream catch-all; CORS is
    // applied below so both Pusher and upstream responses share host policy.
    #[cfg(feature = "broadcast")]
    let router = {
        use claviron_pusher::pusher_routes;
        if let Some(pusher_cfg) = host_cfg.broadcast_cfg() {
            if matches!(
                pusher_cfg.target,
                claviron_pusher::PusherTarget::Http(_)
            ) {
                router.merge(pusher_routes(pusher_cfg))
            } else {
                router
            }
        } else {
            router
        }
    };

    router
}
