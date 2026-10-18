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
    // Layer order (request flow = outermost → innermost):
    //   1. Explicit static mounts (when configured)
    //   2. PHP gateway
    //   3. General file handler / fallback (404)
    //
    use axum::Extension;
    let mut files = host_cfg.files.clone();
    files.root = host_cfg.root();
    // Explicit route normalization permits one owner. Only that shape has
    // already selected its static mount; legacy flat/runner mounts still scan.
    let selected_mount = if host_cfg.selected_static {
        files
            .statics
            .as_ref()
            .filter(|mounts| mounts.len() == 1)
            .map(|mounts| mounts[0].clone())
    } else {
        None
    };
    if selected_mount.is_some() {
        // A mount miss continues to index/try_files/SPA, not a second probe of
        // the same mount in the general fallback.
        files.statics = None;
    }
    let files = Arc::new(files);
    let router =
        claviron_files::routing::build_router(state.clone(), files.clone())
            .layer(Extension(host_cfg.clone()));

    #[cfg(feature = "upstream")]
    let router = super::upstream::prepare(
        router,
        &state,
        _host_key,
        host_cfg.upstream.as_ref(),
        files.clone(),
        claviron_upstream::proxy::service::UpstreamRequestPolicy {
            allow_h1_upgrade: host_cfg.transport.h1.upgrade,
            allow_connect_protocol: host_cfg.transport.h2.connect_protocol,
        },
    );

    #[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
    let router = claviron_php::routing::build_router(
        router,
        &state,
        host_cfg.build_runtimes(&state),
        |router, state, runtime| match runtime.php_runner {
            #[cfg(feature = "php_fcgi")]
            claviron_php::PhpRunner::Fcgi => {
                claviron_php_fcgi::build_fcgi_router(router, state, runtime)
            }
            #[cfg(feature = "php_embed")]
            claviron_php::PhpRunner::Script => {
                claviron_php_embed::build_script_router(router, state, runtime)
            }
            #[cfg(feature = "php_embed")]
            claviron_php::PhpRunner::Application => {
                claviron_php_embed::build_application_router(
                    router, state, runtime,
                )
            }
        },
    );

    #[cfg(feature = "lua")]
    let router =
        claviron_lua::routing::build_router(router, &state, host_cfg.lua());

    #[cfg(feature = "orbit")]
    let router = super::orbit::prepare(router, &state, host_cfg.orbit());

    // Explicit static mounts retain ownership even when an application claims
    // `/`. Do not promote general try_files/SPA/index probing ahead of PHP.
    let router = match selected_mount {
        Some(mount) => claviron_files::routing::mount_selected(
            router,
            files.root.clone(),
            mount,
        ),
        None => claviron_files::routing::mount(router, files),
    };

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
