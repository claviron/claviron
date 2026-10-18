#[cfg(feature = "orbit")]
mod orbit;
pub mod prepare;

#[cfg(feature = "upstream")]
mod upstream;

pub mod extensions;

pub mod routing;
use std::sync::Arc;

use axum::Router;
#[cfg(feature = "websocket")]
use axum::middleware;

use claviron_http::routing::AxumPipeline;
use claviron_runtime::AppState;
use claviron_types::Host;

use crate::config::HostConfig;

#[cfg(feature = "websocket")]
type H1UpgradeRegistryRef = Option<Arc<crate::pipeline::H1UpgradeRegistry>>;

/// Compile route owners once, then wrap them with the host-wide intercept stack.
pub fn create_router(
    state: AppState,
    host_key: &Host,
    host_cfg: Arc<HostConfig>,
    app_routers: Vec<Router<AppState>>,
    #[cfg(feature = "websocket")] h1_upgrade_registry: H1UpgradeRegistryRef,
) -> claviron_runtime::registry::Result<Router> {
    let fallback =
        host_cfg.fallback().cloned().unwrap_or_else(|| host_cfg.clone());
    let router = prepare::prepare(state.clone(), host_key, fallback.clone());
    let registry =
        state.registry_ref().resolve::<super::apps::HostAppRegistry>();
    let root = claviron_http::ext::PathPrefix::root();
    let host_layers = match &registry {
        Some(registry) => {
            registry.resolve_host_layers(&routing::RouteContext {
                state: &state,
                host: host_key,
                config: host_cfg.as_ref(),
                route: fallback.as_ref(),
                prefix: &root,
            })?
        }
        None => Vec::new(),
    };
    let selected_policies = !host_layers.is_empty();
    let host_downstream = AxumPipeline::new(router.clone());
    let wrap_default = |router| {
        host_layers.iter().fold(router, |router, layer| layer.apply(router))
    };
    // Native H1 admission belongs inside the selected route's policy layers,
    // not outside them. H2 CONNECT/WebTransport retain their own admission.
    #[cfg(feature = "websocket")]
    let h1_upgrade_registry = h1_upgrade_registry.or_else(|| {
        state.registry_ref().resolve::<crate::pipeline::H1UpgradeRegistry>()
    });
    #[cfg(feature = "websocket")]
    let upgrade_state = h1_upgrade_registry
        .filter(|registry| {
            host_cfg.transport.h1.upgrade && registry.has_host(host_key)
        })
        .map(|registry| claviron_websocket::H1UpgradeMiddlewareState {
            app_state: state.clone(),
            registry,
            host: host_key.clone(),
        });
    let app_router = app_routers
        .into_iter()
        .fold(Router::new(), |router, contribution| router.merge(contribution));
    // Every intercept receives the selected owner's delegation handle. Neither
    // an entry condition nor delegation needs a second prefix selection.
    let merge = |downstream: Router| {
        let delegation = AxumPipeline::new(downstream.clone());
        let router = app_router
            .clone()
            .layer(axum::Extension(AxumPipeline::new(downstream.clone())))
            .with_state(state.clone())
            .merge(downstream);
        #[cfg(feature = "websocket")]
        let router = match (&upgrade_state, selected_policies) {
            (Some(upgrade_state), true) => {
                router.layer(middleware::from_fn_with_state(
                    upgrade_state.clone(),
                    claviron_websocket::h1_upgrade_middleware,
                ))
            }
            _ => router,
        };
        match (&registry, selected_policies) {
            (Some(registry), true) => registry.layer_routable_routes(
                &state,
                host_key,
                host_cfg.clone(),
                delegation,
                router,
            ),
            _ => router,
        }
    };
    let default = wrap_default(merge(router.clone()));
    let router = if host_cfg.entry_rules().is_empty() {
        default
    } else {
        let mut entries = Vec::new();
        let mut direct = None;
        for rule in host_cfg.entry_rules() {
            let mut conditions = Vec::new();
            let downstream = if let Some(config) =
                host_cfg.routes.find(rule.as_str())
            {
                let context = routing::RouteContext {
                    state: &state,
                    host: host_key,
                    config: host_cfg.as_ref(),
                    route: config.as_ref(),
                    prefix: rule,
                };
                let (handler, resolved_conditions, layers) = match &registry {
                    Some(registry) => {
                        registry.resolve_route_extensions(&context)?
                    }
                    None if config.extensions.is_empty() => {
                        (None, Vec::new(), Vec::new())
                    }
                    None => {
                        return Err(claviron_runtime::registry::Error::msg(
                            "route extensions require HostAppRegistry",
                        ));
                    }
                };
                conditions = resolved_conditions;
                let downstream = match handler {
                    Some(handler) => handler.with_state(state.clone()),
                    None => prepare::prepare(
                        state.clone(),
                        host_key,
                        config.clone(),
                    ),
                };
                layers.into_iter().fold(merge(downstream), |router, layer| {
                    layer.apply(router)
                })
            } else {
                default.clone()
            };
            let unconditional = conditions.is_empty();
            // A sole unconditional root owns every path. Compile it directly;
            // prefix selection and a dispatch wrapper would add no decision.
            if host_cfg.entry_rules().len() == 1
                && rule.as_str() == "/"
                && unconditional
            {
                direct = Some(downstream);
                continue;
            }
            entries.push((
                rule.clone(),
                routing::ConfiguredRoute {
                    pipeline: AxumPipeline::new(downstream),
                    conditions,
                },
            ));
        }
        match direct {
            Some(router) => router,
            None => {
                let dispatch = routing::RouteDispatch {
                    entries: Arc::new(routing::RouteTable::new(entries)),
                    fallback: AxumPipeline::new(default),
                    rejected: AxumPipeline::new(wrap_default(
                        match (&registry, selected_policies) {
                            (Some(registry), true) => registry
                                .layer_routable_routes(
                                    &state,
                                    host_key,
                                    host_cfg.clone(),
                                    AxumPipeline::new(router.clone()),
                                    router,
                                ),
                            _ => router,
                        },
                    )),
                };
                Router::new()
                    .fallback(routing::dispatch::serve)
                    .with_state(dispatch)
            }
        }
    };

    // Unconfigured hosts retain their original once-per-host intercept stack.
    // Opt-in inherited policies instead surround the selected route, including
    // native H1 admission and direct delegation by a Rust intercept.
    #[cfg(feature = "websocket")]
    let router = match (&upgrade_state, selected_policies) {
        (Some(upgrade_state), false) => {
            router.layer(middleware::from_fn_with_state(
                upgrade_state.clone(),
                claviron_websocket::h1_upgrade_middleware,
            ))
        }
        _ => router,
    };
    let router = match (&registry, selected_policies) {
        (Some(registry), false) => registry.layer_routable_routes(
            &state,
            host_key,
            host_cfg.clone(),
            host_downstream,
            router,
        ),
        _ => router,
    };

    // Admission — how many of this host's requests run at once, and how long
    // one may take to its head — is not a router layer: it applies to every
    // path to the host, bypass and relays too, so `HostService` applies it
    // (`pipeline::apps::Admission`). Path-deny is not router middleware either:
    // HostService applies it before app dispatch.

    use tower_http::trace::TraceLayer;
    let router = router.layer(TraceLayer::new_for_http());

    let _ = host_key;
    Ok(router)
}

/// Builds the standard config-driven Axum router for a host.
///
/// This is the default path for hosts that are declared in config but do
/// not have a userspace router registered through `register_app`.
/// It delegates to [`create_router`] with an empty `Router`, producing the
/// same Claviron host pipeline without any merged Rust application routes.
pub fn create_default_router(
    state: AppState,
    host_key: &Host,
    host_cfg: Arc<HostConfig>,
    #[cfg(feature = "websocket")] h1_upgrade_registry: H1UpgradeRegistryRef,
) -> claviron_runtime::registry::Result<Router> {
    create_router(
        state,
        host_key,
        host_cfg,
        Vec::new(),
        #[cfg(feature = "websocket")]
        h1_upgrade_registry,
    )
}

#[cfg(all(test, feature = "php_embed"))]
#[path = "../../tests/unit/merged.rs"]
mod tests;
