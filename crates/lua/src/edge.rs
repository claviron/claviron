//! Native Lua HTTP dispatch and optional socket/session bindings.
//!
//! Host preparation resolves configured owners and native session bindings.
//! Socket ownership and admission remain with the native transport registries;
//! their precedence does not follow the ordinary HTTP backend's position.

#[cfg(any(feature = "websocket", feature = "webtransport"))]
mod origin;
#[cfg(feature = "websocket")]
mod websocket;
#[cfg(feature = "webtransport")]
mod webtransport;

use std::sync::Arc;

use crate::serve::{LuaApplicationAdapter, LuaApplications};
use claviron_http::config::routes::LuaRouteConfig;
use claviron_http::ext::PathPrefix;
use claviron_http::policy::HttpPolicyOverride;
use claviron_http::{RouteBody, ServiceResult};
use http::Request;

use claviron_edge::pipeline::{DispatchCall, DispatchStep};

struct Mount {
    path: Option<PathPrefix>,
    adapter: LuaApplicationAdapter,
}

/// One prepared Lua backend. An absent declaration installs no backend step.
/// The most specific mount claims the request, including unavailable/error
/// responses. A non-match yields the same unread request to the next owner.
#[derive(Clone)]
pub struct LuaBackend {
    mounts: Arc<[Mount]>,
}

impl LuaBackend {
    pub fn new(
        service: Option<Arc<LuaApplications>>,
        routes: &[LuaRouteConfig],
        prefix: Option<&PathPrefix>,
        policy: HttpPolicyOverride,
    ) -> Option<Self> {
        if routes.is_empty() {
            return None;
        }
        let mut mounts: Vec<_> = routes
            .iter()
            .map(|route| Mount {
                path: prefix.cloned(),
                adapter: LuaApplicationAdapter::new(
                    service.clone(),
                    route.application.as_str(),
                    policy,
                ),
            })
            .collect();
        // Preserve the former stable mount precedence: longer first, explicit
        // root before a catch-all, and later declarations first on equal keys.
        mounts.sort_by(|a, b| match (&a.path, &b.path) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
        });
        mounts.reverse();
        Some(Self { mounts: mounts.into() })
    }

    fn select(
        &self,
        path: &str,
    ) -> Option<&Mount> {
        self.mounts.iter().find(|mount| {
            mount
                .path
                .as_ref()
                .is_none_or(|prefix| prefix.has_path_segment(path))
        })
    }
}

impl claviron_edge::pipeline::BackendHandler for LuaBackend {
    fn contract(&self) -> claviron_edge::pipeline::BackendContract {
        claviron_edge::pipeline::BackendContract {
            identity: claviron_edge::pipeline::BackendIdentity::None,
            reply: claviron_edge::pipeline::BackendReply::Owner,
        }
    }
}

impl DispatchStep for LuaBackend {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<claviron_edge::pipeline::host::HostContext<'_>>,
    ) -> DispatchCall {
        let Some(mount) = self.select(request.uri().path()) else {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        };
        let adapter = mount.adapter.clone();
        // Copied while the context is lent: the call outlives it.
        let edge = context.and_then(|context| context.policy()).copied();
        DispatchCall::new(async move {
            ServiceResult::Handled(adapter.handle(request, edge).await)
        })
    }
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
fn mounted_websocket_path(
    mount: Option<&claviron_http::ext::PathPrefix>,
    path: &claviron_http::ext::PathPrefix,
) -> Arc<str> {
    match mount.map(|mount| mount.as_str()) {
        None | Some("/") => Arc::from(path.as_str()),
        Some(mount) if path.as_str() == "/" => Arc::from(mount),
        Some(mount) => Arc::from(format!("{mount}{}", path.as_str())),
    }
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
fn configured_route<'a>(
    host: &'a claviron_edge::HostConfig,
    path: &str,
    application: &str,
) -> Option<&'a LuaRouteConfig> {
    let selected = host.routes.find(path).map(|route| &**route).unwrap_or(host);
    selected.lua().iter().find(|route| {
        route.application == application
            && route.websockets().iter().any(|socket| {
                mounted_websocket_path(
                    selected
                        .files
                        .mapping
                        .as_ref()
                        .map(|mapping| mapping.prefix()),
                    &socket.path,
                )
                .as_ref()
                    == path
            })
    })
}

#[cfg(all(test, feature = "websocket"))]
#[path = "../tests/edge/socket_path.rs"]
mod socket_path_tests;

#[cfg(test)]
#[path = "../tests/edge.rs"]
mod tests;

/// Register configured HTTP backends and optional native session contributions.
/// Native Lua application and Runner use do not select this Edge capability.
pub fn register(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<()> {
    let registry = state
        .registry_ref()
        .resolve::<claviron_edge::HostAppRegistry>()
        .ok_or_else(|| {
            claviron_runtime::registry::Error::msg(
                "Lua requires the Edge host registry",
            )
        })?;
    registry.register_backend_factory(Arc::new(Factory))?;
    #[cfg(any(feature = "websocket", feature = "webtransport"))]
    claviron_edge::register_routable(
        state,
        Arc::new(Sessions {
            #[cfg(feature = "websocket")]
            state: state.clone(),
        }),
    )?;
    Ok(())
}

struct Factory;
impl claviron_edge::pipeline::backend::BackendFactory for Factory {
    fn name(&self) -> &'static str {
        "lua"
    }
    fn placement(&self) -> claviron_edge::pipeline::backend::BackendPlacement {
        claviron_edge::pipeline::backend::BackendPlacement::AfterFiles
    }
    fn prepare(
        &self,
        context: &claviron_edge::pipeline::backend::BackendContext<'_>,
    ) -> claviron_runtime::registry::Result<
        Vec<Arc<dyn claviron_edge::pipeline::BackendHandler>>,
    > {
        let policy = claviron_edge::extension::RouteContext {
            state: context.state,
            host: context.host,
            config: context.host_config,
            route: context.route,
            prefix: context.prefix,
        }
        .policy();
        Ok(LuaBackend::new(
            context.state.registry_ref().resolve::<LuaApplications>(),
            context.route.lua(),
            Some(context.prefix),
            policy,
        )
        .into_iter()
        .map(|owner| {
            Arc::new(owner) as Arc<dyn claviron_edge::pipeline::BackendHandler>
        })
        .collect())
    }
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
struct Sessions {
    #[cfg(feature = "websocket")]
    state: claviron_runtime::AppState,
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
impl claviron_runtime::registry::Component<claviron_runtime::AppState>
    for Sessions
{
    type Deps = ();
    fn name(&self) -> &str {
        "LuaEdgeSessions"
    }
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
impl claviron_edge::Routable for Sessions {
    fn match_host(
        &self,
        _: &claviron_types::Host,
        host: &claviron_edge::HostConfig,
    ) -> bool {
        lua_routes(host).any(|(_, route)| !route.websockets().is_empty())
    }
    fn prepare_sessions(
        &self,
        state: &claviron_runtime::AppState,
        host: &claviron_types::Host,
        config: &claviron_edge::HostConfig,
    ) -> claviron_runtime::registry::Result<()> {
        #[cfg(feature = "webtransport")]
        webtransport::append_routes(state, host, config)?;
        #[cfg(not(feature = "webtransport"))]
        let _ = (state, host, config);
        Ok(())
    }
    #[cfg(feature = "websocket")]
    fn h1_upgrades(
        &self,
        host: &claviron_types::Host,
        config: Arc<claviron_edge::HostConfig>,
    ) -> claviron_runtime::registry::Result<
        Vec<claviron_edge::pipeline::H1UpgradeRoute>,
    > {
        let mut routes = Vec::new();
        websocket::append_routes(&self.state, host, &config, &mut routes)?;
        Ok(routes)
    }
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
fn lua_routes(
    host: &claviron_edge::HostConfig
) -> impl Iterator<Item = (Option<&PathPrefix>, &LuaRouteConfig)> {
    host.scopes().flat_map(|(prefix, scope)| {
        scope.lua().iter().map(move |route| (prefix, route))
    })
}
