//! Native Lua HTTP dispatch and optional socket/session bindings.
//!
//! Host preparation calls the registration functions once per publication.
//! Socket ownership and admission remain with the native transport registries;
//! their precedence does not follow the ordinary HTTP backend's position.

#[cfg(any(feature = "websocket", feature = "webtransport"))]
mod origin;
#[cfg(feature = "websocket")]
mod websocket;
#[cfg(feature = "webtransport")]
mod webtransport;

#[cfg(feature = "websocket")]
pub(crate) use websocket::append_routes as append_websocket_routes;
#[cfg(feature = "webtransport")]
pub(crate) use webtransport::append_routes as append_webtransport_routes;

use std::sync::Arc;

use claviron_http::config::routes::LuaRouteConfig;
use claviron_http::ext::PathPrefix;
use claviron_http::policy::HttpPolicyOverride;
use claviron_http::{RouteBody, ServiceResult};
use claviron_lua::serve::{LuaApplicationAdapter, LuaApplications};
use http::Request;

use crate::pipeline::host::{DispatchFuture, DispatchStep};

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
    ) -> Option<Self> {
        if routes.is_empty() {
            return None;
        }
        let mut mounts: Vec<_> = routes
            .iter()
            .map(|route| Mount {
                path: route.path.clone(),
                adapter: LuaApplicationAdapter::new(
                    service.clone(),
                    route.application.as_str(),
                    HttpPolicyOverride {
                        requests: route.requests,
                        responses: route.responses,
                    },
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

impl DispatchStep for LuaBackend {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchFuture {
        let Some(mount) = self.select(request.uri().path()) else {
            return DispatchFuture::ready(ServiceResult::Unhandled(request));
        };
        let adapter = mount.adapter.clone();
        DispatchFuture::new(async move {
            ServiceResult::Handled(adapter.handle(request).await)
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
    host: &'a crate::HostConfig,
    path: &str,
    application: &str,
) -> Option<&'a LuaRouteConfig> {
    let selected = host.routes.find(path).map(|route| &**route).unwrap_or(host);
    selected.lua().iter().find(|route| {
        route.application == application
            && route.websockets().iter().any(|socket| {
                mounted_websocket_path(route.path.as_ref(), &socket.path)
                    .as_ref() == path
            })
    })
}

#[cfg(all(test, feature = "websocket"))]
#[path = "../../../tests/pipeline/embed/lua/socket_path.rs"]
mod socket_path_tests;

#[cfg(test)]
#[path = "../../../tests/pipeline/embed/lua.rs"]
mod tests;
