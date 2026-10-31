//! Cold Lua mount selection and direct native application calls.

use std::sync::Arc;

use claviron_http::config::routes::LuaRouteConfig;
use claviron_http::ext::PathPrefix;
use claviron_http::policy::HttpPolicyOverride;
use claviron_http::{RouteBody, ServiceResult};
use http::Request;
use claviron_lua::serve::{LuaApplicationAdapter, LuaApplications};

use crate::pipeline::host::{DispatchStep, DispatchFuture};

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
        let mut mounts: Vec<_> = routes.iter().map(|route| Mount {
            path: route.path.clone(),
            adapter: LuaApplicationAdapter::new(
                service.clone(), route.application.as_str(),
                HttpPolicyOverride { requests: route.requests, responses: route.responses },
            ),
        }).collect();
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

    fn select(&self, path: &str) -> Option<&Mount> {
        self.mounts.iter().find(|mount| {
            mount.path.as_ref().is_none_or(|prefix| prefix.has_path_segment(path))
        })
    }
}

impl DispatchStep for LuaBackend {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let backend = self.clone();
        DispatchFuture::new(async move {
            let Some(mount) = backend.select(request.uri().path()) else {
                return ServiceResult::Unhandled(request);
            };
            ServiceResult::Handled(mount.adapter.handle(request).await)
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/embed/lua.rs"]
mod tests;
