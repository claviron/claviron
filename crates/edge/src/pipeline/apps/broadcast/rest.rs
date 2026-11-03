//! Claim protocol-owned REST paths without wrapping the native downstream.

use std::collections::HashSet;
use std::sync::Arc;

use claviron_http::{RouteBody, ServiceResult};
use claviron_pusher::{PusherAdapter, PusherTarget};
use claviron_runtime::AppState;
use http::Request;

use crate::HostConfig;
use crate::pipeline::host::ApplicationHandler;
use crate::pipeline::host::{DispatchCall, DispatchStep};

enum Endpoints {
    Publish(HashSet<String>),
    Http,
}

impl Endpoints {
    fn accepts(
        &self,
        path: &str,
    ) -> bool {
        match self {
            Self::Publish(paths) => paths.contains(path),
            Self::Http => {
                let Some(path) = path.strip_prefix('/') else {
                    return false;
                };
                let mut segments = path.split('/');
                match (segments.next(), segments.next(), segments.next()) {
                    (Some("app"), Some(key), None) => !key.is_empty(),
                    (Some("apps"), Some(id), rest) if !id.is_empty() => rest
                        .is_none_or(|first| {
                            !first.is_empty() || segments.next().is_some()
                        }),
                    _ => false,
                }
            }
        }
    }
}

enum RestOwner {
    Fleet(axum::Router),
    Http(PusherAdapter),
}

impl RestOwner {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        match self {
            Self::Fleet(owner) => {
                let owner = owner.clone();
                DispatchCall::new(async move {
                    let response = owner.call(request).await;
                    ServiceResult::Handled(response)
                })
            }
            Self::Http(adapter) => {
                let adapter = adapter.clone();
                DispatchCall::new(async move {
                    let Ok(response) = adapter.handle(request).await;
                    ServiceResult::Handled(response)
                })
            }
        }
    }
}

struct Rest {
    endpoints: Endpoints,
    owner: RestOwner,
}

impl crate::pipeline::host::BackendHandler for Rest {}

impl DispatchStep for Rest {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        if !self.endpoints.accepts(request.uri().path()) {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        }
        self.owner.call(request)
    }
}

pub(crate) fn prepare(
    state: &AppState,
    host: &HostConfig,
) -> Option<Arc<dyn crate::pipeline::host::BackendHandler>> {
    let config = host.broadcast_cfg()?;
    let (endpoints, owner) = match &config.target {
        PusherTarget::Fleet => {
            let service = state
                .registry_ref()
                .resolve::<claviron_broadcast::BroadcastService>()?;
            let paths = service
                .applications()
                .applications()
                .values()
                .map(|application| format!("/apps/{}/events", application.id))
                .collect::<HashSet<_>>();
            if paths.is_empty() {
                return None;
            }
            (
                Endpoints::Publish(paths),
                RestOwner::Fleet(claviron_broadcast::pusher_routes(service)),
            )
        }
        PusherTarget::Http(http) => (
            Endpoints::Http,
            RestOwner::Http(PusherAdapter::new(Arc::new(http.clone()))),
        ),
    };
    Some(Arc::new(Rest { endpoints, owner }))
}

#[cfg(test)]
#[path = "../../../../tests/pipeline/broadcast/rest.rs"]
mod tests;
