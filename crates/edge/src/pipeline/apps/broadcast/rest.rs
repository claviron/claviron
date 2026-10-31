//! Claim protocol-owned REST paths without wrapping the native downstream.

use std::sync::Arc;
use std::collections::HashSet;

use claviron_http::{RouteBody, ServiceResult};
use claviron_pusher::{PusherAdapter, PusherTarget};
use claviron_runtime::AppState;
use http::Request;

use crate::HostConfig;
use crate::pipeline::host::{DispatchFuture, DispatchStep};
use crate::pipeline::other::OtherService;

enum Endpoints {
    Publish(HashSet<String>),
    Http,
}

impl Endpoints {
    fn accepts(&self, path: &str) -> bool {
        match self {
            Self::Publish(paths) => paths.contains(path),
            Self::Http => {
                let Some(path) = path.strip_prefix('/') else { return false; };
                let mut segments = path.split('/');
                match (segments.next(), segments.next(), segments.next()) {
                    (Some("app"), Some(key), None) => !key.is_empty(),
                    (Some("apps"), Some(id), rest) if !id.is_empty() => {
                        rest.is_none_or(|first| !first.is_empty() || segments.next().is_some())
                    }
                    _ => false,
                }
            }
        }
    }
}

struct Rest {
    endpoints: Endpoints,
    owner: OtherService,
}

impl DispatchStep for Rest {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        if !self.endpoints.accepts(request.uri().path()) {
            return DispatchFuture::ready(ServiceResult::Unhandled(request));
        }
        let owner = self.owner.clone();
        DispatchFuture::new(async move {
            let response = match owner.handle(request).await {
                Ok(response) => response,
                Err(never) => match never {},
            };
            ServiceResult::Handled(response)
        })
    }
}

pub(crate) fn prepare(state: &AppState, host: &HostConfig) -> Option<Arc<dyn DispatchStep>> {
    let config = host.broadcast_cfg()?;
    let (endpoints, owner) = match &config.target {
        PusherTarget::Fleet => {
            let service = state.registry_ref().resolve::<claviron_broadcast::BroadcastService>()?;
            let paths = service.applications().applications().values()
                .map(|application| format!("/apps/{}/events", application.id))
                .collect::<HashSet<_>>();
            if paths.is_empty() { return None; }
            (Endpoints::Publish(paths),
                crate::pipeline::app::owner(claviron_broadcast::pusher_routes(service)))
        }
        PusherTarget::Http(http) => (
            Endpoints::Http,
            OtherService::new(PusherAdapter::new(Arc::new(http.clone()))),
        ),
    };
    Some(Arc::new(Rest { endpoints, owner }))
}

#[cfg(test)]
#[path = "../../../../tests/pipeline/broadcast/rest.rs"]
mod tests;
