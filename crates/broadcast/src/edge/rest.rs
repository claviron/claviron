//! Claim protocol-owned REST paths without wrapping the native downstream.

use std::collections::HashSet;
use std::sync::Arc;

use claviron_http::{RouteBody, ServiceResult};
use claviron_pusher::{PusherAdapter, PusherTarget};
use claviron_runtime::AppState;
use http::Request;

use claviron_edge::HostConfig;
use claviron_edge::pipeline::{DispatchCall, DispatchStep};

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
            Self::Http => PusherAdapter::accepts_path(path),
        }
    }
}

enum RestOwner {
    Fleet(claviron_http::RustRouter),
    Http(PusherAdapter),
}

impl RestOwner {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        match self {
            Self::Fleet(owner) => DispatchCall::response(owner.handle(request)),
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

impl claviron_edge::pipeline::host::BackendHandler for Rest {
    fn contract(&self) -> claviron_edge::pipeline::BackendContract {
        claviron_edge::pipeline::BackendContract {
            identity: claviron_edge::pipeline::BackendIdentity::Request,
            reply: claviron_edge::pipeline::BackendReply::Owner,
        }
    }
}

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
) -> Option<Arc<dyn claviron_edge::pipeline::host::BackendHandler>> {
    let config = host.broadcast_cfg()?;
    let (endpoints, owner) = match &config.target {
        PusherTarget::Fleet => {
            let service =
                state.registry_ref().resolve::<crate::BroadcastService>()?;
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
                RestOwner::Fleet(crate::pusher_routes(service).into()),
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
#[path = "../../tests/edge/rest.rs"]
mod tests;
