//! Exact WebSocket endpoints prepared before application dispatch.

use std::collections::HashMap;
use std::sync::Arc;

use claviron_http::upgrade::{H1UpgradeBinding, H1UpgradeRegistry, try_h1_upgrade};
use claviron_http::{RouteBody, ServiceResult};
use claviron_runtime::AppState;
use claviron_types::Host;
use http::Request;

use super::{DispatchChain, DispatchFuture, DispatchStep};
use crate::HostConfig;
use crate::pipeline::other::OtherService;

/// One host generation's exact HTTP/1 WebSocket endpoints.
///
/// Construct only when the host allows the protocol. Empty registries produce
/// no stage. Ordinary requests borrow the captured map and pass their original
/// body onward without allocation, body inspection or a registry lock.
pub struct WebSocketStage {
    state: AppState,
    host: Host,
    bindings: HashMap<Arc<str>, H1UpgradeBinding>,
}

impl WebSocketStage {
    pub fn new(state: AppState, host: Host, registry: &H1UpgradeRegistry) -> Option<Self> {
        let bindings = registry.snapshot_for_host(&host);
        (!bindings.is_empty()).then_some(Self { state, host, bindings })
    }

    pub(crate) fn prepare(
        state: &AppState,
        host: &Host,
        config: &HostConfig,
        local: Option<Arc<H1UpgradeRegistry>>,
    ) -> Option<Arc<Self>> {
        if !config.transport.allows_protocol(claviron_http::listener::HttpProtocol::H1)
            || !config.transport.h1.upgrade
        {
            return None;
        }
        let registry = local.or_else(|| state.registry_ref().resolve::<H1UpgradeRegistry>())?;
        Self::new(state.clone(), host.clone(), &registry).map(Arc::new)
    }
}

impl DispatchStep for WebSocketStage {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let Some(binding) = self.bindings.get(request.uri().path()) else {
            return DispatchFuture::ready(ServiceResult::Unhandled(request));
        };
        let H1UpgradeBinding::Handler(handler) = binding;
        if !handler.is_upgrade(&request) {
            return DispatchFuture::ready(ServiceResult::Unhandled(request));
        }
        let (state, host, binding) = (self.state.clone(), self.host.clone(), binding.clone());
        DispatchFuture::new(try_h1_upgrade(state, host, binding, request))
    }
}

pub(crate) fn compose_websocket_other(
    owner: OtherService,
    websocket: Option<Arc<WebSocketStage>>,
) -> OtherService {
    match websocket {
        Some(websocket) => OtherService::new(DispatchChain::new(owner, vec![websocket])),
        None => owner,
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/websocket.rs"]
mod tests;
