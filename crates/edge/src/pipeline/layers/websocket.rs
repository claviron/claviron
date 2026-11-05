//! Exact WebSocket endpoints prepared before application dispatch.

use std::collections::HashMap;
use std::sync::Arc;

use claviron_http::upgrade::{
    H1UpgradeBinding, H1UpgradeRegistry, try_h1_upgrade,
};
use claviron_http::{RouteBody, ServiceResult};
use claviron_runtime::AppState;
use claviron_types::Host;
use http::Request;

use super::{DispatchCall, DispatchStep, HostLayer, HostLayerBuilder};
use crate::HostConfig;

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
    pub fn new(
        state: AppState,
        host: Host,
        registry: &H1UpgradeRegistry,
    ) -> Option<Self> {
        let bindings = registry.snapshot_for_host(&host);
        (!bindings.is_empty()).then_some(Self { state, host, bindings })
    }

    pub(crate) fn prepare(
        state: &AppState,
        host: &Host,
        config: &HostConfig,
        listener: Option<&claviron_http::config::ListenerConfig>,
        local: Option<Arc<H1UpgradeRegistry>>,
    ) -> Option<Arc<Self>> {
        if !config
            .transport
            .allows_protocol(claviron_http::listener::HttpProtocol::H1)
            || !config.transport.h1.upgrade
            || listener.is_some_and(|listener| {
                !matches!(&listener.settings,
                    claviron_http::config::ListenerSettings::Tcp(tcp)
                    if tcp.protocols.contains(&claviron_http::listener::HttpProtocol::H1))
            })
        {
            return None;
        }
        let registry = local
            .or_else(|| state.registry_ref().resolve::<H1UpgradeRegistry>())?;
        Self::new(state.clone(), host.clone(), &registry).map(Arc::new)
    }
}

impl DispatchStep for WebSocketStage {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<crate::pipeline::host::HostContext<'_>>,
    ) -> DispatchCall {
        let Some(binding) = self.bindings.get(request.uri().path()) else {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        };
        let H1UpgradeBinding::Handler(handler) = binding;
        if !handler.is_upgrade(&request) {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        }
        let (state, host, binding) =
            (self.state.clone(), self.host.clone(), binding.clone());
        DispatchCall::new(try_h1_upgrade(state, host, binding, request))
    }
}

impl DispatchStep for Arc<WebSocketStage> {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<crate::pipeline::host::HostContext<'_>>,
    ) -> DispatchCall {
        self.as_ref().call(request, context)
    }
}

impl crate::pipeline::host::BackendHandler for Arc<WebSocketStage> {
    fn contract(&self) -> crate::pipeline::BackendContract {
        crate::pipeline::BackendContract {
            identity: crate::pipeline::BackendIdentity::Request,
            reply: crate::pipeline::BackendReply::Owner,
        }
    }
}

impl<S> HostLayer<S> for Arc<WebSocketStage> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        crate::pipeline::host::Backend::new(
            self,
            claviron_http::policy::HttpPolicy::unlimited(),
        )
        .prepare(layer)
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/layers/websocket.rs"]
mod tests;
