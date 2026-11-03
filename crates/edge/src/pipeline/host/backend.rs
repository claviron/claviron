//! Prepared Backend ownership and cold application/backend selection.

use std::sync::Arc;

use crate::pipeline::extension::RouteContext;

use super::{DispatchCall, DispatchStep, HostLayerBuilder, HostRequest};
use crate::pipeline::{HostHandler, HostLayer, IdentityPolicy, RustRouter};
use claviron_http::{RouteBody, policy::HttpPolicy};
use http::Request;

/// An application, embedded runtime or remote execution owner.
/// Internal Static/TryFiles operations implement only `DispatchStep`.
/// Register this owner through `Backend`, so its metadata contract is prepared
/// with the host plan rather than rediscovered during request execution.
pub trait BackendHandler: DispatchStep {}

/// A selected request owner with its compiled scope metadata contract.
/// The first Backend in a plan exports identity and effective policy; later
/// Backend calls receive that same request metadata, including on Unhandled.
#[derive(Clone)]
pub struct Backend<B> {
    pub(crate) owner: B,
    pub(in crate::pipeline) identity: IdentityPolicy,
}

impl<B> Backend<B> {
    pub fn new(
        owner: B,
        policy: HttpPolicy,
    ) -> Self {
        Self { owner, identity: IdentityPolicy::new(policy) }
    }

    pub(crate) fn prepare_request(
        &self,
        mut request: Request<RouteBody>,
        context: Option<super::HostContext<'_>>,
    ) -> Request<RouteBody> {
        self.identity.inject(request.extensions_mut(), context);
        request
    }
}

impl<B: BackendHandler, S> HostLayer<S> for Backend<B> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.identity = Some(self.identity.policy);
        layer.request(move |request| self.owner.call(request));
        Ok(())
    }
}

impl DispatchStep for RustRouter {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        DispatchCall::response(self.handle(request))
    }
}
impl BackendHandler for RustRouter {}

impl DispatchStep for Arc<dyn HostHandler> {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        DispatchCall::application(HostHandler::call(
            self.as_ref(),
            HostRequest::new(request),
        ))
    }
}
impl BackendHandler for Arc<dyn HostHandler> {}

impl DispatchStep for Arc<dyn BackendHandler> {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        self.as_ref().call(request)
    }
}
impl BackendHandler for Arc<dyn BackendHandler> {}
use crate::pipeline::layers::prepare_fallback;

#[derive(Clone)]
pub(super) struct BackendPlan {
    pub steps: Vec<Arc<dyn BackendHandler>>,
    pub fallback: Option<Arc<dyn DispatchStep>>,
}

impl BackendPlan {
    pub(super) fn prepare(context: &RouteContext<'_>) -> Self {
        let state = context.state;
        let host = context.host;
        let config = context.route;
        #[cfg(any(feature = "orbit", feature = "lua"))]
        let policy = context.policy();
        let steps = std::iter::empty::<Arc<dyn BackendHandler>>();
        #[cfg(feature = "orbit")]
        let steps = steps.chain(
            crate::pipeline::link::LinkBackend::new(
                state,
                config.orbit(),
                Some(context.prefix),
                policy.requests,
            )
            .map(|owner| Arc::new(owner) as Arc<dyn BackendHandler>),
        );
        #[cfg(feature = "lua")]
        let steps = steps.chain(
            crate::pipeline::embed::lua::LuaBackend::new(
                state
                    .registry_ref()
                    .resolve::<claviron_lua::serve::LuaApplications>(),
                config.lua(),
                Some(context.prefix),
                policy,
            )
            .map(|owner| Arc::new(owner) as Arc<dyn BackendHandler>),
        );
        #[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
        let steps = steps.chain(
            crate::pipeline::embed::php::PhpBackend::new(state, config)
                .map(|owner| Arc::new(owner) as Arc<dyn BackendHandler>),
        );
        #[cfg(feature = "upstream")]
        let steps = steps.chain(
            crate::pipeline::upstream::UpstreamStage::prepare(
                state,
                host,
                config.upstream.as_ref(),
                claviron_upstream::proxy::service::UpstreamRequestPolicy {
                    allow_h1_upgrade: config.transport.h1.upgrade,
                    allow_connect_protocol: config
                        .transport
                        .h2
                        .connect_protocol,
                },
            )
            .map(|owner| Arc::new(owner) as Arc<dyn BackendHandler>),
        );
        let _ = host;
        Self {
            steps: steps.collect(),
            fallback: prepare_fallback(state, config),
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/backend.rs"]
mod tests;
