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
pub trait BackendHandler: DispatchStep {
    /// Declared once during preparation. There is no implicit identity export
    /// or response-lifetime policy for a newly registered backend.
    fn contract(&self) -> BackendContract;
}

/// Request metadata exported at the application boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendIdentity {
    /// Keep the request unchanged; native layers can borrow HostContext.
    None,
    /// Export the prepared policy, connection and selected hostname.
    Request,
}

/// Who completes a streaming response when execution is draining.
#[derive(Clone)]
pub enum BackendReply {
    /// The execution owner cooperatively completes its own output.
    Owner,
    /// End this edge's external SSE body on the supplied owner's shutdown.
    /// Finite responses retain their normal completion and drain.
    ExternalEventStream(tokio_util::sync::CancellationToken),
}

/// Mandatory cold declaration of the request and response handoff.
#[derive(Clone)]
pub struct BackendContract {
    pub identity: BackendIdentity,
    pub reply: BackendReply,
}

/// A selected request owner with its compiled scope metadata contract.
/// The first Backend in a plan that reads identity has it exported with the
/// effective policy; later Backend calls receive that same request metadata,
/// including on Unhandled. A plan whose owners read none exports none.
#[derive(Clone)]
pub struct Backend<B> {
    pub(crate) owner: B,
    pub(in crate::pipeline) identity: Option<IdentityPolicy>,
    reply: BackendReply,
}

impl<B: BackendHandler> Backend<B> {
    pub fn new(
        owner: B,
        policy: HttpPolicy,
    ) -> Self {
        let contract = owner.contract();
        let identity = (contract.identity == BackendIdentity::Request)
            .then(|| IdentityPolicy::new(policy));
        Self { owner, identity, reply: contract.reply }
    }
}

impl<B> Backend<B> {
    /// An owner with no declaration of its own, which always reads identity.
    #[cfg(feature = "axum")]
    pub(crate) fn exporting(
        owner: B,
        policy: HttpPolicy,
    ) -> Self {
        Self {
            owner,
            identity: Some(IdentityPolicy::new(policy)),
            reply: BackendReply::Owner,
        }
    }

    pub(crate) fn prepare_request(
        &self,
        mut request: Request<RouteBody>,
        context: Option<super::HostContext<'_>>,
    ) -> Request<RouteBody> {
        if let Some(identity) = &self.identity {
            identity.inject(request.extensions_mut(), context);
        }
        request
    }
}

impl<B: BackendHandler, S> HostLayer<S> for Backend<B> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.identity = self.identity.map(|identity| identity.policy);
        let action = Box::new(move |request| self.owner.call(request));
        layer.before.push(match self.reply {
            BackendReply::Owner => super::Action::Request(action),
            BackendReply::ExternalEventStream(shutdown) => {
                super::Action::ExternalRequest(action, shutdown)
            }
        });
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
impl BackendHandler for RustRouter {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: if RustRouter::reads_identity(self) {
                BackendIdentity::Request
            } else {
                BackendIdentity::None
            },
            reply: BackendReply::Owner,
        }
    }
}

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
impl BackendHandler for Arc<dyn HostHandler> {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::None,
            reply: BackendReply::Owner,
        }
    }
}

impl DispatchStep for Arc<dyn BackendHandler> {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        self.as_ref().call(request)
    }
}
impl BackendHandler for Arc<dyn BackendHandler> {
    fn contract(&self) -> BackendContract {
        self.as_ref().contract()
    }
}
use crate::pipeline::layers::prepare_fallback;

#[derive(Clone)]
pub(super) struct BackendPlan {
    pub steps: Vec<Arc<dyn BackendHandler>>,
    pub fallback: Option<Arc<dyn DispatchStep>>,
}

impl BackendPlan {
    pub(super) fn prepare(context: &RouteContext<'_>) -> Self {
        if context
            .setting(|scope| scope.redirect.as_ref())
            .is_some_and(|rules| !rules.is_empty())
        {
            return Self { steps: Vec::new(), fallback: None };
        }
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
