//! Prepared Backend ownership.

use super::HostLayerBuilder;
use crate::layers::{HostLayer, IdentityPolicy};
use crate::{BackendHandler, BackendIdentity, BackendReply};
use claviron_http::{RouteBody, policy::HttpPolicy};
use http::Request;

/// A selected request owner with its scope's effective policy and its
/// compiled metadata contract.
///
/// Every call lends the owner that policy through its `HostContext`, beside
/// the accepted connection and host name. Writing them into the request is a
/// separate, declared matter: the first Backend in a plan that reads identity
/// from the request has it exported, later Backend calls receive that same
/// request metadata, including on Unhandled, and a plan whose owners read
/// none exports none.
#[derive(Clone)]
pub struct Backend<B> {
    pub owner: B,
    policy: HttpPolicy,
    pub(crate) identity: Option<IdentityPolicy>,
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
        Self { owner, policy, identity, reply: contract.reply }
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
            policy,
            identity: Some(IdentityPolicy::new(policy)),
            reply: BackendReply::Owner,
        }
    }

    pub fn prepare_request(
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
        let (owner, policy) = (self.owner, self.policy);
        let action: super::RequestAction = Box::new(
            move |request, context: Option<super::HostContext<'_>>| {
                owner.call(
                    request,
                    context.map(|context| context.with_policy(&policy)),
                )
            },
        );
        layer.before.push(match self.reply {
            BackendReply::Owner => super::Action::Request(action),
            BackendReply::ExternalEventStream(shutdown) => {
                super::Action::ExternalRequest(action, shutdown, None)
            }
            BackendReply::Relay(shutdown) => super::Action::ExternalRequest(
                action,
                shutdown,
                Some(policy.responses),
            ),
        });
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/plan/backend.rs"]
mod tests;
