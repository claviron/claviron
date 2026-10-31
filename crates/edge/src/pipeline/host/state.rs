//! Typed state and ownership handoff for native host admission.

use std::sync::Arc;

use claviron_http::ServiceResult;
use claviron_http::authority::RequestAuthority;
use claviron_http::ext::ConnCtx;

use super::*;
use crate::deny::{DenyCheckedRequest, DenyService};
use crate::service::{HostIngress, RequestArrival, RequestRejection, VhostCtx};

/// Independent phase state for one native request, never a shared extension map.
pub struct HostRequestState {
    ingress: IngressState,
    identity: IdentityState,
}

impl HostRequestState {
    pub fn new(arrival: RequestArrival) -> Self {
        Self {
            ingress: IngressState::new(arrival),
            identity: IdentityState::default(),
        }
    }

    pub fn identity(&self) -> &IdentityState {
        &self.identity
    }
}

impl AsMut<IngressState> for HostRequestState {
    fn as_mut(&mut self) -> &mut IngressState {
        &mut self.ingress
    }
}

impl AsMut<IdentityState> for HostRequestState {
    fn as_mut(&mut self) -> &mut IdentityState {
        &mut self.identity
    }
}

pub(crate) struct IdentifiedRequest<B> {
    pub(crate) request: Request<B>,
    pub(crate) connection: Arc<ConnCtx>,
}

impl HostPipeline<HostRequestState, RequestRejection> {
    pub(crate) fn for_host(
        connection: &Arc<ConnCtx>,
        host: &VhostCtx,
        ingress: &HostIngress,
    ) -> Self {
        let mut ingress_layer = HostLayerBuilder::default();
        IngressPolicy::new(connection, host, ingress.deny.clone())
            .register(&mut ingress_layer);
        let mut identity_layer = HostLayerBuilder::default();
        IdentityPolicy::new(connection.clone(), host, ingress.proxy.clone())
            .register(&mut identity_layer);
        HostPipelineBuilder { layers: vec![ingress_layer, identity_layer] }
            .build()
    }

    /// The native plan is head-only. Resume live policy and dispatch with the
    /// original body and the same identity already installed in its extensions.
    pub(crate) fn admit<B>(
        &self,
        request: Request<B>,
        state: HostRequestState,
    ) -> Result<IdentifiedRequest<B>, RequestRejection> {
        let prepared = match self.prepare_with_state(request, state) {
            ServiceResult::Handled(reply) => return Err(reply.response),
            ServiceResult::Unhandled(prepared) => prepared,
        };
        let (request, state) = prepared.into_request_and_state();
        let connection = state
            .identity
            .connection
            .ok_or(RequestRejection::MissingIdentity)?;
        Ok(IdentifiedRequest { request, connection })
    }

    pub(crate) fn admit_checked<B>(
        &self,
        request: DenyCheckedRequest<'_, B>,
        deny: &DenyService,
        arrival: RequestArrival,
        authority: Option<RequestAuthority>,
    ) -> Result<IdentifiedRequest<B>, RequestRejection> {
        let (request, ingress) =
            IngressState::checked(request, deny, arrival, authority);
        self.admit(
            request,
            HostRequestState { ingress, identity: IdentityState::default() },
        )
    }
}

impl<B, S> PreparedRequest<'_, B, S, RequestRejection> {
    /// Typed rejection plans cannot register response hooks. Transfer their
    /// unclaimed request and phase state without discarding response work.
    pub(crate) fn into_request_and_state(self) -> (Request<B>, S) {
        (Request::from_parts(self.head, self.body), self.state)
    }
}
