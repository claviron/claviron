//! Typed state and ownership handoff for native host admission.

use std::sync::Arc;

use claviron_http::ServiceResult;
use claviron_http::authority::RequestAuthority;
use claviron_http::ext::ConnCtx;

use super::*;
use crate::VhostResolver;
use crate::deny::{DenyCheckedRequest, DenyService};
use crate::pipeline::host::PolicySnapshot;
use crate::service::{
    HostIngress, HostResponse, RequestArrival, RequestRejection, VhostCtx,
};

/// Independent phase state for one native request, never a shared extension map.
pub struct HostRequestState {
    ingress: IngressState,
    identity: IdentityState,
    access: AccessState,
}

impl HostRequestState {
    pub fn new(arrival: RequestArrival) -> Self {
        Self {
            ingress: IngressState::new(arrival),
            identity: IdentityState::default(),
            access: AccessState::default(),
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

impl AsRef<IdentityState> for HostRequestState {
    fn as_ref(&self) -> &IdentityState {
        &self.identity
    }
}

impl AsMut<AccessState> for HostRequestState {
    fn as_mut(&mut self) -> &mut AccessState {
        &mut self.access
    }
}

pub(crate) struct IdentifiedRequest<B> {
    pub(crate) head: RequestParts,
    pub(crate) body: B,
    pub(crate) connection: Arc<ConnCtx>,
    pub(crate) access: PolicySnapshot,
}

impl HostPipeline<HostRequestState, HostResponse> {
    pub(crate) fn for_host(
        connection: &Arc<ConnCtx>,
        host: &VhostCtx,
        ingress: &HostIngress,
        access: Arc<VhostResolver>,
    ) -> Self {
        let mut ingress_layer = HostLayerBuilder::default();
        IngressPolicy::new(connection, host, ingress.deny.clone())
            .register(&mut ingress_layer);
        let mut identity_layer = HostLayerBuilder::default();
        IdentityPolicy::new(connection.clone(), host, ingress.proxy.clone())
            .register(&mut identity_layer);
        let mut access_layer = HostLayerBuilder::default();
        AccessPolicy::new(access, host.vhost_key.clone())
            .register(&mut access_layer);
        HostPipelineBuilder {
            layers: vec![ingress_layer, identity_layer, access_layer],
        }
        .build()
    }

    /// The native plan is head-only. Resume quota admission and dispatch with
    /// the original body, prepared identity and admitted access generation.
    pub(crate) fn admit<B>(
        &self,
        request: Request<B>,
        state: HostRequestState,
    ) -> Result<IdentifiedRequest<B>, HostResponse> {
        let prepared = match self.prepare_with_state(request, state) {
            ServiceResult::Handled(reply) => return Err(reply.response),
            ServiceResult::Unhandled(prepared) => prepared,
        };
        let (head, body, state) = prepared.into_parts_and_state();
        let connection = state
            .identity
            .connection
            .ok_or(RequestRejection::MissingIdentity)?;
        Ok(IdentifiedRequest {
            head,
            body,
            connection,
            access: state.access.snapshot,
        })
    }

    pub(crate) fn admit_checked<B>(
        &self,
        request: DenyCheckedRequest<'_, B>,
        deny: &DenyService,
        arrival: RequestArrival,
        authority: Option<RequestAuthority>,
    ) -> Result<IdentifiedRequest<B>, HostResponse> {
        let (request, ingress) =
            IngressState::checked(request, deny, arrival, authority);
        self.admit(
            request,
            HostRequestState {
                ingress,
                identity: IdentityState::default(),
                access: AccessState::default(),
            },
        )
    }
}

// Only these typed head decisions are eligible for the ownership handoff;
// RouteResponse plans may carry response hooks that must not be discarded.
pub(crate) trait HeadOnlyReply {}
impl HeadOnlyReply for RequestRejection {}
impl HeadOnlyReply for HostResponse {}

impl<B, S, R> PreparedRequest<'_, B, S, R> {
    /// Typed rejection plans cannot register response hooks. Transfer their
    /// unclaimed request and phase state without discarding response work.
    pub(crate) fn into_parts_and_state(self) -> (RequestParts, B, S)
    where
        R: HeadOnlyReply,
    {
        (self.head, self.body, self.state)
    }
}

#[cfg(feature = "rate-limit")]
impl<S, R> HostPipeline<S, R> {
    /// Continue a head-only plan without assembling, moving or erasing a body.
    /// Typed plans cannot register response hooks, so none are discarded here.
    pub(crate) fn check_head(
        &self,
        head: &mut RequestParts,
        state: &mut S,
    ) -> Result<(), R>
    where
        R: HeadOnlyReply,
    {
        for step in &self.before {
            if let ControlFlow::Break(reply) = (step.action)(head, state) {
                return Err(reply);
            }
        }
        Ok(())
    }
}
