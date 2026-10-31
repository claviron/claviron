//! Captured transport and host admission, before identity or application work.

use std::sync::Arc;

use claviron_http::ServiceResult;
use claviron_http::authority::RequestAuthority;
use claviron_http::ext::{ConnCtx, HostName};
use claviron_http::listener::HttpProtocol;

use super::*;
use crate::deny::{DenyCheckedRequest, DenyService};
use crate::service::{RequestArrival, RequestRejection, VhostCtx};

/// A request's native arrival and optional immutable cleartext admission proof.
/// These facts never come from a request extension or forwarding header.
pub struct IngressState {
    arrival: RequestArrival,
    authority: Option<RequestAuthority>,
    deny_checked: bool,
}

impl IngressState {
    pub fn new(arrival: RequestArrival) -> Self {
        Self { arrival, authority: None, deny_checked: false }
    }
}

/// Cold host ingress preparation. Captures only this connection's policy;
/// live ACL, proxy identity, quotas and handler dispatch remain separate.
///
/// Validation order is protocol, authority/binding, early data, H1 upgrade,
/// then deny. Disabled upgrade/deny gates contribute no head action. Typed
/// rejections stay intact for the native transport's response mapping.
pub struct IngressPolicy {
    protocol: Option<HttpProtocol>,
    binding_allowed: bool,
    host: HostName,
    upgrade: bool,
    #[cfg(feature = "http3")]
    early_data: crate::config::EarlyDataPolicy,
    deny: Arc<DenyService>,
}

impl IngressPolicy {
    pub fn new(
        connection: &ConnCtx,
        host: &VhostCtx,
        deny: Arc<DenyService>,
    ) -> Self {
        Self {
            protocol: connection.proto,
            binding_allowed: connection.proto.is_some_and(|protocol| {
                host.vhost_config
                    .allows_transport(&connection.listener, protocol)
            }),
            host: host.hostname.clone(),
            upgrade: host.vhost_config.transport.h1.upgrade,
            #[cfg(feature = "http3")]
            early_data: host.vhost_config.transport.h3.early_data,
            deny,
        }
    }

    pub(crate) fn into_pipeline(
        self
    ) -> HostPipeline<IngressState, RequestRejection> {
        let mut layer = HostLayerBuilder::default();
        self.register(&mut layer);
        let mut builder = HostPipelineBuilder::new();
        builder.layers.push(layer);
        builder.build()
    }

    fn register(
        self,
        layer: &mut HostLayerBuilder<IngressState, RequestRejection>,
    ) {
        let Some(protocol) = self.protocol else {
            layer.before(|_, _| {
                ControlFlow::Break(RequestRejection::MissingProtocol)
            });
            return;
        };
        let binding_allowed = self.binding_allowed;
        let host = self.host;
        layer.before(move |head, state| {
            let authority = match state.authority.take() {
                Some(authority) => Some(authority),
                None => match RequestAuthority::from_parts(head) {
                    Ok(authority) => authority,
                    Err(error) => {
                        return ControlFlow::Break(
                            RequestRejection::InvalidAuthority(error),
                        );
                    }
                },
            };
            if !binding_allowed
                || !authority_matches_sni(authority.as_ref(), &host)
            {
                return ControlFlow::Break(RequestRejection::Misdirected);
            }
            ControlFlow::Continue(())
        });

        #[cfg(feature = "http3")]
        let early_data = if protocol == HttpProtocol::H3 {
            self.early_data
        } else {
            crate::config::EarlyDataPolicy::DenyAll
        };
        #[cfg(feature = "http3")]
        match early_data {
            crate::config::EarlyDataPolicy::AllowAll => {}
            crate::config::EarlyDataPolicy::AllowIdempotent => {
                layer.before(|head, state| {
                    if state.arrival == RequestArrival::EarlyData
                        && !matches!(
                            head.method,
                            http::Method::GET
                                | http::Method::HEAD
                                | http::Method::OPTIONS
                        )
                    {
                        return ControlFlow::Break(RequestRejection::TooEarly);
                    }
                    ControlFlow::Continue(())
                });
            }
            crate::config::EarlyDataPolicy::DenyAll => {
                register_early_data_denial(layer);
            }
        }
        #[cfg(not(feature = "http3"))]
        register_early_data_denial(layer);

        if protocol == HttpProtocol::H1 && !self.upgrade {
            layer.before(|head, _| {
                if head.headers.contains_key(http::header::UPGRADE) {
                    ControlFlow::Break(RequestRejection::UpgradeForbidden)
                } else {
                    ControlFlow::Continue(())
                }
            });
        }
        if !self.deny.is_empty() {
            layer.before(move |head, state| {
                if !state.deny_checked && self.deny.is_blocked(head.uri.path())
                {
                    ControlFlow::Break(RequestRejection::PathDenied)
                } else {
                    ControlFlow::Continue(())
                }
            });
        }
    }
}

/// Compare validated authority with the pinned host; ports do not alter SNI
/// identity. None is only the valid HTTP/1.0 missing-authority case.
pub(crate) fn authority_matches_sni(
    authority: Option<&RequestAuthority>,
    host: &HostName,
) -> bool {
    authority.is_none_or(|authority| authority.host() == host.0.as_ref())
}

fn register_early_data_denial(
    layer: &mut HostLayerBuilder<IngressState, RequestRejection>
) {
    layer.before(|_, state| {
        if state.arrival == RequestArrival::EarlyData {
            ControlFlow::Break(RequestRejection::TooEarly)
        } else {
            ControlFlow::Continue(())
        }
    });
}

impl HostLayer<IngressState, RequestRejection> for IngressPolicy {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<IngressState, RequestRejection>,
    ) -> crate::Result<()> {
        self.register(layer);
        Ok(())
    }
}

impl HostPipeline<IngressState, RequestRejection> {
    /// Native ingress is a head-only plan: typed decision builders cannot add
    /// response/body hooks. The enclosing HostService retains the pinned plan
    /// and resumes its existing identity/dispatch path with the original body.
    pub(crate) fn admit<B>(
        &self,
        request: Request<B>,
        state: IngressState,
    ) -> Result<Request<B>, RequestRejection> {
        match self.prepare_with_state(request, state) {
            ServiceResult::Handled(reply) => Err(reply.response),
            ServiceResult::Unhandled(prepared) => {
                Ok(Request::from_parts(prepared.head, prepared.body))
            }
        }
    }

    pub(crate) fn admit_checked<B>(
        &self,
        request: DenyCheckedRequest<'_, B>,
        deny: &DenyService,
        arrival: RequestArrival,
        authority: Option<RequestAuthority>,
    ) -> Result<Request<B>, RequestRejection> {
        let (request, deny_checked) = request.into_ingress(deny);
        // Only native ingress can construct these proof fields. No earlier
        // action mutates the parsed authority or the deny-checked path.
        self.admit(request, IngressState { arrival, authority, deny_checked })
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/ingress.rs"]
mod tests;
