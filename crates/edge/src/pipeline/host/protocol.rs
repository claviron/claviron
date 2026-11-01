//! Explicit request-part policies for a selected transport and authority.

use claviron_http::authority::RequestAuthority;
use claviron_http::ext::{ConnCtx, HostName};
use claviron_http::listener::HttpProtocol;
use super::*;
use crate::service::{RequestRejection, VhostCtx};
#[cfg(feature = "http3")]
use crate::service::RequestArrival;

/// A constant rejection selected during binding, never a repeated config test.
pub struct RejectLayer(pub RequestRejection);

impl RejectLayer {
    pub fn for_binding(connection: &ConnCtx, host: &VhostCtx) -> Option<Self> {
        match connection.proto {
            None => Some(Self(RequestRejection::MissingProtocol)),
            Some(protocol) if !host.vhost_config.allows_transport(&connection.listener, protocol) => {
                Some(Self(RequestRejection::Misdirected))
            }
            _ => None,
        }
    }
}

impl<S, R: From<RequestRejection>> HostLayer<S, R> for RejectLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        layer.before(move |_, _| ControlFlow::Break(self.0.into()));
        Ok(())
    }
}

/// Compare request authority with an accepted TLS/SNI binding. Cleartext host
/// selection already validates its authority and does not need this layer.
pub struct AuthorityLayer(pub HostName);

impl<S, R: From<RequestRejection>> HostLayer<S, R> for AuthorityLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        layer.before(move |head, _| {
            match RequestAuthority::from_parts(head) {
                Err(error) => ControlFlow::Break(RequestRejection::InvalidAuthority(error).into()),
                Ok(Some(authority)) if authority.host() != self.0.0.as_ref() => {
                    ControlFlow::Break(RequestRejection::Misdirected.into())
                }
                _ => ControlFlow::Continue(()),
            }
        });
        Ok(())
    }
}

/// Only an H1 binding that forbids upgrades contributes this check.
pub struct UpgradeLayer;

impl UpgradeLayer {
    pub fn new(protocol: HttpProtocol, allowed: bool) -> Option<Self> {
        (protocol == HttpProtocol::H1 && !allowed).then_some(Self)
    }
}

impl<S, R: From<RequestRejection>> HostLayer<S, R> for UpgradeLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        layer.before(|head, _| {
            if head.headers.contains_key(http::header::UPGRADE) {
                ControlFlow::Break(RequestRejection::UpgradeForbidden.into())
            } else { ControlFlow::Continue(()) }
        });
        Ok(())
    }
}

/// Replay policy exists only on H3 bindings. AllowAll contributes no layer.
#[cfg(feature = "http3")]
pub struct EarlyDataLayer(crate::config::EarlyDataPolicy);

#[cfg(feature = "http3")]
impl EarlyDataLayer {
    pub fn new(protocol: HttpProtocol, policy: crate::config::EarlyDataPolicy) -> Option<Self> {
        (protocol == HttpProtocol::H3 && !matches!(policy, crate::config::EarlyDataPolicy::AllowAll))
            .then_some(Self(policy))
    }
}

#[cfg(feature = "http3")]
impl<S: AsRef<RequestArrival>, R: From<RequestRejection>> HostLayer<S, R> for EarlyDataLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        match self.0 {
            crate::config::EarlyDataPolicy::AllowAll => {},
            crate::config::EarlyDataPolicy::DenyAll => {
                layer.before(|_, state| {
                    if *state.as_ref() == RequestArrival::EarlyData {
                        ControlFlow::Break(RequestRejection::TooEarly.into())
                    } else { ControlFlow::Continue(()) }
                });
            }
            crate::config::EarlyDataPolicy::AllowIdempotent => {
                layer.before(|head, state| {
                    if *state.as_ref() == RequestArrival::EarlyData
                        && !matches!(head.method, http::Method::GET | http::Method::HEAD | http::Method::OPTIONS) {
                        ControlFlow::Break(RequestRejection::TooEarly.into())
                    } else { ControlFlow::Continue(()) }
                });
            }
        }
        Ok(())
    }
}
