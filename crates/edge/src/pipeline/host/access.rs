//! Live canonical-host access, evaluated after trusted request identity.

use std::sync::Arc;

use claviron_http::ext::HostKey;

use super::*;
use crate::service::policy::PolicySnapshot;
use crate::service::{HostResponse, RequestRejection};
use crate::VhostResolver;

/// Retains the exact rule generation admitted for this request. A removed
/// canonical is distinct from an open host; no alias/default fallback applies.
pub struct AccessState {
    pub(super) snapshot: PolicySnapshot,
}

impl Default for AccessState {
    fn default() -> Self {
        Self { snapshot: PolicySnapshot::Removed }
    }
}

impl AsMut<AccessState> for AccessState {
    fn as_mut(&mut self) -> &mut AccessState {
        self
    }
}

/// One live canonical-host lookup per admission. Only configured ACL/redirect
/// rules are compiled into the selected snapshot. An open host clones no rule
/// Arc and traverses no rules; removal and live policy changes still apply to
/// the next request on an existing connection.
pub struct AccessPolicy {
    resolver: Arc<VhostResolver>,
    canonical: HostKey,
}

impl AccessPolicy {
    pub fn new(resolver: Arc<VhostResolver>, canonical: HostKey) -> Self {
        Self { resolver, canonical }
    }

    pub(super) fn register<S>(self, layer: &mut HostLayerBuilder<S, HostResponse>)
    where
        S: AsRef<IdentityState> + AsMut<AccessState>,
    {
        layer.before(move |head, state| {
            let Some(ip) = state.as_ref().connection().and_then(|connection| connection.real_ip) else {
                return ControlFlow::Break(RequestRejection::MissingIdentity.into());
            };
            let snapshot = self.resolver.request_policy(&self.canonical.0);
            let result = snapshot.check(&head.uri, ip);
            state.as_mut().snapshot = snapshot;
            match result {
                Ok(()) => ControlFlow::Continue(()),
                Err(reply) => ControlFlow::Break(reply),
            }
        });
    }
}

impl<S> HostLayer<S, HostResponse> for AccessPolicy
where
    S: AsRef<IdentityState> + AsMut<AccessState>,
{
    fn prepare(self, layer: &mut HostLayerBuilder<S, HostResponse>) -> crate::Result<()> {
        self.register(layer);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/access.rs"]
mod tests;
