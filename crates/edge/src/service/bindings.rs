use std::sync::Arc;

use claviron_http::ext::{HostKey, HostName};
use claviron_types::Host;

use super::{HostRejection, VhostCtx};
use crate::loader::table::VhostTable;

/// One handshake's immutable name view paired with its transport policy epoch.
///
/// Alias-only publications reuse canonical config Arcs. Canonical replacement
/// does not: policy must be prepared with the new declaration before
/// publication, or bindings to that host fail closed. A captured view never changes
/// during ClientHello processing or later request dispatch.
#[derive(Clone)]
pub struct VhostBindings {
    policy: Arc<VhostTable>,
    names: Arc<VhostTable>,
}

impl VhostBindings {
    pub fn new(
        policy: Arc<VhostTable>,
        names: Arc<VhostTable>,
    ) -> Self {
        Self { policy, names }
    }

    pub fn resolve_sni(
        &self,
        server_name: Option<&str>,
    ) -> Result<VhostCtx, HostRejection> {
        let sni = server_name.ok_or(HostRejection::Missing)?;
        let host = sni
            .parse()
            .map_err(|error| HostRejection::Invalid(sni.to_owned(), error))?;
        let (live, application) = self
            .names
            .resolve_with_application(&host)
            .ok_or_else(|| HostRejection::NotHosted(sni.to_owned()))?;
        let canonical = &live.canonical;
        let captured = self
            .policy
            .canonical_config(canonical)
            .filter(|config| Arc::ptr_eq(config, &live.config))
            .ok_or_else(|| HostRejection::NotHosted(sni.to_owned()))?;
        Ok(VhostCtx {
            hostname: HostName(Arc::new(host)),
            vhost_key: HostKey(Arc::new(canonical.clone())),
            vhost_config: captured,
            application: application.cloned(),
        })
    }
    /// The canonical key a handshake for `server_name` selects its TLS
    /// configuration by — and only that. Same acceptance as
    /// [`resolve_sni`](Self::resolve_sni): the live name must resolve to a
    /// host whose configuration is the one captured with transport policy;
    /// a mismatched host/policy generation is refused.
    /// Nothing is cloned or allocated for the answer: the connection's own
    /// binding still comes from one `resolve_sni` after the handshake.
    pub fn handshake_key(
        &self,
        server_name: Option<&str>,
    ) -> Option<&Host> {
        let host: Host = server_name?.parse().ok()?;
        let live = self.names.resolve_record(&host)?;
        let captured = self.policy.resolve_record(&live.canonical)?;
        (captured.canonical == live.canonical
            && Arc::ptr_eq(&captured.config, &live.config))
        .then_some(&live.canonical)
    }
}

#[cfg(test)]
#[path = "../../tests/service/bindings.rs"]
mod tests;
