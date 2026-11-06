use std::sync::Arc;

use claviron_edge::transport::{VhostCtx, VhostTable};
use claviron_http::authority::{AuthorityError, RequestAuthority};
use claviron_http::ext::{HostKey, HostName};
use http::Request;

/// A validated cleartext authority bound to its unchanged request.
/// Only shared access is exposed until host admission consumes the proof.
pub struct CleartextRequest<B> {
    request: Request<B>,
    authority: RequestAuthority,
}

impl<B> CleartextRequest<B> {
    pub fn new(request: Request<B>) -> Result<Self, AuthorityError> {
        let authority = RequestAuthority::from_cleartext(&request)?;
        Ok(Self { request, authority })
    }

    pub fn authority(&self) -> &RequestAuthority {
        &self.authority
    }

    pub fn request(&self) -> &Request<B> {
        &self.request
    }

    pub fn into_parts(self) -> (Request<B>, RequestAuthority) {
        (self.request, self.authority)
    }
}

/// One connection's last immutable requested and canonical host values.
/// It retains no configuration, application or access policy, and never
/// records a failed host lookup.
#[derive(Default)]
pub struct HostIdentityCache {
    hostname: Option<HostName>,
    vhost_key: Option<HostKey>,
}

impl HostIdentityCache {
    pub fn resolve(
        &mut self,
        authority: &RequestAuthority,
        table: &VhostTable,
        listener: Option<&claviron_http::ListenerId>,
    ) -> Option<VhostCtx> {
        let (record, application) =
            table.resolve_application_on(authority.host(), listener)?;
        let name = self.hostname.get_or_insert_with(|| {
            HostName(Arc::new(authority.host().clone()))
        });
        if name.0.as_ref() != authority.host() {
            *name = HostName(Arc::new(authority.host().clone()));
        }
        let key = self
            .vhost_key
            .get_or_insert_with(|| HostKey(Arc::new(record.canonical.clone())));
        if key.0.as_ref() != &record.canonical {
            *key = HostKey(Arc::new(record.canonical.clone()));
        }
        Some(VhostCtx {
            hostname: name.clone(),
            vhost_key: key.clone(),
            vhost_config: record.config.clone(),
            application: application.cloned(),
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/edge/service/authority.rs"]
mod tests;
