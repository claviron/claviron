use std::sync::Arc;

#[cfg(feature = "tcp")]
use claviron_http::authority::AuthorityError;
use claviron_http::authority::RequestAuthority;
use claviron_http::ext::{HostKey, HostName};
use claviron_types::Host;
#[cfg(feature = "tcp")]
use http::Request;

use super::VhostCtx;
#[cfg(feature = "tcp")]
use crate::deny::DenyCheckedRequest;
use crate::loader::table::VhostTable;

/// A cleartext authority bound to its unchanged, deny-checked request.
/// Only shared access is exposed until host admission consumes the proof.
#[cfg(feature = "tcp")]
pub(crate) struct CleartextRequest<'a, B> {
    request: DenyCheckedRequest<'a, B>,
    authority: RequestAuthority,
}

#[cfg(feature = "tcp")]
impl<'a, B> CleartextRequest<'a, B> {
    pub(crate) fn new(
        request: DenyCheckedRequest<'a, B>
    ) -> Result<Self, AuthorityError> {
        let authority = RequestAuthority::from_cleartext(request.request())?;
        Ok(Self { request, authority })
    }

    pub(crate) fn request(&self) -> &Request<B> {
        self.request.request()
    }

    pub(crate) fn authority(&self) -> &RequestAuthority {
        &self.authority
    }

    pub(super) fn into_parts(
        self
    ) -> (DenyCheckedRequest<'a, B>, RequestAuthority) {
        (self.request, self.authority)
    }
}

/// One connection's last immutable requested and canonical host values.
/// It retains no configuration, application or access policy, and never
/// records a failed host lookup.
#[cfg(feature = "tcp")]
#[derive(Default)]
pub(crate) struct HostIdentityCache {
    hostname: Option<HostName>,
    vhost_key: Option<HostKey>,
}

#[cfg(feature = "tcp")]
impl HostIdentityCache {
    pub(crate) fn resolve(
        &mut self,
        authority: &RequestAuthority,
        table: &VhostTable,
    ) -> Option<VhostCtx> {
        resolve_request_authority_with(authority, table, |canonical| {
            let name = self.hostname.get_or_insert_with(|| {
                HostName(Arc::new(authority.host().clone()))
            });
            if name.0.as_ref() != authority.host() {
                *name = HostName(Arc::new(authority.host().clone()));
            }
            let key = self
                .vhost_key
                .get_or_insert_with(|| HostKey(Arc::new(canonical.clone())));
            if key.0.as_ref() != canonical {
                *key = HostKey(Arc::new(canonical.clone()));
            }
            (name.clone(), key.clone())
        })
    }
}

/// Resolve an already validated wire authority against the configured table.
/// Preserve the requested host separately from its canonical binding.
pub fn resolve_request_authority(
    authority: &RequestAuthority,
    table: &VhostTable,
) -> Option<VhostCtx> {
    resolve_request_authority_with(authority, table, |canonical| {
        (
            HostName(Arc::new(authority.host().clone())),
            HostKey(Arc::new(canonical.clone())),
        )
    })
}

fn resolve_request_authority_with(
    authority: &RequestAuthority,
    table: &VhostTable,
    identity: impl FnOnce(&Host) -> (HostName, HostKey),
) -> Option<VhostCtx> {
    let record = table.resolve_record(authority.host())?;
    let (hostname, vhost_key) = identity(&record.canonical);
    Some(VhostCtx {
        hostname,
        vhost_key,
        vhost_config: record.config.clone(),
        application: table
            .prepared::<crate::pipeline::HostAppSnapshot>()
            .and_then(|apps| apps.resolve(&record.canonical)),
    })
}

#[cfg(all(test, feature = "tcp"))]
#[path = "../../tests/service/authority.rs"]
mod tests;
