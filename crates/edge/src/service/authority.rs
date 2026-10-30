use std::sync::Arc;

#[cfg(feature = "tcp")]
use claviron_http::authority::AuthorityError;
use claviron_http::authority::RequestAuthority;
use claviron_http::ext::{HostKey, HostName};
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

/// Resolve an already validated wire authority against the configured table.
/// Preserve the requested host separately from its canonical binding.
pub fn resolve_request_authority(
    authority: &RequestAuthority,
    table: &VhostTable,
) -> Option<VhostCtx> {
    let (key, config) = table.resolve(authority.host())?;
    Some(VhostCtx {
        hostname: HostName(Arc::new(authority.host().clone())),
        vhost_key: HostKey(Arc::new(key.clone())),
        vhost_config: config,
        application: table
            .prepared::<crate::pipeline::HostAppSnapshot>()
            .and_then(|apps| apps.resolve(&key)),
    })
}

#[cfg(all(test, feature = "tcp"))]
#[path = "../../tests/service/authority.rs"]
mod tests;
