use std::sync::Arc;

use claviron_http::authority::RequestAuthority;
use claviron_http::ext::{HostKey, HostName};

use super::VhostCtx;
use crate::loader::table::VhostTable;

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
