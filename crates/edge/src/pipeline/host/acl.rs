//! Configured ACL rules. Accepted identity is borrowed, not injected into a map.

use std::sync::Arc;
use claviron_http::ext::{ConnCtx, PathPrefix, RealIpExt};
use crate::config::acl::{AccessLimiter, AclConfig};
use crate::service::RequestRejection;
use super::*;

mod path;
use path::CompiledAcl;
#[cfg(feature = "rate-limit")]
pub(super) use path::comparison_paths;

pub struct AclLayer {
    rules: Arc<CompiledAcl>,
}

impl AclLayer {
    pub fn new(config: &AclConfig) -> Option<Self> {
        CompiledAcl::compile(config).map(|rules| Self { rules })
    }

    pub fn from_entries(
        entries: impl IntoIterator<Item = (PathPrefix, AccessLimiter)>,
    ) -> Option<Self> {
        CompiledAcl::from_entries(entries).map(|rules| Self { rules })
    }
}

impl<S: AsRef<ConnCtx>, R: From<RequestRejection>> HostLayer<S, R> for AclLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        layer.before(move |head, state| {
            let connection = state.as_ref();
            let ip = head.extensions.get::<RealIpExt>().map(|ip| ip.0)
                .or(connection.real_ip).unwrap_or_else(|| connection.remote.ip());
            match self.rules.check(head.uri.path(), ip) {
                Ok(()) => ControlFlow::Continue(()),
                Err(reason) => ControlFlow::Break(reason.into()),
            }
        });
        Ok(())
    }
}
