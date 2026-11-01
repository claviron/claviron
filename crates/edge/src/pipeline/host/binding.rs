//! Accepted connection facts and narrowed declarations; no request executor.

use std::sync::Arc;
use claviron_http::config::HttpPolicy;
use claviron_http::ext::ConnCtx;
use crate::service::VhostCtx;

pub(crate) struct HostPolicy {
    pub(crate) connection: Arc<ConnCtx>,
    pub(crate) host: VhostCtx,
    pub(crate) http: HttpPolicy,
}

impl HostPolicy {
    pub(crate) fn new(connection: Arc<ConnCtx>, host: VhostCtx, listener: HttpPolicy) -> Self {
        let http = listener.narrow(&host.vhost_config.policy());
        Self { connection, host, http }
    }
}
