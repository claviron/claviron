//! Accepted connection facts and narrowed declarations; no request executor.

use std::sync::Arc;

use crate::transport::VhostCtx;
use claviron_http::config::HttpPolicy;
use claviron_http::ext::{ConnCtx, RequestArrival};
use claviron_pipeline::HostContext;

pub(super) struct HostPolicy {
    pub(super) connection: Arc<ConnCtx>,
    pub(super) host: VhostCtx,
    pub(super) http: HttpPolicy,
}

impl HostPolicy {
    pub(super) fn context(
        &self,
        arrival: RequestArrival,
    ) -> HostContext<'_> {
        HostContext::new(&self.connection, arrival)
            .with_host(&self.host.hostname, &self.host.vhost_key)
    }

    pub(super) fn new(
        connection: Arc<ConnCtx>,
        host: VhostCtx,
        listener: HttpPolicy,
    ) -> Self {
        let http = listener.narrow(&host.vhost_config.policy());
        Self { connection, host, http }
    }
}
