//! Immutable native policy preparation retained by an accepted host binding.

use std::sync::Arc;

use claviron_http::config::HttpPolicy;
use claviron_http::ext::ConnCtx;

use super::{HostPipeline, HostRequestState};
use crate::VhostResolver;
use crate::service::{HostResponse, VhostCtx};

pub(crate) struct HostPolicy {
    #[cfg(test)]
    pub(crate) connection: Arc<ConnCtx>,
    pub(crate) host: VhostCtx,
    pub(crate) http: HttpPolicy,
    pub(crate) pipeline: HostPipeline<HostRequestState, HostResponse>,
}

impl HostPolicy {
    pub(crate) fn new(
        connection: Arc<ConnCtx>,
        host: VhostCtx,
        access: Arc<VhostResolver>,
        listener: HttpPolicy,
    ) -> Self {
        // Both declarations belong to this immutable binding. Requests reuse
        // their narrowed limits; live ACL/redirect policy is read separately.
        let http = listener.narrow(&host.vhost_config.policy());
        let pipeline = HostPipeline::for_host(&connection, &host, access);
        Self {
            #[cfg(test)]
            connection,
            host,
            http,
            pipeline,
        }
    }
}
