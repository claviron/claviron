//! Immutable native policy preparation retained by an accepted host binding.

use std::sync::Arc;

use claviron_http::config::HttpPolicy;
use claviron_http::ext::ConnCtx;

#[cfg(feature = "access-log")]
use super::AccessLogLayer;
use super::{AdmissionPolicy, HostPipeline, HostRequestState, ReplyPolicy};
use crate::VhostResolver;
use crate::service::{HostIngress, HostResponse, VhostCtx};

pub(crate) struct HostPolicy {
    #[cfg(any(test, all(feature = "tcp", feature = "tls")))]
    pub(crate) connection: Arc<ConnCtx>,
    pub(crate) host: VhostCtx,
    pub(crate) ingress: HostIngress,
    pub(crate) http: HttpPolicy,
    pub(crate) admission_policy: AdmissionPolicy,
    pub(crate) reply: Option<ReplyPolicy>,
    #[cfg(feature = "access-log")]
    pub(crate) access_log: Option<AccessLogLayer>,
    pub(crate) pipeline: HostPipeline<HostRequestState, HostResponse>,
}

impl HostPolicy {
    pub(crate) fn new(
        connection: Arc<ConnCtx>,
        host: VhostCtx,
        ingress: HostIngress,
        access: Arc<VhostResolver>,
        listener: HttpPolicy,
    ) -> Self {
        // Both declarations belong to this immutable binding. Requests reuse
        // their narrowed limits; live ACL/redirect policy is read separately.
        let http = listener.narrow(&host.vhost_config.policy());
        let admission_policy = AdmissionPolicy::new(http.requests);
        let reply = ReplyPolicy::new(http.responses);
        #[cfg(feature = "access-log")]
        let access_log = AccessLogLayer::new(
            host.vhost_key.clone(),
            host.vhost_config.access_log(),
        );
        let pipeline =
            HostPipeline::for_host(&connection, &host, &ingress, access);
        Self {
            #[cfg(any(test, all(feature = "tcp", feature = "tls")))]
            connection,
            host,
            ingress,
            http,
            admission_policy,
            reply,
            pipeline,
            #[cfg(feature = "access-log")]
            access_log,
        }
    }
}
