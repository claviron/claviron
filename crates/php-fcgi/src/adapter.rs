use std::sync::Arc;

use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use claviron_php::PhpRuntime;
use http::Request;

use crate::service::PhpFcgiService;

/// FastCGI dispatch for HTTP input prepared by ingress; see [`PhpRuntime`].
#[derive(Clone)]
pub struct PhpFpmAdapter {
    php: Arc<PhpFcgiService>,
    runtime: Arc<PhpRuntime>,
}

impl PhpFpmAdapter {
    pub fn new(
        php: Arc<PhpFcgiService>,
        runtime: Arc<PhpRuntime>,
    ) -> Self {
        Self { php, runtime }
    }

    /// Try a runtime whose prefix the caller already selected. An unclaimed
    /// request retains its original, unread body; no FPM connection is acquired
    /// until script resolution claims the request. Consumes the handle rather
    /// than cloning its service/runtime references again during dispatch.
    pub async fn try_handle(
        self,
        req: Request<RouteBody>,
    ) -> Result<
        ServiceResult<RouteResponse, Request<RouteBody>>,
        crate::GatewayError,
    > {
        crate::proxy::try_fcgi_proxy(self.php, self.runtime, req).await
    }
}
