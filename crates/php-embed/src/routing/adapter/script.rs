use std::convert::Infallible;
use std::sync::Arc;

use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use claviron_php::PhpRuntime;
use http::Request;

use crate::routing::proxy::upstream_sapi_gateway;
use crate::{PhpScriptService, SapiGatewayError};

/// Script dispatch for HTTP input prepared by ingress; see [`PhpRuntime`].
#[derive(Clone)]
pub struct PhpScriptAdapter {
    php: Arc<PhpScriptService>,
    runtime: Arc<PhpRuntime>,
}

impl PhpScriptAdapter {
    pub fn new(
        php: Arc<PhpScriptService>,
        host_cfg: Arc<PhpRuntime>,
    ) -> Self {
        Self { php, runtime: host_cfg }
    }

    pub async fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, Infallible> {
        Ok(upstream_sapi_gateway(
            Arc::clone(&self.php),
            Arc::clone(&self.runtime),
            req,
        )
        .await)
    }

    /// Resolve one already-matched runtime without claiming a non-PHP body.
    /// On Unhandled the original request is returned without polling, decoding,
    /// collecting or replacing its body. Prefix selection belongs to the caller.
    /// Consumes this handle so dispatch transfers its existing service/runtime
    /// references instead of cloning them again inside the call.
    pub async fn try_handle(
        self,
        req: Request<RouteBody>,
    ) -> Result<
        ServiceResult<RouteResponse, Request<RouteBody>>,
        SapiGatewayError,
    > {
        crate::routing::proxy::try_php_proxy(self.php, self.runtime, req).await
    }
}
