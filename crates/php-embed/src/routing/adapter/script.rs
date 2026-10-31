use std::convert::Infallible;
use std::sync::Arc;

use claviron_http::{HttpExchange, RouteBody, RouteResponse, ServiceResult};
use claviron_php::PhpRuntime;
use http::Request;
use tokio::sync::mpsc::Sender;

use crate::routing::proxy::upstream_sapi_gateway;
use crate::{PhpScriptService, SapiGatewayError};

#[derive(Clone)]
pub struct PhpScriptAdapter {
    php: Arc<PhpScriptService>,
    runtime: Arc<PhpRuntime>,
    exchange_tx: Option<Sender<HttpExchange>>,
}

impl PhpScriptAdapter {
    pub fn new(
        php: Arc<PhpScriptService>,
        host_cfg: Arc<PhpRuntime>,
    ) -> Self {
        Self { php, runtime: host_cfg, exchange_tx: None }
    }

    pub fn with_exchange_tx(
        mut self,
        tx: Option<Sender<HttpExchange>>,
    ) -> Self {
        self.exchange_tx = tx;
        self
    }

    pub async fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, Infallible> {
        Ok(upstream_sapi_gateway(
            Arc::clone(&self.php),
            Arc::clone(&self.runtime),
            req,
            self.exchange_tx.clone(),
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
        crate::routing::proxy::try_php_proxy(
            self.php,
            self.runtime,
            req,
            self.exchange_tx,
        )
        .await
    }
}
