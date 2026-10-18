use std::convert::Infallible;
use std::sync::Arc;

use claviron_http::{HttpExchange, RouteBody, RouteResponse};
use claviron_php::PhpRuntime;
use http::Request;
use tokio::sync::mpsc::Sender;

use crate::service::PhpFcgiService;

#[derive(Clone)]
pub struct PhpFpmAdapter {
    php: Arc<PhpFcgiService>,
    runtime: Arc<PhpRuntime>,
    exchange_tx: Option<Sender<HttpExchange>>,
}

impl PhpFpmAdapter {
    pub fn new(
        php: Arc<PhpFcgiService>,
        runtime: Arc<PhpRuntime>,
    ) -> Self {
        Self { php, runtime, exchange_tx: None }
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
        Ok(crate::proxy::upstream_fcgi_gateway(
            Arc::clone(&self.php),
            Arc::clone(&self.runtime),
            req,
            self.exchange_tx.clone(),
        )
        .await)
    }
}
