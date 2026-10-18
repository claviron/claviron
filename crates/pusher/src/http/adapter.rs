use std::convert::Infallible;
use std::sync::Arc;

use claviron_http::{RouteBody, RouteResponse};
use http::Request;

use crate::HttpTargetConfig;
use crate::http::handler::handle_pusher_request;

#[derive(Clone)]
pub struct PusherAdapter {
    cfg: Arc<HttpTargetConfig>,
}

impl PusherAdapter {
    pub fn new(cfg: Arc<HttpTargetConfig>) -> Self {
        Self { cfg }
    }

    pub async fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, Infallible> {
        Ok(handle_pusher_request(self.cfg.as_ref(), req).await)
    }
}
