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

    /// Select Pusher HTTP namespaces before transferring or reading the body.
    /// Independent of host routing; Runner and Edge use the same protocol paths.
    pub fn accepts_path(path: &str) -> bool {
        let Some(path) = path.strip_prefix('/') else {
            return false;
        };
        let mut segments = path.split('/');
        match (segments.next(), segments.next(), segments.next()) {
            (Some("app"), Some(key), None) => !key.is_empty(),
            (Some("apps"), Some(id), rest) if !id.is_empty() => rest
                .is_none_or(|first| {
                    !first.is_empty() || segments.next().is_some()
                }),
            _ => false,
        }
    }

    pub async fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, Infallible> {
        Ok(handle_pusher_request(self.cfg.as_ref(), req).await)
    }
}

#[cfg(test)]
#[path = "../../tests/http/adapter.rs"]
mod tests;
