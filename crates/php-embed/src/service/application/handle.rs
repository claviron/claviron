//! A route-bound endpoint following one application's lifecycle generations.
use crate::SapiGatewayError;
use crate::engine::application::ApplicationRequests;
use arc_swap::ArcSwapOption;
use claviron_http::{RouteBody, RouteResponse};
use http::Request;
use std::sync::Arc;

#[derive(Clone)]
pub struct PhpApplicationHandle {
    name: Arc<str>,
    current: Arc<ArcSwapOption<ApplicationRequests>>,
}
impl PhpApplicationHandle {
    pub(super) fn new(name: Arc<str>) -> Self {
        Self { name, current: Arc::new(ArcSwapOption::empty()) }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub(super) fn publish(
        &self,
        requests: Option<Arc<ApplicationRequests>>,
    ) {
        self.current.store(requests);
    }
    /// Deliver typed HTTP to this application's existing instances. No name
    /// lookup, lifecycle lock, instance creation or body serialization occurs.
    pub async fn execute(
        &self,
        request: Request<RouteBody>,
    ) -> Result<RouteResponse, SapiGatewayError> {
        let requests = self.current.load_full().ok_or_else(|| {
            SapiGatewayError::ServiceUnavailable(format!(
                "PHP application '{}' is not running",
                self.name
            ))
        })?;
        requests.execute(request).await
    }
}
