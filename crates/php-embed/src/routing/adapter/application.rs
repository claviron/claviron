use std::convert::Infallible;
use std::sync::Arc;

use claviron_http::policy::{HttpPolicy, HttpPolicyOverride};
use claviron_http::{RouteBody, RouteResponse, empty_body};
use claviron_php::PhpRuntime;
use claviron_runtime::{AppState, registry};
use http::{Request, Response, StatusCode};

use crate::engine::context::RequestEnvironment;
use crate::{PhpApplicationHandle, PhpApplicationService};

/// Explicit application selection for custom Rust routing, independent of Axum.
/// No filesystem script resolution or fleet forwarding happens on this path.
#[derive(Clone)]
pub struct PhpApplicationAdapter {
    application: PhpApplicationHandle,
    environment: Option<RequestEnvironment>,
    /// The route's `requests:` / `responses:`, narrowing what the edge hands
    /// over. Empty for a caller routing on its own.
    route: HttpPolicyOverride,
}

impl PhpApplicationAdapter {
    pub fn new(
        service: Arc<PhpApplicationService>,
        application: impl Into<String>,
    ) -> Self {
        Self {
            application: service
                .application(Arc::<str>::from(application.into())),
            environment: None,
            route: HttpPolicyOverride::default(),
        }
    }

    /// Resolve the site's request values while preparing the route. This never
    /// changes the application's boot environment, instances or management identity.
    /// Rebuilding the route rereads the files; no per-request I/O or watcher.
    pub fn environment(
        mut self,
        state: &AppState,
        runtime: &PhpRuntime,
    ) -> registry::Result<Self> {
        self.environment =
            if runtime.env_file.is_empty() && runtime.environment.is_empty() {
                None
            } else {
                let recipe = state
                    .program_recipe(
                        runtime.env_file.iter(),
                        runtime
                            .environment
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone())),
                    )
                    .map_err(|error| registry::Error::msg(error.to_string()))?;
                Some(RequestEnvironment(
                    state.config_reader().environments().read(recipe).map_err(
                        |error| registry::Error::msg(error.to_string()),
                    )?,
                ))
            };
        Ok(self)
    }

    /// The route's own policy blocks.
    pub fn route_policy(
        mut self,
        route: HttpPolicyOverride,
    ) -> Self {
        self.route = route;
        self
    }
    pub async fn handle(
        &self,
        mut request: Request<RouteBody>,
    ) -> Result<RouteResponse, Infallible> {
        let Some(policy) = request.extensions().get::<HttpPolicy>().copied()
        else {
            tracing::error!(application = %self.application.name(), "HttpPolicy is required for PHP application execution");
            let mut response = Response::new(empty_body());
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            return Ok(response);
        };
        let policy = policy.narrow(&self.route);
        request.extensions_mut().insert(policy);
        if let Some(environment) = &self.environment {
            request.extensions_mut().insert(environment.clone());
        }
        Ok(match self.application.execute(request).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(application = %self.application.name(), %error, "PHP application request failed");
                let mut response = Response::new(empty_body());
                *response.status_mut() = error.status_code();
                if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                    response.headers_mut().insert(
                        http::header::RETRY_AFTER,
                        http::HeaderValue::from_static("1"),
                    );
                }
                response
            }
        })
    }
}
