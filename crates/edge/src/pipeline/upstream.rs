//! Prepared native HTTP upstream selection after local file steps.

use claviron_http::config::host::HostUpstreamConfig;
use claviron_http::{RouteBody, ServiceResult, err_status};
use claviron_runtime::AppState;
use claviron_upstream::UpstreamProvider;
use claviron_upstream::binding::{UpstreamApp, call_upstream_service};
use claviron_upstream::proxy::service::UpstreamRequestPolicy;
use http::{Request, StatusCode};

use super::host::{DispatchFuture, DispatchStep};

/// One configured upstream application. Selection retains its existing live
/// profile binding; accepted IO belongs to the selected service generation.
/// Local Static/TryFiles steps are composed by the host, not repeated here.
#[derive(Clone)]
pub struct UpstreamStage {
    app: UpstreamApp,
    policy: UpstreamRequestPolicy,
}

impl UpstreamStage {
    pub fn new(app: UpstreamApp, policy: UpstreamRequestPolicy) -> Self {
        Self { app, policy }
    }

    pub(crate) fn prepare(
        state: &AppState,
        host: &impl std::fmt::Display,
        config: Option<&HostUpstreamConfig>,
        policy: UpstreamRequestPolicy,
    ) -> Option<Self> {
        let name = config?.name();
        let Some(provider) = state.registry_ref().resolve::<UpstreamProvider>() else {
            tracing::error!(%host, upstream = name,
                "host declares upstream but UpstreamProvider is not registered");
            return None;
        };
        let Some(app) = provider.app_for_state(name, state) else {
            tracing::error!(%host, upstream = name,
                "host declares unknown upstream app/profile");
            return None;
        };
        Some(Self::new(app, policy))
    }
}

impl DispatchStep for UpstreamStage {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let path = request.uri().path();
        let service = self.app.select_explicit(path).or_else(|| self.app.select_root(path));
        let Some(service) = service else {
            // This configured owner retains its explicit excludes and final
            // 404; a miss must not enter another backend or read the body.
            return DispatchFuture::ready(ServiceResult::Handled(err_status(StatusCode::NOT_FOUND)));
        };
        let policy = self.policy;
        DispatchFuture::new(async move {
            ServiceResult::Handled(call_upstream_service(
                service, request, policy.allow_h1_upgrade, policy.allow_connect_protocol,
            ).await)
        })
    }
}
