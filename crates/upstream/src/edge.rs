//! Prepared native HTTP upstream selection after local file steps.

use crate::UpstreamProvider;
use crate::app::UpstreamApp;
use crate::proxy::service::UpstreamRequestPolicy;
use crate::proxy::service::{UpstreamService, request::RequestScope};
use claviron_http::config::host::HostUpstreamConfig;
use claviron_http::{RouteBody, ServiceResult, err_status};
use claviron_runtime::AppState;
use http::{Request, StatusCode};

use claviron_edge::pipeline::backend::{BackendPlacement, factory};
use claviron_edge::pipeline::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchCall, DispatchStep, HostAppRegistry,
};
use claviron_runtime::registry::{Error, Result};
use std::sync::Arc;

/// Register the upstream directive before host plans are prepared.
pub fn register(state: &AppState) -> Result<()> {
    let registry =
        state.registry_ref().resolve::<HostAppRegistry>().ok_or_else(|| {
            Error::msg("Upstream requires the Edge host registry")
        })?;
    registry.register_backend_factory(factory::<HostUpstreamConfig, _>(
        "upstream",
        BackendPlacement::AfterFiles,
        |context, config| {
            let provider = context
                .state
                .registry_ref()
                .resolve::<UpstreamProvider>()
                .ok_or_else(|| {
                    Error::msg("UpstreamProvider is not registered")
                })?;
            let app = provider
                .app_for_state(config.name(), context.state)
                .ok_or_else(|| {
                    Error::msg(format!(
                        "unknown upstream '{}' for host '{}'",
                        config.name(),
                        context.host
                    ))
                })?;
            let transport = context.transport;
            Ok(vec![Arc::new(UpstreamStage::new(
                app,
                UpstreamRequestPolicy {
                    allow_h1_upgrade: transport.h1.upgrade,
                    allow_connect_protocol: transport.h2.connect_protocol,
                },
                context.state.shutdown_token(),
            ))])
        },
    ))
}

/// One configured upstream application. Selection retains its existing live
/// profile binding; accepted IO belongs to the selected service generation.
/// Local Static/TryFiles steps are composed by the host, not repeated here.
#[derive(Clone)]
struct UpstreamStage {
    app: UpstreamApp,
    policy: UpstreamRequestPolicy,
    shutdown: tokio_util::sync::CancellationToken,
}

impl UpstreamStage {
    fn new(
        app: UpstreamApp,
        policy: UpstreamRequestPolicy,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self { app, policy, shutdown }
    }
}

impl BackendHandler for UpstreamStage {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::None,
            reply: BackendReply::ExternalEventStream(self.shutdown.clone()),
        }
    }
}

impl DispatchStep for UpstreamStage {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<claviron_edge::pipeline::host::HostContext<'_>>,
    ) -> DispatchCall {
        let path = request.uri().path();
        let service = self
            .app
            .select_explicit(path)
            .or_else(|| self.app.select_root(path));
        let Some(service) = service else {
            // This configured owner retains its explicit excludes and final
            // 404; a miss must not enter another backend or read the body.
            return DispatchCall::ready(ServiceResult::Handled(err_status(
                StatusCode::NOT_FOUND,
            )));
        };
        let policy = self.policy;
        // Copied while the context is lent: the exchange outlives this call.
        // With no accepted transport behind the request there is no edge
        // policy for the profile's own to narrow.
        let edge = context
            .and_then(|context| context.policy())
            .copied()
            .unwrap_or(claviron_http::policy::HttpPolicy::unlimited());
        DispatchCall::new(async move {
            ServiceResult::Handled(
                call_upstream_service(service, request, edge, policy).await,
            )
        })
    }
}

async fn call_upstream_service(
    service: UpstreamService,
    request: Request<RouteBody>,
    edge: claviron_http::policy::HttpPolicy,
    policy: UpstreamRequestPolicy,
) -> claviron_http::RouteResponse {
    let mut scope = RequestScope::under(&request, edge);
    scope.policy = policy;
    service.handle_scoped(request, scope).await
}
