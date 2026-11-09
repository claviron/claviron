//! The same prepared dispatch and pooled client used by production callers.
use super::{
    Result,
    config::{Launch, Transport, Workload},
};
use bytes::Bytes;
use claviron::http::config::routes::OrbitRouteConfig;
use claviron::http::policy::{Buffering, Limit, RequestPolicyOverride};
use claviron::http::{ServiceResult, bytes_body, empty_body};
use claviron::pipeline::link::LinkBackend;
use claviron::pipeline::{DispatchCall, DispatchStep};
use claviron::upstream::UpstreamProvider;
use claviron::upstream::proxy::service::UpstreamService;
use claviron::{AppState, FleetName};
use http::StatusCode;
use http_body_util::BodyExt;
use std::sync::Arc;

pub enum Backend {
    Orbit(LinkBackend),
    H2(UpstreamService),
}

impl Backend {
    pub fn prepare(
        state: &AppState,
        launch: &Launch,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(match launch.transport {
            Transport::Orbit => {
                let route = OrbitRouteConfig {
                    fleet_name: FleetName::try_new(&launch.application_fleet)?,
                    application: launch.application_name.clone(),
                };
                let policy = RequestPolicyOverride {
                    buffering: Some(Buffering::StreamDefault),
                    buffer_size: launch.args.buffer_size.map(Limit::Value),
                    queue_timeout: launch.args.queue_timeout,
                    ..Default::default()
                };
                Self::Orbit(
                    LinkBackend::new(state, &[route], None, policy, false)
                        .ok_or("Link backend was not prepared")?,
                )
            }
            Transport::H2c | Transport::UdsH2 => Self::H2(
                state
                    .registry_ref()
                    .resolve::<UpstreamProvider>()
                    .and_then(|provider| provider.service("application"))
                    .ok_or("upstream application was not prepared")?,
            ),
        }))
    }

    pub async fn exchange(
        &self,
        workload: Workload,
        body: &Bytes,
    ) -> Result<(StatusCode, Bytes)> {
        let builder = http::Request::builder()
            .uri(workload.path())
            .header(http::header::HOST, "application.example.test");
        let request = match workload {
            Workload::Plain => builder.method("GET").body(empty_body())?,
            Workload::Echo => builder
                .method("POST")
                .header(http::header::CONTENT_LENGTH, body.len())
                .body(bytes_body(body.clone()))?,
        };
        let response = match self {
            Self::Orbit(backend) => {
                let result = match backend.call(request, None) {
                    DispatchCall::Ready(result) => result,
                    DispatchCall::Pending(work) => work.await,
                };
                let ServiceResult::Handled(response) = result else {
                    return Err("Link did not handle its request".into());
                };
                response
            }
            // Boxed as the pipeline boxes it for an Edge: inline, its state
            // would size this future for every transport, and every request
            // of every transport would move it.
            Self::H2(service) => Box::pin(service.handle(request)).await,
        };
        let status = response.status();
        Ok((status, response.into_body().collect().await?.to_bytes()))
    }
}
