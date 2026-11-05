//! Native Edge handoff to an external FPM execution owner.

use std::sync::Arc;

use claviron_edge::pipeline::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchCall, DispatchStep,
};
use claviron_http::{RouteBody, ServiceResult, bytes_body};
use claviron_php::PhpRuntime;
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use http::{Request, Response};

/// Prepare a selected runtime. The service retains FPM pool and lease ownership.
pub fn prepare(
    state: &AppState,
    runtime: PhpRuntime,
) -> Result<Arc<dyn BackendHandler>> {
    let service = state
        .registry_ref()
        .resolve::<crate::PhpFcgiService>()
        .ok_or_else(|| Error::msg("PhpFcgiService is not registered"))?;
    let shutdown = service.shutdown_token();
    Ok(Arc::new(Fcgi { service, runtime: Arc::new(runtime), shutdown }))
}

struct Fcgi {
    service: Arc<crate::PhpFcgiService>,
    runtime: Arc<PhpRuntime>,
    shutdown: tokio_util::sync::CancellationToken,
}
impl BackendHandler for Fcgi {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::Request,
            reply: BackendReply::ExternalEventStream(self.shutdown.clone()),
        }
    }
}
impl DispatchStep for Fcgi {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<claviron_edge::pipeline::host::HostContext<'_>>,
    ) -> DispatchCall {
        let service = self.service.clone();
        let runtime = self.runtime.clone();
        DispatchCall::new(async move {
            match crate::proxy::try_fcgi_proxy(service, runtime, request).await
            {
                Ok(result) => result,
                Err(error) => {
                    let mut response =
                        Response::new(bytes_body(error.to_string().into()));
                    *response.status_mut() = error.status_code();
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        http::HeaderValue::from_static(
                            "text/plain; charset=utf-8",
                        ),
                    );
                    ServiceResult::Handled(response)
                }
            }
        })
    }
}
