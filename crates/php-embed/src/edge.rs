//! Optional native Edge handoff; PHP execution and Runner APIs do not require Edge.

use std::sync::Arc;

use claviron_edge::pipeline::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchCall, DispatchStep,
};
use claviron_http::{
    RouteBody, RouteResponse, ServiceResult, bytes_body, empty_body,
};
use claviron_php::PhpRuntime;
use claviron_runtime::{AppState, registry::Result};
use http::{Request, Response, StatusCode};

mod app;
mod script;

/// Prepare a script runtime without starting an engine.
pub fn prepare_script(
    state: &AppState,
    runtime: PhpRuntime,
) -> Result<Arc<dyn BackendHandler>> {
    script::prepare(state, Arc::new(runtime))
}

/// Prepare a persistent application; an unavailable target retains its 503 owner.
pub fn prepare_application(
    state: &AppState,
    runtime: PhpRuntime,
) -> Arc<dyn BackendHandler> {
    app::prepare(state, runtime)
}

fn gateway_error(
    status: StatusCode,
    error: impl std::fmt::Display,
) -> RouteResponse {
    let mut response = Response::new(bytes_body(error.to_string().into()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

struct UnavailableApplication;
impl BackendHandler for UnavailableApplication {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::None,
            reply: BackendReply::Owner,
        }
    }
}
impl DispatchStep for UnavailableApplication {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<claviron_edge::pipeline::host::HostContext<'_>>,
    ) -> DispatchCall {
        drop(request);
        DispatchCall::ready(ServiceResult::Handled(
            Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(http::header::RETRY_AFTER, "1")
                .body(empty_body())
                .expect("static response"),
        ))
    }
}
