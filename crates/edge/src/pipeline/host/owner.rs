//! Native route policies surrounding the selected application and backend.

use std::sync::Arc;

use claviron_http::{RouteBody, RouteFuture, ServiceResult};
use http::Request;

use super::{ContextFuture, ContextHandler, HostContext, HostPipeline, HostPipelineBuilder};
use crate::pipeline::other::OtherService;

impl<S: Default + Send + 'static> ContextHandler for HostPipeline<S> {
    fn call(self: Arc<Self>, request: Request<RouteBody>) -> RouteFuture {
        let plan = self;
        Box::pin(async move {
            Ok(match plan.prepare_with_state(request, S::default()) {
                ServiceResult::Handled(reply) => reply.finish(),
                ServiceResult::Unhandled(request) => {
                    request.dispatch(|request| async move {
                        drop(request);
                        claviron_http::err_status(http::StatusCode::NOT_FOUND)
                    }).await
                }
            })
        })
    }
    fn call_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a> {
        Box::pin(async move {
            Ok(match self.prepare_in(request, S::default(), context) {
                ServiceResult::Handled(reply) => reply.finish(),
                ServiceResult::Unhandled(request) => {
                    request.dispatch(|request| async move {
                        drop(request);
                        claviron_http::err_status(http::StatusCode::NOT_FOUND)
                    }).await
                }
            })
        })
    }
}

/// Finish a prepared scope with its explicit Rust/backend owner. With no
/// preceding work the original owner is already the complete call.
pub(crate) fn bind_other<S: Default + Send + 'static>(
    mut builder: HostPipelineBuilder<S>,
    owner: OtherService,
) -> crate::Result<OtherService> {
    if builder.is_empty() {
        return Ok(owner);
    }
    builder.layer(crate::pipeline::RustRouter::Other(owner))?;
    Ok(OtherService::Pipeline(Arc::new(builder.build())))
}
