//! Native route policies surrounding the selected application and backend.

use std::sync::Arc;

use claviron_http::{RouteBody, RouteFuture, ServiceResult};
use http::Request;

use super::{HostLayer, HostLayerBuilder, HostPipeline, HostPipelineBuilder};
use crate::pipeline::ext::RouteLayer;
use crate::pipeline::other::{OtherHandler, OtherService};

impl HostLayer for &dyn RouteLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        RouteLayer::prepare(self, layer)
    }
}

impl OtherHandler for Arc<HostPipeline> {
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        let plan = self.clone();
        Box::pin(async move {
            Ok(match plan.prepare(request) {
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
pub(crate) fn bind_other(
    mut builder: HostPipelineBuilder,
    owner: OtherService,
) -> crate::Result<OtherService> {
    if builder.is_empty() {
        return Ok(owner);
    }
    builder.layer(crate::pipeline::RustRouter::Other(owner))?;
    Ok(OtherService::new(Arc::new(builder.build())))
}
