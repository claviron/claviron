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

struct LayeredOwner {
    pipeline: HostPipeline,
    owner: OtherService,
}

impl OtherHandler for Arc<LayeredOwner> {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let plan = self.clone();
        Box::pin(async move {
            Ok(match plan.pipeline.prepare(request) {
                ServiceResult::Handled(reply) => reply.finish(),
                ServiceResult::Unhandled(request) => {
                    request.dispatch_to(plan.owner.clone()).await
                }
            })
        })
    }
}

pub(crate) fn prepare_layers(
    owner: OtherService,
    layers: &[Arc<dyn RouteLayer>],
) -> crate::Result<OtherService> {
    if layers.is_empty() {
        return Ok(owner);
    }
    let mut builder = HostPipelineBuilder::new();
    for layer in layers {
        builder.layer(layer.as_ref())?;
    }
    let pipeline = builder.build();
    if pipeline.before.is_empty()
        && pipeline.after.is_empty()
        && pipeline.body.is_empty()
    {
        return Ok(owner);
    }
    Ok(OtherService::new(Arc::new(LayeredOwner { pipeline, owner })))
}
