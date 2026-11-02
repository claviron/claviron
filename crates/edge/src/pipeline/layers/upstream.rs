use super::{DispatchStep, HostLayer, HostLayerBuilder};
use crate::pipeline::upstream::UpstreamStage;

impl<S> HostLayer<S> for UpstreamStage {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.request(move |request| DispatchStep::call(&self, request));
        Ok(())
    }
}
