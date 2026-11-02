use super::{DispatchStep, HostLayer, HostLayerBuilder};
use crate::pipeline::link::LinkBackend;

impl<S> HostLayer<S> for LinkBackend {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.request(move |request| DispatchStep::call(&self, request));
        Ok(())
    }
}
