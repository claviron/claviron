use std::sync::Arc;
use super::{DispatchStep, HostLayer, HostLayerBuilder};

impl<S> HostLayer<S> for Arc<dyn DispatchStep> {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.request(move |request| self.call(request));
        Ok(())
    }
}

