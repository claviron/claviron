use super::{DispatchStep, HostLayer, HostLayerBuilder};
use std::sync::Arc;

impl<S> HostLayer<S> for Arc<dyn DispatchStep> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.request_in(move |request, context| self.call(request, context));
        Ok(())
    }
}
