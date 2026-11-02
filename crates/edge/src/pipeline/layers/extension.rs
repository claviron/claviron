use super::{HostLayer, HostLayerBuilder};
use crate::pipeline::extension::RouteLayer;

impl HostLayer for &dyn RouteLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        RouteLayer::prepare(self, layer)
    }
}
