use crate::pipeline::extension::RouteLayer;
use super::{HostLayer, HostLayerBuilder};

impl HostLayer for &dyn RouteLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        RouteLayer::prepare(self, layer)
    }
}

