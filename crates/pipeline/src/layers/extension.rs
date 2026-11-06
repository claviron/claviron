use super::{HostLayer, HostLayerBuilder};
use crate::extension::RouteLayer;

impl<L: RouteLayer + ?Sized> HostLayer for &L {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        RouteLayer::prepare(self, layer)
    }
}

pub struct ScopedExtension<'a>(
    pub &'a dyn RouteLayer,
    pub &'a crate::extension::PreparedScope<'a>,
);

impl HostLayer for ScopedExtension<'_> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        self.0.prepare_scoped(layer, self.1)
    }
}
