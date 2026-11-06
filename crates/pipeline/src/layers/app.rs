use super::{HostLayer, HostLayerBuilder};
use crate::plan::HostHandler;
use claviron_http::RustRouter;
use std::sync::Arc;

impl<S> HostLayer<S> for RustRouter {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        crate::plan::Backend::new(
            self,
            claviron_http::policy::HttpPolicy::unlimited(),
        )
        .prepare(layer)
    }
}

impl<S> HostLayer<S> for Arc<dyn HostHandler> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        crate::plan::Backend::new(
            self,
            claviron_http::policy::HttpPolicy::unlimited(),
        )
        .prepare(layer)
    }
}
