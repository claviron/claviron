use super::{HostLayer, HostLayerBuilder};
use crate::pipeline::link::LinkBackend;

impl<S> HostLayer<S> for LinkBackend {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        crate::pipeline::host::Backend::new(
            self,
            claviron_http::policy::HttpPolicy::unlimited(),
        )
        .prepare(layer)
    }
}
