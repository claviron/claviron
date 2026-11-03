use super::{HostLayer, HostLayerBuilder};
use crate::pipeline::upstream::UpstreamStage;

impl<S> HostLayer<S> for UpstreamStage {
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
