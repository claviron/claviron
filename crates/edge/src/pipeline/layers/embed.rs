use super::{HostLayer, HostLayerBuilder};
#[cfg(feature = "lua")]
use crate::pipeline::embed::lua::LuaBackend;
#[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
use crate::pipeline::embed::php::PhpBackend;

#[cfg(feature = "lua")]
impl<S> HostLayer<S> for LuaBackend {
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

#[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
impl<S> HostLayer<S> for PhpBackend {
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
