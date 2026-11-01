use super::{DispatchStep, HostLayer, HostLayerBuilder};
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
        layer.request(move |request| DispatchStep::call(&self, request));
        Ok(())
    }
}

#[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
impl<S> HostLayer<S> for PhpBackend {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.request(move |request| DispatchStep::call(&self, request));
        Ok(())
    }
}
