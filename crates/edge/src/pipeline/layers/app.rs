use std::sync::Arc;
#[cfg(feature = "axum")]
use futures_util::FutureExt;
use crate::pipeline::RustRouter;
use crate::pipeline::host::{HostHandler, HostRequest};
use super::{DispatchFuture, HostLayer, HostLayerBuilder};

impl<S> HostLayer<S> for RustRouter {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        match self {
            Self::Other(owner) => {
                layer.request(move |request| DispatchFuture::response(owner.handle(request)));
            }
            #[cfg(feature = "axum")]
            Self::Axum(router) => {
                use crate::pipeline::host::ApplicationHandler;
                layer.request(move |request| {
                    DispatchFuture::response(Box::pin(router.clone().call(request).map(Ok)))
                });
            }
        }
        Ok(())
    }
}

impl<S> HostLayer<S> for Arc<dyn HostHandler> {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.request(move |request| {
            DispatchFuture::application(self.call(HostRequest::new(request)))
        });
        Ok(())
    }
}

