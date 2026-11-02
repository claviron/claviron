use std::sync::Arc;
#[cfg(feature = "axum")]
use futures_util::FutureExt;
use crate::pipeline::RustRouter;
use crate::pipeline::host::{ContextFuture, ContextHandler, HostContext, HostHandler, HostRequest};
use claviron_http::{RouteBody, RouteFuture};
use http::Request;
use super::{DispatchFuture, HostLayer, HostLayerBuilder};

impl<S> HostLayer<S> for RustRouter {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.request_context(self);
        Ok(())
    }
}

impl ContextHandler for RustRouter {
    fn call(self: Arc<Self>, request: Request<RouteBody>) -> RouteFuture {
        match self.as_ref() {
            Self::Other(owner) => {
                owner.handle(request)
            }
            #[cfg(feature = "axum")]
            Self::Axum(router) => {
                use crate::pipeline::host::ApplicationHandler;
                Box::pin(router.clone().call(request).map(Ok))
            }
        }
    }

    fn call_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a> {
        match self {
            Self::Other(owner) => owner.handle_in(request, context),
            #[cfg(feature = "axum")]
            Self::Axum(router) => {
                use crate::pipeline::host::ApplicationHandler;
                Box::pin(router.clone().call(request).map(Ok))
            }
        }
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

