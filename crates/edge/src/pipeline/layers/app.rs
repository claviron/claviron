use super::{DispatchFuture, HostLayer, HostLayerBuilder};
use crate::pipeline::RustRouter;
use crate::pipeline::host::{
    ContextFuture, ContextHandler, HostContext, HostHandler, HostRequest,
};
use claviron_http::{RouteBody, RouteFuture};
use http::Request;
use std::sync::Arc;

impl<S> HostLayer<S> for RustRouter {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.request_context(self);
        Ok(())
    }
}

impl ContextHandler for RustRouter {
    fn call(
        self: Arc<Self>,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        self.handle(request)
    }

    fn call_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        _context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a> {
        self.handle(request)
    }
}

impl<S> HostLayer<S> for Arc<dyn HostHandler> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.request(move |request| {
            DispatchFuture::application(self.call(HostRequest::new(request)))
        });
        Ok(())
    }
}
