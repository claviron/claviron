//! One prefix/condition selection as an explicit prepared host action.

use claviron_http::dispatch::{RouteDispatch, RuleContext};
use crate::pipeline::other::OtherService;
use crate::pipeline::host::{ContextFuture, ContextHandler, HostContext};
use claviron_http::{RouteBody, RouteFuture};
use http::Request;
use std::sync::Arc;
use super::*;

pub struct RouteSelectionLayer {
    routes: RouteDispatch<OtherService>,
}

impl RouteSelectionLayer {
    pub fn new(routes: RouteDispatch<OtherService>) -> Self { Self { routes } }
}

impl<S> HostLayer<S> for RouteSelectionLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.request_context(self);
        Ok(())
    }
}

impl ContextHandler for RouteSelectionLayer {
    fn call(self: Arc<Self>, request: Request<RouteBody>) -> RouteFuture {
        self.routes.select(&RuleContext {
            method: request.method(), uri: request.uri(), headers: request.headers(),
        }).handle(request)
    }

    fn call_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a> {
        self.routes.select(&RuleContext {
            method: request.method(), uri: request.uri(), headers: request.headers(),
        }).handle_in(request, context)
    }
}
