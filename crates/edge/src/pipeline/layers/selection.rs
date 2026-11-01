//! One prefix/condition selection as an explicit prepared host action.

use claviron_http::dispatch::{RouteDispatch, RuleContext};
use crate::pipeline::other::OtherService;
use super::*;

pub struct RouteSelectionLayer {
    routes: RouteDispatch<OtherService>,
}

impl RouteSelectionLayer {
    pub fn new(routes: RouteDispatch<OtherService>) -> Self { Self { routes } }
}

impl<S> HostLayer<S> for RouteSelectionLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.request(move |request| {
            let owner = self.routes.select(&RuleContext {
                method: request.method(),
                uri: request.uri(),
                headers: request.headers(),
            });
            DispatchFuture::response(owner.handle(request))
        });
        Ok(())
    }
}
