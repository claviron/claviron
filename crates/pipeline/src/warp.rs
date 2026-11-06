//! Warp application adaptation and host binding.

use std::sync::Arc;

use claviron_http::warp::Router;
use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::Request;

use crate::plan::{
    ApplicationHandler, HostHandler, HostHandlerFuture, HostRequest,
};

impl ApplicationHandler for Router {
    fn call(
        self,
        request: Request<RouteBody>,
    ) -> impl std::future::Future<Output = RouteResponse> + Send {
        self.handle(request).map(move |response| {
            let _owner = self;
            let Ok(response) = response;
            response
        })
    }
}

struct WarpApplication(Router);

/// Adapt Warp to a native contribution; every Warp response is terminal.
pub fn application(router: Router) -> Arc<dyn HostHandler> {
    Arc::new(WarpApplication(router))
}

impl HostHandler for WarpApplication {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        Box::pin(
            ApplicationHandler::call(self.0.clone(), request.into_request())
                .map(ServiceResult::Handled),
        )
    }
}
