//! Optional Axum adaptation for explicitly supplied Rust applications.

use std::sync::Arc;

use axum::Router;
use claviron_http::{RouteBody, RouteFuture, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::Request;
use std::future::Future;
use tower::ServiceExt;

use super::other::{OtherHandler, OtherService};
use super::host::{
    ApplicationHandler, HostHandler,
    HostHandlerFuture, HostRequest,
};

struct AxumApplication(Router);

impl OtherHandler for AxumApplication {
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        Box::pin(ApplicationHandler::call(self.0.clone(), request).map(Ok))
    }
}

pub(crate) fn owner(router: Router) -> OtherService {
    OtherService::new(AxumApplication(router))
}

impl ApplicationHandler for Router {
    fn call(self, request: Request<RouteBody>) -> impl Future<Output = RouteResponse> + Send {
        self.oneshot(request.map(axum::body::Body::new)).map(|reply| match reply {
            Ok(response) => claviron_http::from_axum_response(response),
            Err(never) => match never {},
        })
    }
}

/// Adapt an application-owned Axum router to a native Rust contribution.
/// The router owns its response, including a 404. Status never means Unhandled.
pub fn application(router: Router) -> Arc<dyn HostHandler> {
    Arc::new(AxumApplication(router))
}

impl HostHandler for AxumApplication {
    fn call(&self, request: HostRequest) -> HostHandlerFuture {
        Box::pin(ApplicationHandler::call(self.0.clone(), request.into_request())
            .map(ServiceResult::Handled))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/file_routes.rs"]
mod file_route_tests;
