//! Prepared Rust host handlers before the configured files/backend chain.

use std::future::Future;
use std::pin::Pin;

use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use http::{Request, request::Parts};

/// An unclaimed request with its original body still inaccessible.
///
/// Head inspection/changes do not expose or poll the body. To handle the body,
/// consume this value with `into_request`; it cannot then be returned as
/// Unhandled. Only the pipeline constructs this capability.
pub struct HostRequest {
    head: Parts,
    body: RouteBody,
}

impl HostRequest {
    pub(in crate::pipeline) fn new(request: Request<RouteBody>) -> Self {
        let (head, body) = request.into_parts();
        Self { head, body }
    }

    pub fn head(&self) -> &Parts {
        &self.head
    }

    pub fn head_mut(&mut self) -> &mut Parts {
        &mut self.head
    }

    /// Claim the request, transferring its existing body without reading it.
    pub fn into_request(self) -> Request<RouteBody> {
        Request::from_parts(self.head, self.body)
    }
}

pub type HostHandlerResult = ServiceResult<RouteResponse, HostRequest>;
pub type HostHandlerFuture =
    Pin<Box<dyn Future<Output = HostHandlerResult> + Send>>;

/// A prepared Rust host handler.
///
/// The request arrives as the client sent it: no connection, host or policy
/// value is written into it. An owner that needs the accepted facts is a
/// route extension and borrows them from `HostContext`.
///
/// Return Handled even for an error response. Return Unhandled with the supplied
/// capability to continue with the same unread body. Status codes never select
/// another application. Runtime heterogeneous registration erases each active
/// application's future here; absent contributions install no stage.
pub trait HostHandler: Send + Sync + 'static {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture;
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/handler.rs"]
mod tests;
