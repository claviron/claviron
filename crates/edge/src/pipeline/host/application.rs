//! Terminal request ownership, independent of host policy and runtime control.

use std::future::Future;

use claviron_http::{RouteBody, RouteResponse};
use futures_util::FutureExt;
use http::Request;

use crate::pipeline::other::OtherService;

/// One selected application's consuming request call.
///
/// This is the terminal boundary after host admission, not a synchronous
/// [`super::HostLayer`] or the application's lifecycle controller. The caller
/// transfers the request exactly once. Dropping the returned future cancels
/// that call and releases whatever request/backend state it still owns.
///
/// Each implementation keeps its concrete future. This contract adds no
/// service clone, task, readiness wait or future box. Other retains its
/// existing handler erasure; Axum retains its native oneshot future. Hosts
/// choose their owner before constructing either future, so an unused backend
/// cannot enlarge the selected backend's state.
///
/// `B` need not be cloneable or buffered. A terminal handler always answers;
/// an unclaimed request stays in [`claviron_http::ServiceResult::Unhandled`]
/// before this boundary. A response status does not signal fallthrough.
pub trait ApplicationHandler<B = RouteBody>: Send {
    /// Whether this owner enforces the effective request head timeout itself.
    /// AdmissionPolicy queries this only when a head limit was declared.
    fn times_its_head(&self) -> bool {
        false
    }

    fn call(
        self,
        request: Request<B>,
    ) -> impl Future<Output = RouteResponse> + Send;
}

impl ApplicationHandler for OtherService {
    fn times_its_head(&self) -> bool {
        OtherService::times_its_head(self)
    }

    fn call(
        self,
        request: Request<RouteBody>,
    ) -> impl Future<Output = RouteResponse> + Send {
        // Retain the owner beside its existing erased future through the head
        // or cancellation; do not introduce another request-carrying future.
        self.handle(request).map(move |reply| {
            let _owner = self;
            match reply {
                Ok(response) => response,
                Err(never) => match never {},
            }
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/application.rs"]
mod tests;
