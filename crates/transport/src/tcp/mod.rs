//! TCP for HTTP/1.1 and HTTP/2, driven by Hyper.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;

use claviron_http::{OtherHandler, RouteResponse};
use hyper::Request;
use hyper::body::Incoming;

pub mod body;
pub mod io;
pub mod listener;
pub mod plain;
pub mod protocol;
#[cfg(feature = "tls")]
pub mod secure;

/// The owner of one connection's requests. It lives on that connection's task
/// and is called from it, so it needs no synchronization of its own.
pub trait Requests: Send + 'static {
    /// Answer one request. The response future owns what it needs; it does
    /// not borrow this owner.
    fn call(
        &self,
        request: Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<Self>;
}

/// One connection of a listener that gives every request to one handler.
/// Nothing is selected and nothing is attached: the handler receives the
/// request as the connection produced it, with only the body erased to
/// [`claviron_http::RouteBody`].
pub struct HandlerRequests<H>(Arc<H>);

impl<H: OtherHandler> Requests for HandlerRequests<H> {
    fn call(
        &self,
        request: Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<H>
    {
        self.0.call(request.map(body::adapt))
    }
}
