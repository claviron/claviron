//! TCP for HTTP/1.1 and HTTP/2, driven by Hyper.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;

use claviron_http::session::SessionSpawner;
use claviron_http::{OtherHandler, RouteResponse};
use hyper::Request;
use hyper::body::Incoming;

pub mod body;
pub mod io;
pub mod listener;
pub mod plain;
pub mod prior;
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

/// Give a request that can take over its connection the owner of the session
/// it may start: a CONNECT or an `Upgrade`. An ordinary request pays nothing,
/// neither the handle's reference count nor its extension map.
pub fn attach_sessions<B>(
    request: &mut Request<B>,
    sessions: &SessionSpawner,
) {
    if request.method() == http::Method::CONNECT
        || request.headers().contains_key(http::header::UPGRADE)
    {
        request.extensions_mut().insert(sessions.clone());
    }
}

/// One connection of a listener that gives every request to one handler.
/// Nothing is selected and nothing else is attached: the handler receives the
/// request as the connection produced it, with only the body erased to
/// [`claviron_http::RouteBody`] and [`attach_sessions`] applied.
pub struct HandlerRequests<H> {
    handler: Arc<H>,
    sessions: SessionSpawner,
}

impl<H: OtherHandler> Requests for HandlerRequests<H> {
    fn call(
        &self,
        mut request: Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<H>
    {
        attach_sessions(&mut request, &self.sessions);
        self.handler.call(request.map(body::adapt))
    }
}
