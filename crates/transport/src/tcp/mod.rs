//! TCP for HTTP/1.1 and HTTP/2, driven by Hyper.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;

use claviron_http::RouteResponse;
use claviron_http::ext::{ConnCtx, RequestArrival};
use claviron_http::session::SessionSpawner;
use hyper::Request;
use hyper::body::Incoming;

use crate::owner::Owner;

pub mod body;
pub mod io;
pub mod listener;
pub mod plain;
pub mod prior;
pub mod protocol;
#[cfg(feature = "tls")]
pub mod secure;
#[cfg(feature = "tls")]
pub mod tls;

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

/// One connection of a listener whose every request has one [`Owner`].
pub struct OwnerRequests<O> {
    owner: Arc<O>,
    connection: Option<Arc<ConnCtx>>,
    sessions: SessionSpawner,
}

impl<O: Owner> OwnerRequests<O> {
    pub(crate) fn new(
        owner: Arc<O>,
        connection: Option<Arc<ConnCtx>>,
        sessions: SessionSpawner,
    ) -> Self {
        Self { owner, connection, sessions }
    }
}

impl<O: Owner> Requests for OwnerRequests<O> {
    fn call(
        &self,
        mut request: Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<O>
    {
        attach_sessions(&mut request, &self.sessions);
        let answering = self.owner.call(
            request.map(body::adapt),
            self.connection.as_ref(),
            RequestArrival::Established,
        );
        async move { Ok(answering.await) }
    }
}
