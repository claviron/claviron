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

/// What a request becomes at the door: its session offer when it asks for one,
/// or an answer when what it asks for is not valid.
#[cfg(any(feature = "websocket", feature = "edge"))]
pub(crate) type Attempt = Option<claviron_http::socket::Offer>;
#[cfg(not(any(feature = "websocket", feature = "edge")))]
pub(crate) type Attempt = Option<std::convert::Infallible>;

/// Attach what a session needs, and say whether this is an attempt at one.
#[cfg_attr(
    not(any(feature = "websocket", feature = "edge")),
    allow(clippy::unnecessary_wraps)
)]
pub(crate) fn attempt<B>(
    request: &mut Request<B>,
    sessions: &SessionSpawner,
) -> Result<Attempt, http::StatusCode> {
    attach_sessions(request, sessions);
    #[cfg(any(feature = "websocket", feature = "edge"))]
    return claviron_http::socket::attach(request, sessions);
    #[cfg(not(any(feature = "websocket", feature = "edge")))]
    Ok(None)
}

/// End an attempt with what the plan answered. An ordinary request has none and
/// is not touched.
pub(crate) fn finished(
    attempt: Attempt,
    response: RouteResponse,
) -> RouteResponse {
    #[cfg(any(feature = "websocket", feature = "edge"))]
    if let Some(offer) = attempt {
        // While owners still answer upgrades themselves, an attempt nobody
        // claimed keeps their answer.
        return if offer.claimed() { offer.finish(response) } else { response };
    }
    #[cfg(not(any(feature = "websocket", feature = "edge")))]
    let _ = attempt;
    response
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
        use futures_util::future::{Either, ready};
        let attempt = match attempt(&mut request, &self.sessions) {
            Ok(attempt) => attempt,
            Err(status) => {
                return Either::Left(ready(Ok(claviron_http::err_status(
                    status,
                ))));
            }
        };
        let answering = self.owner.call(
            request.map(body::adapt),
            self.connection.as_ref(),
            RequestArrival::Established,
        );
        Either::Right(async move { Ok(finished(attempt, answering.await)) })
    }
}
