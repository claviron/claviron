//! Thin Hyper request/body adaptation. Host behavior belongs to HostPipeline.

use std::convert::Infallible;
use std::future::Future;

use claviron_edge::transport::PreparedRequest;
#[cfg(feature = "tls")]
use claviron_edge::transport::{HostService, RequestArrival};
use claviron_http::RouteResponse;
#[cfg(feature = "tls")]
use hyper::body::Incoming;

use crate::tcp::body;

/// One TLS connection's requests: admitted on its own task against the
/// authority its handshake agreed, then handed to the bound host.
#[cfg(feature = "tls")]
pub(crate) struct HostRequests {
    host: HostService,
    accepted: std::cell::RefCell<claviron_edge::transport::AcceptedAuthority>,
    sessions: claviron_http::session::SessionSpawner,
}

#[cfg(feature = "tls")]
impl HostRequests {
    pub(crate) fn new(
        host: HostService,
        sessions: claviron_http::session::SessionSpawner,
    ) -> Self {
        Self { host, accepted: Default::default(), sessions }
    }
}

#[cfg(feature = "tls")]
impl crate::tcp::Requests for HostRequests {
    fn call(
        &self,
        mut request: http::Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<>
    {
        use futures_util::future::{Either, ready};
        crate::tcp::attach_sessions(&mut request, &self.sessions);
        if let Err(reason) = self
            .host
            .validate_authority(&request, &mut self.accepted.borrow_mut())
        {
            return Either::Left(ready(Ok(reason.into())));
        }
        Either::Right(dispatch(
            self.host.request(request, RequestArrival::Established),
        ))
    }
}

pub(crate) fn dispatch<B>(
    request: PreparedRequest<B>
) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send
where
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    request.map_body(body::adapt).into_tcp_future()
}
