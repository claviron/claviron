//! Thin Hyper request/body adaptation. Host behavior belongs to HostPipeline.

use std::convert::Infallible;
use std::future::Future;

use crate::transport::PreparedRequest;
#[cfg(feature = "tls")]
use crate::transport::{HostService, RequestArrival};
use claviron_http::RouteResponse;
#[cfg(feature = "tls")]
use hyper::body::Incoming;

use claviron_transport::tcp::body;

/// One TLS connection's requests: admitted on its own task against the
/// authority its handshake agreed, then handed to the bound host.
#[cfg(feature = "tls")]
pub(crate) struct HostRequests {
    host: HostService,
    accepted: std::cell::RefCell<crate::service::AcceptedAuthority>,
}

#[cfg(feature = "tls")]
impl HostRequests {
    pub(crate) fn new(host: HostService) -> Self {
        Self { host, accepted: Default::default() }
    }
}

#[cfg(feature = "tls")]
impl claviron_transport::tcp::Requests for HostRequests {
    fn call(
        &self,
        request: http::Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<>
    {
        use futures_util::future::{Either, ready};
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
