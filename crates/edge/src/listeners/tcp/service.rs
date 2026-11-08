//! Thin Hyper request/body adaptation. Host behavior belongs to HostPipeline.

use std::convert::Infallible;
use std::future::Future;

#[cfg(feature = "tls")]
use crate::service::HostService;
use crate::service::PreparedRequest;
use claviron_http::RouteResponse;
#[cfg(feature = "tls")]
use claviron_http::ext::RequestArrival;
#[cfg(feature = "tls")]
use hyper::body::Incoming;

use claviron_transport::tcp::body;

/// One TLS connection's requests: admitted on its own task against the
/// authority its handshake agreed, then handed to the bound host.
#[cfg(feature = "tls")]
pub(crate) struct HostRequests {
    host: HostService,
    accepted: std::cell::RefCell<claviron_http::authority::AcceptedAuthority>,
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
impl claviron_transport::tcp::Requests for HostRequests {
    fn call(
        &self,
        mut request: http::Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<>
    {
        use futures_util::future::{Either, ready};
        let attempt = match claviron_transport::tcp::attempt(
            &mut request,
            &self.sessions,
        ) {
            Ok(attempt) => attempt,
            Err(status) => {
                return Either::Left(ready(Ok(claviron_http::err_status(
                    status,
                ))));
            }
        };
        if let Err(reason) = self
            .host
            .validate_authority(&request, &mut self.accepted.borrow_mut())
        {
            return Either::Left(ready(Ok(reason.into())));
        }
        let answering =
            dispatch(self.host.request(request, RequestArrival::Established));
        Either::Right(async move {
            let Ok(response) = answering.await;
            Ok(claviron_transport::tcp::finished(attempt, response))
        })
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
