//! Thin Hyper request/body adaptation. Host behavior belongs to HostPipeline.

use std::convert::Infallible;
use std::future::Future;

use crate::transport::PreparedRequest;
#[cfg(feature = "tls")]
use crate::transport::{HostService, RequestArrival};
use claviron_http::RouteResponse;
#[cfg(feature = "tls")]
use hyper::body::Incoming;

mod body;

#[cfg(feature = "tls")]
pub(crate) fn service(
    host: HostService
) -> impl hyper::service::Service<
    http::Request<Incoming>,
    Response = RouteResponse,
    Error = Infallible,
    Future: Send,
> {
    // One connection's service: its requests are admitted on its own task.
    let accepted =
        std::cell::RefCell::new(crate::service::AcceptedAuthority::default());
    hyper::service::service_fn(move |request| {
        use futures_util::future::{Either, ready};
        if let Err(reason) =
            host.validate_authority(&request, &mut accepted.borrow_mut())
        {
            return Either::Left(ready(Ok(reason.into())));
        }
        Either::Right(dispatch(
            host.request(request, RequestArrival::Established),
        ))
    })
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
