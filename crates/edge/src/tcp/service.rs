//! Thin Hyper request/body adaptation. Host behavior belongs to HostPipeline.

use std::convert::Infallible;
use std::future::{Future, ready};

use crate::transport::{HostResponse, PreparedRequest};
#[cfg(feature = "tls")]
use crate::transport::{HostService, RequestArrival};
use claviron_http::{RouteResponse, empty_body};
use futures_util::future::Either;
use hyper::body::Incoming;

mod body;

#[cfg(feature = "tls")]
pub(crate) fn service(host: HostService) -> impl hyper::service::Service<
    http::Request<Incoming>,
    Response = RouteResponse,
    Error = Infallible,
    Future: Send,
> {
    hyper::service::service_fn(move |request| {
        dispatch(host.prepare(request, RequestArrival::Established))
    })
}

pub(crate) fn dispatch(
    prepared: Result<PreparedRequest<Incoming>, HostResponse>,
) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send {
    match prepared {
        Err(reply) => Either::Left(ready(Ok(
            reply.into_response().map(|()| empty_body()),
        ))),
        Ok(request) => {
            Either::Right(request.map_body(body::adapt).into_tcp_future())
        }
    }
}
