//! Thin Hyper request/body adaptation. Host behavior belongs to HostPipeline.

use std::convert::Infallible;
use std::future::Future;

use crate::transport::PreparedRequest;
#[cfg(feature = "tls")]
use crate::transport::{HostService, RequestArrival};
use claviron_http::RouteResponse;
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
    hyper::service::service_fn(move |request| {
        dispatch(host.request(request, RequestArrival::Established))
    })
}

pub(crate) fn dispatch(
    request: PreparedRequest<Incoming>
) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send {
    request.map_body(body::adapt).into_tcp_future()
}
