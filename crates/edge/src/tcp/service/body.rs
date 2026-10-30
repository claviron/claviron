//! Ordinary native request-body adaptation; tunnels keep their native body.

use std::error::Error as StdError;
use std::time::Duration;

use bytes::Bytes;
use claviron_http::body::GuardedBody;
use claviron_http::{RouteBody, boxed_body};
use http_body_util::{BodyExt, Empty};
use hyper::body::Body;

pub(super) fn adapt<B>(
    body: B,
    limit: Option<usize>,
    timeout: Option<Duration>,
) -> RouteBody
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: StdError + Send + Sync + 'static,
{
    if body.is_end_stream() {
        // Only the transport's end-of-stream guarantee proves that there
        // are no data frames or trailers. GET and a zero size hint do not.
        // Empty is zero-sized, so its erased box needs no heap allocation.
        boxed_body(Empty::<Bytes>::new())
    } else if limit.is_none() && timeout.is_none() {
        boxed_body(body)
    } else {
        GuardedBody::new(body, limit, timeout).boxed_unsync()
    }
}

#[cfg(test)]
#[path = "../../../tests/tcp/unit/body.rs"]
mod tests;
