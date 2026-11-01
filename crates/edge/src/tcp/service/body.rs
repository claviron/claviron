//! Native body representation without request policy selection.

use std::error::Error as StdError;

use bytes::Bytes;
use claviron_http::{RouteBody, boxed_body};
use http_body_util::Empty;
use hyper::body::Body;

pub(super) fn adapt<B>(body: B) -> RouteBody
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: StdError + Send + Sync + 'static,
{
    if body.is_end_stream() {
        // Only the transport's end-of-stream guarantee proves that there
        // are no data frames or trailers. GET and a zero size hint do not.
        // Empty is zero-sized, so its erased box needs no heap allocation.
        boxed_body(Empty::<Bytes>::new())
    } else {
        boxed_body(body)
    }
}

#[cfg(test)]
#[path = "../../../tests/tcp/unit/body.rs"]
mod tests;
