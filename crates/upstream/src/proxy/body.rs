//! Owns the shared upstream response-body error vocabulary.

use std::error::Error as StdError;
use std::fmt;

use claviron_http::RouteBodyError;

#[derive(Debug)]
pub enum PoolBodyError {
    Pool(RequestFailure),
    Source(RouteBodyError),
}

impl PoolBodyError {
    pub(crate) fn pool(error: RequestFailure) -> Self {
        Self::Pool(error)
    }

    pub(crate) fn source(error: RouteBodyError) -> Self {
        Self::Source(error)
    }
}

impl fmt::Display for PoolBodyError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Pool(error) => write!(f, "{error}"),
            Self::Source(error) => write!(f, "{error}"),
        }
    }
}

impl StdError for PoolBodyError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Pool(error) => Some(error),
            Self::Source(error) => Some(&**error),
        }
    }
}

pub(crate) mod buffer;
pub(crate) mod timeout;

use crate::proxy::failure::RequestFailure;

// Concrete request variants retain the transport's existing RouteBody box.
// Replay retains explicitly bounded request bytes and trailers for a new attempt.
pin_project_lite::pin_project! {
    #[project = RequestBodyProjection]
    pub(crate) enum UpstreamBody<T = timeout::RequestBodyReadTimeout> {
        Empty,
        Route { #[pin] body: RouteBody },
        Timed { #[pin] body: T },
        Replay { bytes: Option<bytes::Bytes>, trailers: Option<http::HeaderMap>, lease: Option<crate::proxy::lease::InputLease> },
    }
}
impl<T: http_body::Body<Data = bytes::Bytes, Error = PoolBodyError>>
    http_body::Body for UpstreamBody<T>
{
    type Data = bytes::Bytes;
    type Error = RouteBodyError;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Option<Result<http_body::Frame<Self::Data>, Self::Error>>,
    > {
        use std::task::Poll;
        match self.project() {
            RequestBodyProjection::Empty => Poll::Ready(None),
            RequestBodyProjection::Route { body } => body.poll_frame(cx),
            RequestBodyProjection::Timed { body } => body
                .poll_frame(cx)
                .map(|frame| frame.map(|result| result.map_err(Into::into))),
            RequestBodyProjection::Replay { bytes, trailers, lease } => {
                let frame =
                    bytes.take().map(http_body::Frame::data).or_else(|| {
                        trailers.take().map(http_body::Frame::trailers)
                    });
                if frame.is_none() {
                    lease.take();
                }
                Poll::Ready(frame.map(Ok))
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Route { body } => body.is_end_stream(),
            Self::Timed { body } => body.is_end_stream(),
            Self::Replay { bytes, trailers, .. } => {
                bytes.is_none() && trailers.is_none()
            }
        }
    }
    fn size_hint(&self) -> http_body::SizeHint {
        match self {
            Self::Empty => http_body::SizeHint::with_exact(0),
            Self::Route { body } => body.size_hint(),
            Self::Timed { body } => body.size_hint(),
            Self::Replay { bytes, trailers, .. } => {
                let length = bytes.as_ref().map_or(0, |b| b.len() as u64);
                if trailers.is_some() {
                    let mut hint = http_body::SizeHint::new();
                    hint.set_lower(length);
                    hint
                } else {
                    http_body::SizeHint::with_exact(length)
                }
            }
        }
    }
}
use claviron_http::RouteBody;

// Hyper 1.11's H2 connection Future requires B: Unpin. Pin only a nonempty
// timed request; GET/empty requests and both response timers need no extra box.
pub(crate) type H2Body =
    UpstreamBody<std::pin::Pin<Box<timeout::RequestBodyReadTimeout>>>;
