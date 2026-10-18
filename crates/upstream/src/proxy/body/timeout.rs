//! Request read-idle timeout; the timer and wrapper stay inside the request.
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use claviron_http::RouteBody;
use http_body::{Body, Frame, SizeHint};
use tokio::time::{Instant, Sleep};

use super::{H2Body, PoolBodyError, UpstreamBody};
use crate::proxy::failure::RequestFailure;

/// A limit the request policy may leave unwritten ran out.
pub(crate) struct Elapsed;

/// `future` within `limit`; with no limit, as long as it takes. The shape
/// `tokio::time::timeout` gives, for a limit that is optional because the
/// request policy invents none.
pub(crate) async fn within<F: std::future::Future>(
    limit: Option<Duration>,
    future: F,
) -> Result<F::Output, Elapsed> {
    match limit {
        Some(limit) => {
            tokio::time::timeout(limit, future).await.map_err(|_| Elapsed)
        }
        None => Ok(future.await),
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct RequestBodyReadTimeout {
        #[pin] inner: RouteBody,
        timeout: Option<Duration>,
        #[pin] idle_timer: Option<Sleep>,
        done: bool,
        lease: Option<crate::proxy::lease::InputLease>,
    }
}
impl RequestBodyReadTimeout {
    fn new(
        inner: RouteBody,
        timeout: Option<Duration>,
        lease: Option<crate::proxy::lease::InputLease>,
    ) -> Self {
        Self {
            inner,
            timeout,
            idle_timer: timeout.map(tokio::time::sleep),
            done: false,
            lease,
        }
    }
}
pub(crate) fn wrap(
    req: http::Request<RouteBody>,
    timeout: Option<Duration>,
    lease: Option<crate::proxy::lease::InputLease>,
) -> http::Request<UpstreamBody> {
    wrap_with(req, timeout, lease, |body| body)
}
pub(crate) fn wrap_h2(
    req: http::Request<RouteBody>,
    timeout: Option<Duration>,
    lease: Option<crate::proxy::lease::InputLease>,
) -> http::Request<H2Body> {
    // Hyper 1.11 requires B: Unpin, while the embedded Sleep is !Unpin.
    // Pin each nonempty timed request exactly once (accepted allocation
    // boundary); constants or storage config cannot change this type contract.
    // Frames still pass through untouched. Empty bodies skip the wrapper.
    wrap_with(req, timeout, lease, Box::pin)
}
fn wrap_with<T>(
    req: http::Request<RouteBody>,
    timeout: Option<Duration>,
    lease: Option<crate::proxy::lease::InputLease>,
    pin: impl FnOnce(RequestBodyReadTimeout) -> T,
) -> http::Request<UpstreamBody<T>> {
    req.map(|body| {
        if body.is_end_stream() {
            UpstreamBody::Empty
        } else {
            UpstreamBody::Timed {
                body: pin(RequestBodyReadTimeout::new(body, timeout, lease)),
            }
        }
    })
}
impl Body for RequestBodyReadTimeout {
    type Data = Bytes;
    type Error = PoolBodyError;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        match this.inner.as_mut().poll_frame(cx) {
            Poll::Pending => {
                match this
                    .idle_timer
                    .as_mut()
                    .as_pin_mut()
                    .map(|timer| timer.poll(cx))
                {
                    Some(Poll::Ready(())) => {
                        *this.done = true;
                        this.lease.take();
                        Poll::Ready(Some(Err(PoolBodyError::pool(
                            RequestFailure::RequestBodyReadIdleTimeout,
                        ))))
                    }
                    Some(Poll::Pending) | None => Poll::Pending,
                }
            }
            Poll::Ready(Some(Ok(frame))) => {
                if let (Some(timer), Some(timeout)) =
                    (this.idle_timer.as_mut().as_pin_mut(), *this.timeout)
                {
                    timer.reset(Instant::now() + timeout);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                *this.done = true;
                this.lease.take();
                tracing::debug!(%error, "upstream incoming request body failed");
                Poll::Ready(Some(Err(PoolBodyError::pool(
                    RequestFailure::RequestBodyFailed,
                ))))
            }
            Poll::Ready(None) => {
                *this.done = true;
                this.lease.take();
                Poll::Ready(None)
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        self.done || self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
