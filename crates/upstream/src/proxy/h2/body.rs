//! Owns response frames and stream capacity until EOF, error or drop.
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use tokio::time::{Instant, Sleep};

use super::permit::H2Permit;
use crate::proxy::body::PoolBodyError;
use crate::proxy::failure::RequestFailure;

pin_project_lite::pin_project! {
    pub struct H2PooledResponseBody {
        #[pin] inner: Incoming,
        #[pin] idle_timer: Option<Sleep>,
        response_body_idle: Option<Duration>,
        lifecycle: Option<crate::proxy::lease::RequestLease>,
        #[pin] forced: Option<tokio_util::sync::WaitForCancellationFutureOwned>,
        permit: Option<H2Permit>,
    }
}
impl H2PooledResponseBody {
    pub(crate) fn with_lifecycle(
        mut self,
        lease: crate::proxy::lease::RequestLease,
    ) -> Self {
        self.forced = Some(lease.epoch.force.clone().cancelled_owned());
        self.lifecycle = Some(lease);
        self
    }

    pub fn new(
        inner: Incoming,
        permit: H2Permit,
        response_body_idle: Option<Duration>,
    ) -> Self {
        Self {
            inner,
            idle_timer: response_body_idle.map(tokio::time::sleep),
            response_body_idle,
            lifecycle: None,
            forced: None,
            permit: Some(permit),
        }
    }
}
impl Body for H2PooledResponseBody {
    type Data = Bytes;
    type Error = PoolBodyError;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let mut this = self.project();
        if this.permit.is_none() {
            return Poll::Ready(None);
        }
        if this
            .forced
            .as_mut()
            .as_pin_mut()
            .is_some_and(|future| future.poll(cx).is_ready())
        {
            this.permit.take();
            this.lifecycle.take();
            return Poll::Ready(Some(Err(PoolBodyError::pool(
                RequestFailure::ProfileStopped,
            ))));
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
                        this.permit.take();
                        this.lifecycle.take();
                        Poll::Ready(Some(Err(PoolBodyError::pool(
                            RequestFailure::ResponseBodyIdleTimeout,
                        ))))
                    }
                    Some(Poll::Pending) | None => Poll::Pending,
                }
            }
            Poll::Ready(Some(Ok(frame))) => {
                if let (Some(timer), Some(idle)) = (
                    this.idle_timer.as_mut().as_pin_mut(),
                    *this.response_body_idle,
                ) {
                    timer.reset(Instant::now() + idle);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.permit.take();
                this.lifecycle.take();
                Poll::Ready(Some(Err(PoolBodyError::source(Box::new(error)))))
            }
            Poll::Ready(None) => {
                this.lifecycle.take();
                this.permit.take();
                Poll::Ready(None)
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        self.permit.is_none() || self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
