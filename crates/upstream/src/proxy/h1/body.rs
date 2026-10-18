//! Owns response frames and the sole H1 lease machine through EOF or drop.
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use tokio::time::{Instant, Sleep};

use super::conn::H1Connection;
use super::lease::H1Lease;
use super::pool::{H1PooledConnection, PoolAccounting, TargetPool};
use crate::proxy::body::PoolBodyError;
use crate::proxy::failure::{DirtyReason, RequestFailure};

pin_project_lite::pin_project! {
    pub struct PooledResponseBody {
        #[pin] inner: Incoming,
        #[pin] idle_timer: Option<Sleep>,
        response_body_idle: Option<Duration>,
        lifecycle: Option<crate::proxy::lease::RequestLease>,
        #[pin] forced: Option<tokio_util::sync::WaitForCancellationFutureOwned>,
        lease: Option<ResponseLease>,
    }
    impl PinnedDrop for PooledResponseBody {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(lease) = this.lease.take() {
                if this.inner.is_end_stream() { lease.clean(); }
                else { lease.dirty(DirtyReason::ResponseBodyDroppedBeforeEof); }
            }
            // Otherwise ResponseLease::drop closes dirty.
        }
    }
}
struct ResponseLease {
    connection: Option<H1Connection>,
    accounting: PoolAccounting,
    lease: H1Lease,
    may_reuse: bool,
}
impl ResponseLease {
    fn clean(mut self) {
        self.lease.mark_reusable();
        debug_assert!(self.lease.is_reusable());
        let mut connection =
            self.connection.take().expect("live response lease");
        connection.mark_request_served();
        if self.may_reuse {
            self.accounting.return_idle(connection, &self.lease);
        } else {
            self.accounting.release();
        }
    }
    fn dirty(
        mut self,
        reason: DirtyReason,
    ) {
        self.lease.mark_dirty(reason.clone());
        #[cfg(debug_assertions)]
        tracing::trace!(reason = reason.as_str(), "upstream h1 dirty close");
        // Drop releases the dirty connection and pool accounting.
    }
}
impl PooledResponseBody {
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
        _pool: Arc<Mutex<TargetPool>>,
        mut pooled: H1PooledConnection,
        response_body_idle: Option<Duration>,
        may_reuse: bool,
    ) -> Self {
        pooled.lease.mark_body_owned();
        Self {
            inner,
            idle_timer: response_body_idle.map(tokio::time::sleep),
            response_body_idle,
            lifecycle: None,
            forced: None,
            lease: Some(ResponseLease {
                accounting: pooled.accounting,
                connection: Some(pooled.connection),
                lease: pooled.lease,
                may_reuse,
            }),
        }
    }
}
impl Body for PooledResponseBody {
    type Data = Bytes;
    type Error = PoolBodyError;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let mut this = self.project();
        if this.lease.is_none() {
            return Poll::Ready(None);
        }
        if this
            .forced
            .as_mut()
            .as_pin_mut()
            .is_some_and(|future| future.poll(cx).is_ready())
        {
            this.lease.take();
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
                        this.lease
                            .take()
                            .unwrap()
                            .dirty(DirtyReason::ResponseBodyIdleTimeout);
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
                this.lease
                    .take()
                    .unwrap()
                    .dirty(DirtyReason::ResponseBodyError);
                this.lifecycle.take();
                Poll::Ready(Some(Err(PoolBodyError::source(Box::new(error)))))
            }
            Poll::Ready(None) => {
                this.lifecycle.take();
                this.lease.take().unwrap().clean();
                Poll::Ready(None)
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        self.lease.is_none() || self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
