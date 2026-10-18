//! Bounded request collection; over-limit requests retain their streaming frames.
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use claviron_http::{RouteBody, boxed_body};
use http::HeaderMap;
use http_body::{Body, Frame, SizeHint};

use crate::proxy::failure::RequestFailure;
#[derive(Clone)]
pub(crate) struct Replay {
    pub(crate) bytes: Bytes,
    pub(crate) trailers: Option<HeaderMap>,
}
pub(crate) enum Collected {
    Replay(Replay),
    Streaming(RouteBody),
}
impl Replay {
    /// The held body sent once, for a request that is not retried.
    pub(crate) fn into_route_body(self) -> RouteBody {
        boxed_body(Held {
            bytes: (!self.bytes.is_empty()).then_some(self.bytes),
            trailers: self.trailers,
        })
    }

    pub(crate) fn body<T>(
        &self,
        lease: Option<crate::proxy::lease::InputLease>,
    ) -> super::UpstreamBody<T> {
        super::UpstreamBody::Replay {
            bytes: (!self.bytes.is_empty()).then(|| self.bytes.clone()),
            trailers: self.trailers.clone(),
            lease,
        }
    }
}
pub(crate) async fn collect(
    mut body: RouteBody,
    limit: u64,
    idle: Option<Duration>,
) -> Result<Collected, RequestFailure> {
    if body.is_end_stream() {
        return Ok(Collected::Replay(Replay {
            bytes: Bytes::new(),
            trailers: None,
        }));
    }
    if limit == 0 || body.size_hint().lower() > limit {
        return Ok(Collected::Streaming(body));
    }
    let capacity = body
        .size_hint()
        .upper()
        .map(|hint| hint.min(limit).min(usize::MAX as u64) as usize);
    let mut first = Bytes::new();
    let mut joined: Option<Vec<u8>> = None;
    let mut total = 0u64;
    let mut trailers = None;
    loop {
        let frame = super::timeout::within(
            idle,
            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)),
        )
        .await
        .map_err(|_| RequestFailure::RequestBodyReadIdleTimeout)?;
        let Some(frame) = frame else {
            return Ok(Collected::Replay(Replay {
                bytes: joined.map(Bytes::from).unwrap_or(first),
                trailers,
            }));
        };
        let frame = frame.map_err(|error| {
            tracing::debug!(%error, "upstream request buffer source failed");
            RequestFailure::RequestBodyFailed
        })?;
        let data = match frame.into_data() {
            Ok(data) => data,
            Err(frame) => {
                if trailers.is_some() {
                    return Err(RequestFailure::RequestBodyFailed);
                }
                trailers = frame.into_trailers().ok();
                continue;
            }
        };
        if trailers.is_some() {
            return Err(RequestFailure::RequestBodyFailed);
        }
        let next = total.checked_add(data.len() as u64);
        if next.is_none_or(|next| next > limit) {
            let prefix = joined.map(Bytes::from).unwrap_or(first);
            return Ok(Collected::Streaming(boxed_body(Prefix {
                prefix: (!prefix.is_empty()).then_some(prefix),
                overflow: Some(data),
                inner: body,
            })));
        }
        total = next.expect("bounded total");
        if data.is_empty() {
            continue;
        }
        if first.is_empty() && joined.is_none() {
            first = data;
            continue;
        }
        let storage = joined.get_or_insert_with(|| {
            let mut bytes = Vec::with_capacity(
                capacity.unwrap_or(total as usize).max(total as usize),
            );
            bytes.extend_from_slice(&first);
            first = Bytes::new();
            bytes
        });
        if storage.capacity() < total as usize {
            storage.reserve_exact(total as usize - storage.len());
        }
        storage.extend_from_slice(&data);
    }
}
/// A held body handed on once: its bytes, then its trailers.
struct Held {
    bytes: Option<Bytes>,
    trailers: Option<HeaderMap>,
}
impl Body for Held {
    type Data = Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if let Some(bytes) = self.bytes.take() {
            return Poll::Ready(Some(Ok(Frame::data(bytes))));
        }
        Poll::Ready(
            self.trailers.take().map(|trailers| Ok(Frame::trailers(trailers))),
        )
    }
    fn is_end_stream(&self) -> bool {
        self.bytes.is_none() && self.trailers.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(
            self.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64),
        )
    }
}
struct Prefix {
    prefix: Option<Bytes>,
    overflow: Option<Bytes>,
    inner: RouteBody,
}
impl Body for Prefix {
    type Data = Bytes;
    type Error = super::PoolBodyError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, super::PoolBodyError>>> {
        if let Some(bytes) = self.prefix.take().or_else(|| self.overflow.take())
        {
            return Poll::Ready(Some(Ok(Frame::data(bytes))));
        }
        Pin::new(&mut self.inner).poll_frame(cx).map(|frame| {
            frame.map(|result| result.map_err(super::PoolBodyError::source))
        })
    }
    fn is_end_stream(&self) -> bool {
        self.prefix.is_none()
            && self.overflow.is_none()
            && self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        let held = self.prefix.as_ref().map_or(0, |bytes| bytes.len() as u64)
            + self.overflow.as_ref().map_or(0, |bytes| bytes.len() as u64);
        let original = self.inner.size_hint();
        let mut hint = SizeHint::new();
        hint.set_lower(held.saturating_add(original.lower()));
        if let Some(upper) =
            original.upper().and_then(|upper| upper.checked_add(held))
        {
            hint.set_upper(upper);
        }
        hint
    }
}
