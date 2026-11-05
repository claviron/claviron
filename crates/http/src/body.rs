//! Keeping a request body inside what the listener agreed to accept.
//!
//! Two different failures wear the same clothes at this level. A body that
//! keeps arriving past the size the listener allows is one; a body that stops
//! arriving while holding the connection open is the other. Neither is
//! expressible as the other — a size cap says nothing about a peer that has
//! gone quiet, and a stall timer says nothing about a peer that is very much
//! still sending — so `RequestPolicy` carries both and this module enforces
//! them. Either may be unset, and then it is not enforced.
//!
//! Edge adapts H1/H2/H3 bodies before the host pipeline applies these bounds.
//! Protocol framing stays with each transport; body policy uses one guard.

use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::{Buf, Bytes};
use http::HeaderMap;
use http::header::CONTENT_LENGTH;
use http_body::{Body, Frame, SizeHint};
use pin_project_lite::pin_project;
use tokio::time::{Sleep, sleep};

/// Why a request body was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyLimit {
    /// More bytes than the listener allows.
    TooLarge { limit: usize },
    /// No further bytes within the listener's read timeout.
    Stalled { after: Duration },
}

impl fmt::Display for BodyLimit {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::TooLarge { limit } => {
                write!(
                    f,
                    "request body exceeds the listener limit of {limit} bytes"
                )
            }
            Self::Stalled { after } => {
                write!(f, "request body stalled for longer than {after:?}")
            }
        }
    }
}

impl StdError for BodyLimit {}

/// Refuses a declared `Content-Length` over the limit before anything is read.
///
/// Worth doing separately from the counting below: at this point the response
/// has not started, so the peer can be told `413` rather than having the
/// connection fail underneath it. A body without a declared length, or one that
/// lies, is caught by the counting instead — and by then a status line is no
/// longer available to send, which is why both exist.
///
/// A malformed `Content-Length` is left alone here. It is the transport's to
/// reject, and treating it as oversized would report the wrong problem.
pub fn declared_length_within(
    headers: &HeaderMap,
    limit: Option<usize>,
) -> Result<(), BodyLimit> {
    let Some(limit) = limit else {
        return Ok(());
    };
    let declared = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());

    match declared {
        Some(length) if length > limit => Err(BodyLimit::TooLarge { limit }),
        _ => Ok(()),
    }
}

pin_project! {
    /// A body guard for callers that retain their own concrete body type.
    /// Optional policies share accounting and timers with `guard_body`.
    pub struct GuardedBody<B> {
        #[pin]
        inner: CheckedBody<B, Option<SizeBound>, Option<IdleBound>>,
    }
}

impl<B> GuardedBody<B> {
    pub fn new(
        inner: B,
        limit: Option<usize>,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            inner: CheckedBody {
                inner,
                size: limit.map(SizeBound::new),
                idle: timeout.map(IdleBound::new),
            },
        }
    }
}

impl<B> Body for GuardedBody<B>
where
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    type Data = Bytes;
    type Error = Box<dyn StdError + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.project().inner.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

struct SizeBound {
    limit: usize,
    remaining: usize,
}

impl SizeBound {
    fn new(limit: usize) -> Self {
        Self { limit, remaining: limit }
    }
}

trait CountBody {
    fn check(
        &mut self,
        frame: &Frame<Bytes>,
    ) -> Result<(), BodyLimit>;
}

impl CountBody for () {
    fn check(
        &mut self,
        _: &Frame<Bytes>,
    ) -> Result<(), BodyLimit> {
        Ok(())
    }
}

impl CountBody for SizeBound {
    fn check(
        &mut self,
        frame: &Frame<Bytes>,
    ) -> Result<(), BodyLimit> {
        if let Some(data) = frame.data_ref() {
            let size = data.remaining();
            if size > self.remaining {
                self.remaining = 0;
                return Err(BodyLimit::TooLarge { limit: self.limit });
            }
            self.remaining -= size;
        }
        Ok(())
    }
}

impl CountBody for Option<SizeBound> {
    fn check(
        &mut self,
        frame: &Frame<Bytes>,
    ) -> Result<(), BodyLimit> {
        match self {
            Some(bound) => bound.check(frame),
            None => Ok(()),
        }
    }
}

pin_project! {
    struct IdleBound {
        timeout: Duration,
        #[pin]
        sleep: Option<Sleep>,
    }
}

impl IdleBound {
    fn new(timeout: Duration) -> Self {
        Self { timeout, sleep: None }
    }
}

trait WaitBody {
    fn progress(self: Pin<&mut Self>);
    fn poll_timeout(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Duration>;
}

impl WaitBody for () {
    fn progress(self: Pin<&mut Self>) {}
    fn poll_timeout(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Duration> {
        Poll::Pending
    }
}

impl WaitBody for IdleBound {
    fn progress(self: Pin<&mut Self>) {
        self.project().sleep.set(None);
    }

    fn poll_timeout(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Duration> {
        let mut this = self.project();
        if this.sleep.is_none() {
            this.sleep.set(Some(sleep(*this.timeout)));
        }
        ready!(
            this.sleep
                .as_mut()
                .as_pin_mut()
                .expect("idle timer is armed")
                .poll(cx)
        );
        Poll::Ready(*this.timeout)
    }
}

impl WaitBody for Option<IdleBound> {
    fn progress(self: Pin<&mut Self>) {
        if let Some(bound) = self.as_pin_mut() {
            bound.progress();
        }
    }

    fn poll_timeout(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Duration> {
        match self.as_pin_mut() {
            Some(bound) => bound.poll_timeout(cx),
            None => Poll::Pending,
        }
    }
}

pin_project! {
    struct CheckedBody<B, S, T> {
        #[pin]
        inner: B,
        size: S,
        #[pin]
        idle: T,
    }
}

impl<B, S, T> Body for CheckedBody<B, S, T>
where
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    S: CountBody,
    T: WaitBody,
{
    type Data = Bytes;
    type Error = Box<dyn StdError + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let mut this = self.project();
        match this.inner.poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Err(error) = this.size.check(&frame) {
                    this.idle.progress();
                    return Poll::Ready(Some(Err(Box::new(error))));
                }
                this.idle.progress();
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.idle.progress();
                Poll::Ready(Some(Err(error.into())))
            }
            Poll::Ready(None) => {
                this.idle.progress();
                Poll::Ready(None)
            }
            Poll::Pending => {
                // Only a pending read arms a timer. Immediately available
                // frames and policies without a timeout need no timer work.
                let after = ready!(this.idle.as_mut().poll_timeout(cx));
                Poll::Ready(Some(Err(Box::new(BodyLimit::Stalled { after }))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Guard only the incoming byte count, with no timer state or timer calls.
pub fn guard_size(
    body: crate::RouteBody,
    limit: usize,
) -> crate::RouteBody {
    wrap_guard(body, SizeBound::new(limit), ())
}

/// Guard only idle reads, with no byte counter or size comparisons.
pub fn guard_idle(
    body: crate::RouteBody,
    timeout: Duration,
) -> crate::RouteBody {
    wrap_guard(body, (), IdleBound::new(timeout))
}

/// Apply both declared bounds with one erased wrapper.
pub fn guard_size_and_idle(
    body: crate::RouteBody,
    limit: usize,
    timeout: Duration,
) -> crate::RouteBody {
    wrap_guard(body, SizeBound::new(limit), IdleBound::new(timeout))
}

fn wrap_guard<S, T>(
    body: crate::RouteBody,
    size: S,
    idle: T,
) -> crate::RouteBody
where
    S: CountBody + Send + 'static,
    T: WaitBody + Send + 'static,
{
    use http_body_util::BodyExt;
    if body.is_end_stream() {
        return body;
    }
    CheckedBody { inner: body, size, idle }.boxed_unsync()
}

/// Select the specialized guard for a caller holding optional declarations.
/// Frames and trailers pass through without collection or byte copies.
pub fn guard_body(
    body: crate::RouteBody,
    limit: Option<usize>,
    timeout: Option<Duration>,
) -> crate::RouteBody {
    match (limit, timeout) {
        (None, None) => body,
        (Some(limit), None) => guard_size(body, limit),
        (None, Some(timeout)) => guard_idle(body, timeout),
        (Some(limit), Some(timeout)) => {
            guard_size_and_idle(body, limit, timeout)
        }
    }
}

/// Apply declared response-body bounds through the same specialized guard.
pub fn guard_reply(
    body: crate::RouteBody,
    limit: Option<usize>,
    gap: Option<Duration>,
) -> crate::RouteBody {
    guard_body(body, limit, gap)
}

pin_project! {
    /// An external event stream whose body is released on its owner's shutdown.
    ///
    /// `NO_IMPLICIT_TERMINATION: EVENT_STREAM_ENDS_AT_SHUTDOWN`.
    /// Only external `text/event-stream` replies opt into this policy through
    /// [`ends_at_shutdown`]. Finite replies drain normally; embedded execution
    /// observes its own cancellation. Dropping the body releases only this
    /// exchange's resources, never the external application process.
    pub struct EndsAtShutdown<F> {
        body: Option<crate::RouteBody>,
        #[pin]
        shutdown: F,
    }
}

impl<F: Future<Output = ()>> EndsAtShutdown<F> {
    pub fn new(
        body: crate::RouteBody,
        shutdown: F,
    ) -> Self {
        Self { body: Some(body), shutdown }
    }
}

impl<F: Future<Output = ()>> Body for EndsAtShutdown<F> {
    type Data = Bytes;
    type Error = crate::RouteBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let Some(body) = this.body.as_mut() else {
            return Poll::Ready(None);
        };
        if this.shutdown.poll(context).is_ready() {
            this.body.take();
            tracing::info!(
                "NO_IMPLICIT_TERMINATION: EVENT_STREAM_ENDS_AT_SHUTDOWN"
            );
            return Poll::Ready(None);
        }
        let frame = Pin::new(body).poll_frame(context);
        if matches!(frame, Poll::Ready(None | Some(Err(_)))) {
            this.body.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.as_ref().is_none_or(Body::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        self.body
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(0), Body::size_hint)
    }
}

/// Wrap a reply in [`EndsAtShutdown`] if its head says it is an event
/// stream; any other reply is returned unchanged. The shutdown future is
/// constructed only for an event stream and stays inline in its body wrapper.
pub fn ends_at_shutdown<F: Future<Output = ()> + Send + 'static>(
    response: crate::RouteResponse,
    shutdown: impl FnOnce() -> F,
) -> crate::RouteResponse {
    let event_stream = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .is_some_and(|value| crate::policy::is_event_stream(value.as_bytes()));
    if !event_stream || response.body().is_end_stream() {
        return response;
    }
    response.map(|body| {
        crate::RouteBody::new(EndsAtShutdown::new(body, shutdown()))
    })
}

/// Hand a reply back as its policy says: held — to be sent whole with its
/// length known — or passed on as it comes. The reply's own head decides
/// first ([`ResponsePolicy::delivery`](crate::policy::ResponsePolicy::delivery):
/// `X-Accel-Buffering`, then
/// `text/event-stream`, then the policy), and `X-Accel-Buffering` is removed:
/// it is a word to the proxy, not the client.
///
/// A held reply that outgrows its limit hands on what was held and then the
/// rest as it arrives, so holding never costs more than the limit. For a
/// runner whose replies arrive as a stream (JavaScript, later an origin);
/// SAPI holds inside its own sink, where a fatal error can still change the
/// status.
pub async fn deliver_reply(
    headers: &mut HeaderMap,
    body: crate::RouteBody,
    policy: &crate::policy::ResponsePolicy,
) -> crate::RouteBody {
    use http_body_util::BodyExt;

    let declared = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    let delivery = policy.delivery(headers, declared);
    headers.remove(crate::policy::X_ACCEL_BUFFERING);
    let up_to = match delivery {
        crate::policy::Delivery::Pieces => return body,
        crate::policy::Delivery::Whole { up_to } => up_to,
    };

    let mut body = body;
    let mut held = bytes::BytesMut::new();
    loop {
        let next = match body.frame().await {
            None => return crate::bytes_body(held.freeze()),
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data)
                    if up_to.is_none_or(|limit| {
                        held.len() + data.len() <= limit
                    }) =>
                {
                    held.extend_from_slice(&data);
                    continue;
                }
                Ok(data) => Ok(Frame::data(data)),
                // Trailers end what can be held; they go on after it.
                Err(frame) => Ok(frame),
            },
            Some(Err(error)) => Err(error),
        };
        // Outgrown, or no longer holdable: what was held, then this, then
        // the rest as it arrives.
        return HeldThenRest {
            held: (!held.is_empty()).then(|| held.freeze()),
            next: Some(next),
            rest: body,
        }
        .boxed_unsync();
    }
}

/// A reply that outgrew its hold: what was held, the frame that did not fit,
/// then the rest as it arrives. Its length is no longer known.
struct HeldThenRest {
    held: Option<Bytes>,
    next: Option<Result<Frame<Bytes>, crate::RouteBodyError>>,
    rest: crate::RouteBody,
}

impl Body for HeldThenRest {
    type Data = Bytes;
    type Error = crate::RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(held) = self.held.take() {
            return Poll::Ready(Some(Ok(Frame::data(held))));
        }
        if let Some(next) = self.next.take() {
            return Poll::Ready(Some(next));
        }
        Pin::new(&mut self.rest).poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.held.is_none() && self.next.is_none() && self.rest.is_end_stream()
    }
}

#[cfg(test)]
#[path = "../tests/unit/body.rs"]
mod tests;
