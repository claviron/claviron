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
//! The policy is the one HTTP/3 already applies: a declared `Content-Length`
//! over the limit is refused before a byte is read, and everything else is
//! counted as it arrives. Only the mechanism differs, because the transports
//! hand over bodies differently — H3 unfolds its own receive stream, while
//! HTTP/1.1 and HTTP/2 arrive as an `http_body::Body` that can simply be
//! wrapped. Sharing the policy and not the plumbing is the honest split.

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
    /// A body that refuses to grow past `limit` or to stall; either unset is
    /// not enforced. A request's on the way in, a reply's on the way out —
    /// and on both it passes the body's own length and end through, so a
    /// guarded body keeps its `Content-Length`.
    ///
    /// The stall timer is restarted by every frame rather than run once for the
    /// whole body: a large upload on a slow link is legitimate and a total
    /// budget would punish it, while a peer that has stopped sending is caught
    /// within one timeout either way.
    pub struct GuardedBody<B> {
        #[pin]
        inner: B,
        #[pin]
        idle: Option<Sleep>,
        limit: Option<usize>,
        read: usize,
        timeout: Option<Duration>
    }
}

impl<B> GuardedBody<B> {
    pub fn new(
        inner: B,
        limit: Option<usize>,
        timeout: Option<Duration>,
    ) -> Self {
        Self { inner, idle: None, limit, read: 0, timeout }
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
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();

        // Armed lazily and re-armed per frame, so a body that is flowing never
        // pays for a timer it does not need.
        if this.idle.is_none()
            && let Some(timeout) = *this.timeout
        {
            this.idle.set(Some(sleep(timeout)));
        }

        match this.inner.poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    *this.read = this.read.saturating_add(data.remaining());
                    if let Some(limit) = *this.limit
                        && *this.read > limit
                    {
                        return Poll::Ready(Some(Err(Box::new(
                            BodyLimit::TooLarge { limit },
                        ))));
                    }
                }

                this.idle.set(None);

                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(error.into())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                let Some(idle) = this.idle.as_mut().as_pin_mut() else {
                    return Poll::Pending;
                };

                ready!(idle.poll(context));

                Poll::Ready(Some(Err(Box::new(BodyLimit::Stalled {
                    after: this.timeout.unwrap_or_default(),
                }))))
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

/// A reply body under its guard: its ceiling and the longest gap between two
/// of its pieces, either unset not enforced. Its length and end pass through.
pub fn guard_reply(
    body: crate::RouteBody,
    limit: Option<usize>,
    gap: Option<Duration>,
) -> crate::RouteBody {
    use http_body_util::BodyExt;
    GuardedBody::new(body, limit, gap).boxed_unsync()
}

/// A reply that ends when the process begins to shut down.
///
/// `NO_IMPLICIT_TERMINATION: EVENT_STREAM_ENDS_AT_SHUTDOWN`.
///
/// # Why this exists
///
/// A graceful shutdown waits for the requests already running: that is the
/// priority, and every reply that ends on its own gets it. The backend says
/// when a reply is over — FastCGI's `END_REQUEST`, an origin's end of body —
/// over TCP or a Unix socket, whatever protocol the client speaks, so even
/// behind an HTTP/3 client the edge knows at once. The drain is bounded by
/// declared values: the listener's `graceful_timeout`, an upstream profile's
/// `lifecycle.drain_timeout`, HTTP/3's `quic.idle_timeout` for a client that
/// died unseen.
///
/// A server-sent event stream is the reply that never ends on its own. It
/// has no completion to wait for, so a drain that waits for it waits out its
/// whole bound, on every worker that holds one, at every restart. Where the
/// execution is ours it hears the cancellation and ends itself (embedded
/// PHP's `claviron_is_cancelled()`); where it runs outside us — FastCGI's
/// FPM, an upstream origin — nothing can ask it to stop. So the edge ends its
/// own side: the body ends cleanly, the client's `EventSource` reconnects to
/// a worker that is serving, and the backend learns what it learns from any
/// client that left (FPM's connection or the origin stream is released).
///
/// That is a termination nobody declared, which the workspace rule
/// forbids; this tag is where it is owned. It is not a drain policy for
/// other replies: only `text/event-stream`, read once from the reply's head
/// by [`ends_at_shutdown`].
pub struct EndsAtShutdown {
    body: crate::RouteBody,
    shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    ended: bool,
}

impl EndsAtShutdown {
    pub fn new(
        body: crate::RouteBody,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        Self { body, shutdown: Box::pin(shutdown), ended: false }
    }
}

impl Body for EndsAtShutdown {
    type Data = Bytes;
    type Error = crate::RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if self.shutdown.as_mut().poll(context).is_ready() {
            self.ended = true;
            tracing::info!(
                "NO_IMPLICIT_TERMINATION: EVENT_STREAM_ENDS_AT_SHUTDOWN"
            );
            return Poll::Ready(None);
        }
        Pin::new(&mut self.body).poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.ended || self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

/// Wrap a reply in [`EndsAtShutdown`] if its head says it is an event
/// stream; any other reply is returned as it is, to be waited for.
pub fn ends_at_shutdown(
    response: crate::RouteResponse,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> crate::RouteResponse {
    let event_stream = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .is_some_and(|value| crate::policy::is_event_stream(value.as_bytes()));
    if !event_stream {
        return response;
    }
    response
        .map(|body| crate::RouteBody::new(EndsAtShutdown::new(body, shutdown)))
}

/// Hand a reply back as its policy says: held — to be sent whole with its
/// length known — or passed on as it comes. The reply's own head decides
/// first ([`ResponsePolicy::delivery`]: `X-Accel-Buffering`, then
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
