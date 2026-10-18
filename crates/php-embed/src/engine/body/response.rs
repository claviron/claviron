//! Captured responses and bounded PHP-to-HTTP streaming.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use claviron_http::policy::{Delivery, ResponsePolicy, X_ACCEL_BUFFERING};
use claviron_http::{RouteBody, RouteResponse, boxed_body, bytes_body};
use futures_util::stream;
use http::{HeaderMap, Response, StatusCode};
use http_body::Frame;
use http_body_util::StreamBody;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::engine::SAPI_SHUTTING_DOWN;

// ---------------------------------------------------------------------
// Captured response — output from one PHP execution before it becomes
// a `RouteResponse`.
// ---------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct CapturedResponse {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: BytesMut,
}

impl Default for CapturedResponse {
    fn default() -> Self {
        Self {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: BytesMut::new(),
        }
    }
}

impl CapturedResponse {
    /// A reply held to its end, sent whole: status (after any fatal-error
    /// rewrite), headers and a body of known length.
    pub(in crate::engine) fn into_response(mut self) -> RouteResponse {
        self.headers.remove(X_ACCEL_BUFFERING);
        let mut response = Response::new(bytes_body(self.body.freeze()));
        *response.status_mut() = self.status;
        // `iter()`, not `into_iter()`: every repeated header (several
        // `Set-Cookie`) keeps its name.
        let dst = response.headers_mut();
        for (name, value) in self.headers.iter() {
            dst.append(name.clone(), value.clone());
        }
        response
    }

    pub(in crate::engine) fn mark_script_failure(&mut self) {
        if self.status.as_u16() < 400 {
            self.status = StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    pub(in crate::engine) fn contains_php_fatal_output(&self) -> bool {
        let body = self.body.as_ref();
        body.windows(b"Fatal error".len())
            .any(|window| window == b"Fatal error")
            || body
                .windows(b"Parse error".len())
                .any(|window| window == b"Parse error")
            || body
                .windows(b"Compile Error".len())
                .any(|window| window == b"Compile Error")
    }

    /// Snapshot the headers + status into a separate, owned tuple so
    /// the streaming sink can publish them via the headers oneshot
    /// without consuming the buffered output (we never mutate again
    /// after commit, but this keeps ownership clean).
    fn snapshot_head(&self) -> (StatusCode, HeaderMap) {
        let mut headers = self.headers.clone();
        // A message to the proxy, not the client (nginx drops it too).
        headers.remove(X_ACCEL_BUFFERING);
        (self.status, headers)
    }

    /// The length the script declared with `header('Content-Length: …')`.
    fn declared_length(&self) -> Option<u64> {
        self.headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse().ok())
    }
}

// ---------------------------------------------------------------------
// Streaming body sink — every reply goes through it.
// ---------------------------------------------------------------------

/// Streaming response body channel capacity. Sized SMALL on purpose:
/// at cap=2 the second `ub_write` blocks the moment the receiver
/// stops draining (browser tab closed, refresh leaked the stream,
/// proxy went away), giving the write-deadline guard immediate
/// feedback. SSE / NDJSON workloads pace at ≤1 msg/sec — pipelining
/// beyond two frames buys no real throughput, only delays
/// stuck-stream detection.
pub(in crate::engine) const STREAM_BODY_CAPACITY: usize = 2;

/// Where PHP body bytes go. The headers + status always land in
/// `ClavironContext.output: CapturedResponse` regardless of mode —
/// only the body destination differs. Buffered keeps the bytes in
/// `CapturedResponse.body` and ships them at PHP exit; Streaming
/// publishes status/headers via a oneshot on first body byte and
/// pushes each `ub_write`'s slice through an mpsc the caller has
/// already wrapped into a `RouteBody`.
pub(crate) enum BodySink {
    /// Body bytes flow through the mpsc immediately; the caller's
    /// `Response<RouteBody>` is built from the headers oneshot well
    /// before PHP returns. The only path that supports SSE,
    /// long-lived chunked responses, and any workload where the PHP
    /// script's lifetime exceeds the desired time-to-first-byte.
    Streaming(StreamingSink),
}

/// Where a reply stands. Every reply starts undecided; its first byte (or
/// a `flush()`, or the end) settles it.
enum SinkState {
    /// No body byte yet.
    Undecided,
    /// Held in `CapturedResponse.body`, up to `limit` when there is one.
    /// Held to its end, it goes back whole with the job's reply — status,
    /// fatal-error rewrite and length all still open.
    Holding { limit: Option<usize> },
    /// The head is out and each write goes to the client as it comes.
    Streaming,
}

pub(crate) struct StreamingSink {
    /// How this reply is handed back: the edge's reply policy for this
    /// request. Read once, at the first byte.
    policy: ResponsePolicy,
    state: Mutex<SinkState>,
    /// Single-shot publish of (status, headers) to the response builder.
    /// Wrapped in `Mutex<Option<…>>` because PHP can call `send_headers`
    /// / `flush` / first `ub_write` multiple times — only the first
    /// wins; subsequent commits are silent no-ops.
    headers_tx: Mutex<Option<oneshot::Sender<(StatusCode, HeaderMap)>>>,
    /// Set on the same edge as `headers_tx.take()` so the streaming
    /// `ub_write` can answer "headers already published?" without
    /// re-locking. Acquire/Release pairs with the lock release.
    headers_committed: AtomicBool,
    /// Body byte channel. Each `ub_write` slice → one frame. Receiver
    /// is wrapped in a `StreamBody` on the caller side; `send_*`
    /// failures (Closed) mean the client / response body was dropped,
    /// at which point we trigger PHP's connection-aborted bailout.
    body_tx: mpsc::Sender<Bytes>,
    /// Per-request write deadline. If the channel has no room within this
    /// window we conclude the receiver (HTTP transport / client) is stalled
    /// and abort the request; `None` waits as long as the client does.
    /// Pinned at request build time from
    /// the route's `responses.write_timeout`.
    write_deadline: Option<Duration>,
    /// The runtime a full channel is waited on from the PHP thread, which is
    /// not one of its own.
    runtime: tokio::runtime::Handle,
    /// This pool generation's drain, which ends that wait.
    shutdown: tokio_util::sync::CancellationToken,
}

impl StreamingSink {
    pub(in crate::engine) fn new(
        headers_tx: oneshot::Sender<(StatusCode, HeaderMap)>,
        body_tx: mpsc::Sender<Bytes>,
        write_deadline: Option<Duration>,
        policy: ResponsePolicy,
    ) -> Self {
        Self {
            policy,
            state: Mutex::new(SinkState::Undecided),
            headers_tx: Mutex::new(Some(headers_tx)),
            headers_committed: AtomicBool::new(false),
            body_tx,
            write_deadline,
            // Built on the request's task, so there is a runtime to name.
            runtime: tokio::runtime::Handle::current(),
            shutdown: crate::engine::sapi_shutdown_signal(),
        }
    }

    /// Commit the captured headers + status to the headers oneshot,
    /// idempotently. Called from `ub_write` (right before the first
    /// body byte goes out) and from `finish_request` (in case PHP
    /// returns without ever writing a body — caller still needs a
    /// response). Concurrent callers race the lock; first one wins.
    pub(in crate::engine) fn commit_headers(
        &self,
        captured: &Mutex<CapturedResponse>,
    ) {
        if self.headers_committed.load(Ordering::Acquire) {
            return;
        }
        let Ok(mut slot) = self.headers_tx.lock() else { return };
        let Some(tx) = slot.take() else {
            self.headers_committed.store(true, Ordering::Release);
            return;
        };
        let head = match captured.lock() {
            Ok(c) => c.snapshot_head(),
            Err(_) => (StatusCode::OK, HeaderMap::new()),
        };
        let _ = tx.send(head);
        self.headers_committed.store(true, Ordering::Release);
    }

    /// One `ub_write`. The first byte decides, from the reply's head, whether
    /// it streams or is held ([`ResponsePolicy::delivery`]): the script's
    /// `X-Accel-Buffering`, then `text/event-stream`, then the policy. A
    /// held reply turns into a stream when it outgrows its limit; what was
    /// held goes out first. `false` means the client is gone.
    pub(in crate::engine) fn write(
        &self,
        output: &Mutex<CapturedResponse>,
        data: &[u8],
    ) -> bool {
        let Ok(mut state) = self.state.lock() else { return false };
        if matches!(*state, SinkState::Undecided) {
            *state = match output.lock() {
                Ok(captured) => {
                    match self
                        .policy
                        .delivery(&captured.headers, captured.declared_length())
                    {
                        Delivery::Whole { up_to } => {
                            SinkState::Holding { limit: up_to }
                        }
                        Delivery::Pieces => SinkState::Streaming,
                    }
                }
                Err(_) => SinkState::Streaming,
            };
        }
        if let SinkState::Holding { limit } = *state {
            let Ok(mut captured) = output.lock() else { return false };
            if limit
                .is_none_or(|limit| captured.body.len() + data.len() <= limit)
            {
                captured.body.extend_from_slice(data);
                return true;
            }
            // Outgrown: the head and what was held go out, then this piece.
            let held = captured.body.split().freeze();
            drop(captured);
            *state = SinkState::Streaming;
            drop(state);
            self.commit_headers(output);
            if !held.is_empty() && !self.push_body(held) {
                return false;
            }
            return self.push_body(Bytes::copy_from_slice(data));
        }
        drop(state);
        // Headers MUST go out before any body byte. The first write triggers
        // commit; later calls are idempotent no-ops via the AtomicBool.
        self.commit_headers(output);
        self.push_body(Bytes::copy_from_slice(data))
    }

    /// The script's own `flush()`: it wants what it wrote to reach the
    /// client now, which a held reply cannot do — so it becomes a stream,
    /// and a reply with no byte yet commits its head (the SSE opening).
    pub(in crate::engine) fn flush(
        &self,
        output: &Mutex<CapturedResponse>,
    ) -> bool {
        let Ok(mut state) = self.state.lock() else { return false };
        if matches!(*state, SinkState::Streaming) {
            return true;
        }
        *state = SinkState::Streaming;
        drop(state);
        let held = match output.lock() {
            Ok(mut captured) => captured.body.split().freeze(),
            Err(_) => return false,
        };
        self.commit_headers(output);
        held.is_empty() || self.push_body(held)
    }

    /// The script ended. A stream has its head out already (or commits it
    /// now, for a body-less reply that chose to stream); a held reply — or
    /// one that never wrote a byte — is left for the job's reply to carry
    /// whole, after the fatal-error check has had its say.
    pub(in crate::engine) fn finish(
        &self,
        output: &Mutex<CapturedResponse>,
    ) {
        let streaming = self
            .state
            .lock()
            .is_ok_and(|state| matches!(*state, SinkState::Streaming));
        if streaming {
            self.commit_headers(output);
        }
    }

    /// Push body bytes through the mpsc, bounded by `write_deadline`.
    /// Returns `true` on successful send, `false` on Closed (receiver
    /// gone), deadline expiry (transport stalled) or the pool draining. The
    /// caller maps `false` → `ub_write` 0 bytes →
    /// `php_handle_aborted_connection`.
    ///
    /// Why a deadline and not a bare `blocking_send`: that waits forever
    /// when the channel is full but the receiver is still alive — "browser
    /// tab abandoned the stream but H2/H3 transport hasn't noticed yet", a
    /// real production hazard. The wait itself is a wake, not a poll: room
    /// in the channel, the deadline or the drain, whichever comes first.
    pub(in crate::engine) fn push_body(
        &self,
        bytes: Bytes,
    ) -> bool {
        if SAPI_SHUTTING_DOWN.load(Ordering::Acquire) {
            return false;
        }
        // Room already: no need to park the thread.
        let bytes = match self.body_tx.try_send(bytes) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            Err(mpsc::error::TrySendError::Full(bytes)) => bytes,
        };
        let deadline = self.write_deadline;
        self.runtime.block_on(async {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => false,
                sent = self.body_tx.send(bytes) => sent.is_ok(),
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep(deadline).await,
                        None => std::future::pending().await
                    }
                } => false,
            }
        })
    }
}

/// RAII guard that fires a tracing event when the transport drops the
/// streaming response body before PHP finished it — the client left. A
/// body read to its end is dropped too, as every body is, and says
/// nothing; `finished` tells the two apart. Diagnostic for verifying that
/// a disconnect reaches `body_rx`: if a client leaves and this does not
/// log, hyper / axum / quinn is holding the body alive past stream-close
/// and the SAPI thread will leak.
struct StreamingBodyDropGuard {
    finished: bool,
}

impl StreamingBodyDropGuard {
    /// The body was read to its end; dropping it now is not a disconnect.
    fn finish(mut self) {
        self.finished = true;
    }
}

impl Drop for StreamingBodyDropGuard {
    fn drop(&mut self) {
        if !self.finished {
            debug!(
                "streaming response body dropped before its end (client gone → body_rx drop)"
            );
        }
    }
}

/// Wrap an `mpsc::Receiver<Bytes>` as a `RouteBody`. Each received
/// chunk becomes one data frame; receiver close → end-of-body. Mirror
/// of `claviron-php-fcgi::service::body_from_receiver` — kept private
/// here so the embed crate has no FCGI dependency.
///
/// The drop guard captured in the unfold state confirms when the
/// transport releases a body early. body_tx will see `Closed`
/// from the moment the guard logs.
pub(in crate::engine) fn body_from_receiver(
    rx: mpsc::Receiver<Bytes>
) -> RouteBody {
    let guard = StreamingBodyDropGuard { finished: false };
    let s = stream::unfold((rx, guard), |(mut rx, guard)| async move {
        match rx.recv().await {
            Some(chunk) => Some((
                Ok::<_, std::convert::Infallible>(Frame::data(chunk)),
                (rx, guard),
            )),
            None => {
                guard.finish();
                None
            }
        }
    });
    boxed_body(StreamBody::new(s))
}
