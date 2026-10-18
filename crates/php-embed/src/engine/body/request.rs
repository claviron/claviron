//! Buffered and streaming HTTP-to-PHP body input.

use std::ffi::c_char;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use claviron_http::RouteBody;
use http::HeaderMap;
use http::header::CONTENT_LENGTH;
use http_body_util::BodyExt;
use tokio::sync::mpsc;
use tracing::debug;

use crate::engine::SAPI_SHUTTING_DOWN;

/// Request streaming channel capacity. Kept small for the same reason
/// as response streaming: PHP's blocking `read_post` should apply
/// backpressure to the HTTP body pump instead of letting large uploads
/// accumulate in memory.
const REQUEST_BODY_STREAM_CAPACITY: usize = 2;

// ---------------------------------------------------------------------
// Request body source — buffered for short requests, streamed for
// upload/large-body paths. PHP's SAPI read side is synchronous, so the
// streaming variant bridges async `RouteBody::frame()` into a bounded
// channel and `read_post` drains that channel from the PHP thread.
// ---------------------------------------------------------------------

pub(crate) enum RequestBodySource {
    Buffered { body: Bytes, pos: usize },
    Streaming(StreamingRequestBody),
}

impl RequestBodySource {
    pub(in crate::engine) fn buffered(body: Bytes) -> Self {
        Self::Buffered { body, pos: 0 }
    }

    pub(in crate::engine) fn streaming(
        body: RouteBody,
        max_bytes: Option<usize>,
    ) -> Self {
        Self::Streaming(StreamingRequestBody::new(body, max_bytes))
    }

    pub(crate) fn content_length(
        &self,
        headers: &HeaderMap,
    ) -> i64 {
        match self {
            RequestBodySource::Buffered { body, .. } => {
                body.len().try_into().unwrap_or(i64::MAX)
            }
            RequestBodySource::Streaming(_) => headers
                .get(CONTENT_LENGTH)
                .and_then(|value| parse_content_length(value.as_bytes()))
                .and_then(|len| i64::try_from(len).ok())
                .unwrap_or(0),
        }
    }

    pub(in crate::engine) fn read_post(
        &mut self,
        buffer: *mut c_char,
        to_read: usize,
    ) -> usize {
        match self {
            RequestBodySource::Buffered { body, pos } => {
                if buffer.is_null() || *pos >= body.len() {
                    return 0;
                }

                let available = body.len() - *pos;
                let read_len = available.min(to_read);
                let source = &body[*pos..*pos + read_len];
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        source.as_ptr().cast::<c_char>(),
                        buffer,
                        read_len,
                    );
                }
                *pos += read_len;
                read_len
            }
            RequestBodySource::Streaming(stream) => {
                stream.read_post(buffer, to_read)
            }
        }
    }

    #[cfg(feature = "metrics")]
    pub(in crate::engine) fn buffered_len(&self) -> Option<usize> {
        match self {
            RequestBodySource::Buffered { body, .. } => Some(body.len()),
            RequestBodySource::Streaming(_) => None,
        }
    }
}

pub(crate) struct StreamingRequestBody {
    rx: mpsc::Receiver<Result<Bytes, String>>,
    current: Bytes,
    current_pos: usize,
    done: bool,
}

impl StreamingRequestBody {
    fn new(
        mut body: RouteBody,
        max_bytes: Option<usize>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<Result<Bytes, String>>(
            REQUEST_BODY_STREAM_CAPACITY,
        );
        // Every wait here ends by being woken — a piece arriving, room in
        // the channel, the reader gone, or the pool draining — never by a
        // timer checking the flag.
        let shutdown = crate::engine::sapi_shutdown_signal();

        tokio::spawn(async move {
            let mut total = 0_usize;
            loop {
                let frame = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        let _ = tx.try_send(Err("request body stream stopped during shutdown".into()));
                        break;
                    }
                    frame = body.frame() => frame,
                };

                let Some(frame) = frame else { break };
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(err) => {
                        let _ = tx
                            .send(Err(format!(
                                "request body read error: {err:?}"
                            )))
                            .await;
                        break;
                    }
                };

                let Ok(data) = frame.into_data() else { continue };
                if let Some(limit) = max_bytes {
                    total = match total.checked_add(data.len()) {
                        Some(total) if total <= limit => total,
                        _ => {
                            let _ = tx
                                .send(Err("request body too large".into()))
                                .await;
                            break;
                        }
                    };
                }
                // Waits for room while PHP reads, which is the backpressure
                // that keeps an upload out of memory.
                let sent = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        let _ = tx.try_send(Err("request body stream stopped during shutdown".into()));
                        break;
                    }
                    sent = tx.send(Ok(data)) => sent,
                };
                if sent.is_err() {
                    // PHP stopped reading: the request is over.
                    break;
                }
            }
        });

        Self { rx, current: Bytes::new(), current_pos: 0, done: false }
    }

    fn read_post(
        &mut self,
        buffer: *mut c_char,
        to_read: usize,
    ) -> usize {
        if buffer.is_null() || to_read == 0 || self.done {
            return 0;
        }

        let mut written = 0;
        while written < to_read {
            if self.current_pos >= self.current.len() && !self.load_next_chunk()
            {
                break;
            }

            let available = self.current.len().saturating_sub(self.current_pos);
            if available == 0 {
                break;
            }

            let copy_len = available.min(to_read - written);
            let source =
                &self.current[self.current_pos..self.current_pos + copy_len];
            unsafe {
                std::ptr::copy_nonoverlapping(
                    source.as_ptr().cast::<c_char>(),
                    buffer.add(written),
                    copy_len,
                );
            }

            self.current_pos += copy_len;
            written += copy_len;
        }

        written
    }

    /// Park the PHP thread until the next piece, the end, or an error. The
    /// pump sends an error on shutdown, so a drain wakes this too.
    fn load_next_chunk(&mut self) -> bool {
        loop {
            if SAPI_SHUTTING_DOWN.load(Ordering::Acquire) {
                self.done = true;
                return false;
            }

            match self.rx.blocking_recv() {
                Some(Ok(chunk)) if chunk.is_empty() => continue,
                Some(Ok(chunk)) => {
                    self.current = chunk;
                    self.current_pos = 0;
                    return true;
                }
                Some(Err(err)) => {
                    debug!(error = %err, "php sapi request body stream ended with error");
                    crate::sapi::handle_abort_connection();
                    self.done = true;
                    return false;
                }
                None => {
                    self.done = true;
                    return false;
                }
            }
        }
    }
}

pub(in crate::engine) async fn collect_body(
    mut body: RouteBody,
    max_bytes: Option<usize>,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let mut buffered = bytes::BytesMut::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame?.into_data() {
            if let Some(limit) = max_bytes
                && buffered.len().saturating_add(data.len()) > limit
            {
                return Err(Box::new(
                    claviron_http::body::BodyLimit::TooLarge { limit },
                ));
            }
            buffered.extend_from_slice(&data);
        }
    }
    Ok(buffered.freeze())
}

fn parse_content_length(value: &[u8]) -> Option<u64> {
    let value = trim_ascii(value);
    if value.is_empty() {
        return None;
    }

    let mut n = 0_u64;
    for &b in value {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?;
        n = n.checked_add(u64::from(b - b'0'))?;
    }

    Some(n)
}

fn trim_ascii(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|idx| idx + 1)
        .unwrap_or(start);

    &value[start..end]
}
