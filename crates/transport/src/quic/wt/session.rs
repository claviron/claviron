//! One WebTransport session over an HTTP/3 connection.

use std::ops::Deref;

use bytes::Bytes;
use h3_webtransport::server::WebTransportSession;

type NativeSession = WebTransportSession<h3_quinn::Connection, Bytes>;

/// A reliable incoming WebTransport bidirectional stream.
pub type H3WebTransportBiStream =
    h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>;

// WebTransport's protocol-defined WT_SESSION_GONE code identifies streams
// whose parent session has ended; changing it would break wire compatibility.
const WT_SESSION_GONE: u64 = 0x170d7b68;

/// Whether a data-stream I/O error reports that its WT session has ended.
///
/// Inspect the typed transport cause rather than its display text or I/O kind.
/// Other stream reset codes, connection errors and unrelated I/O errors return
/// false, so consumers can retain their normal error reporting for failures.
pub fn webtransport_session_ended(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .and_then(|cause| {
            cause.downcast_ref::<h3_webtransport::error::StreamError>()
        })
        .and_then(|error| error.http3_code())
        == Some(WT_SESSION_GONE)
}

/// One application session, with input routed by the native connection owner.
///
/// The H3 connection, ordinary HTTP requests and other sessions stay with the
/// listener. Input is bounded by the listener's configured QUIC bidi limit.
/// Native outgoing stream and datagram operations remain available through
/// `Deref`; receiving a bidi stream never drives the HTTP/3 connection.
pub struct H3WebTransportSession {
    native: NativeSession,
    incoming_capacity: std::num::NonZeroUsize,
    incoming:
        tokio::sync::Mutex<tokio::sync::mpsc::Receiver<H3WebTransportBiStream>>,
}

impl H3WebTransportSession {
    /// Wrap an accepted session with the input its connection driver routes
    /// to it.
    pub fn new(
        native: NativeSession,
        incoming: tokio::sync::mpsc::Receiver<H3WebTransportBiStream>,
    ) -> Self {
        let incoming_capacity =
            std::num::NonZeroUsize::new(incoming.max_capacity())
                .expect("bounded session receiver has positive capacity");
        Self {
            native,
            incoming_capacity,
            incoming: tokio::sync::Mutex::new(incoming),
        }
    }

    /// The pending input bound captured from the listener's QUIC bidi limit.
    /// Session consumers can reuse it to bound concurrently retained handlers.
    pub fn incoming_capacity(&self) -> std::num::NonZeroUsize {
        self.incoming_capacity
    }

    /// Receive this session's next bidi stream, or `None` after its driver ends.
    /// Classification has already completed in the connection driver; an
    /// ordinary HTTP request cannot escape into a session handler.
    pub async fn accept_bi(&self) -> Option<H3WebTransportBiStream> {
        self.incoming.lock().await.recv().await
    }
}

impl Deref for H3WebTransportSession {
    type Target = NativeSession;

    fn deref(&self) -> &Self::Target {
        &self.native
    }
}
