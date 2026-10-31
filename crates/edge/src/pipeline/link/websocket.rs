//! Native WebSocket handshakes around the shared Link message stream.

use std::future::Future;

use claviron_http::{RouteResponse, empty_body, err_status};
use claviron_orbit::transport::client::{LentRead, LentWrite};
use claviron_orbit::transport::{
    ExchangeReply, Message, MessageReader, write_message,
};
use http::Response;
use http::StatusCode;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// A cancelled handshake or dropped listener task must wake its application.
/// A completed FIN remains readable; an abandoned direction becomes RESET.
pub(super) struct Input {
    inner: LentWrite,
    finished: bool,
}

impl Input {
    pub(super) fn new(inner: LentWrite) -> Self {
        Self { inner, finished: false }
    }
    fn reset(&mut self) {
        self.inner.reset();
    }
}

impl Drop for Input {
    fn drop(&mut self) {
        if !self.finished {
            self.inner.reset();
        }
    }
}

impl AsyncWrite for Input {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let result = std::pin::Pin::new(&mut self.inner).poll_shutdown(cx);
        if matches!(result, std::task::Poll::Ready(Ok(()))) {
            self.finished = true;
        }
        result
    }
}

/// HTTP acceptance differs; the application still accepts one message stream.
#[derive(Clone, Debug)]
pub(super) enum Handshake {
    H1(String),
    H2,
}

impl Handshake {
    pub(super) fn from_request<B>(
        request: &http::Request<B>
    ) -> Result<Option<Self>, StatusCode> {
        let h2 = request.version() == http::Version::HTTP_2
            && request.method() == http::Method::CONNECT
            && request
                .extensions()
                .get::<hyper::ext::Protocol>()
                .is_some_and(|protocol| protocol.as_str() == "websocket");
        let h1 =
            claviron_http::upgrade::is_h1_header_upgrade(request, "websocket");
        if !h1 && !h2 {
            return Ok(None);
        }
        if request
            .headers()
            .get(http::header::SEC_WEBSOCKET_VERSION)
            .is_none_or(|version| version != "13")
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        if h2 {
            return Ok(Some(Self::H2));
        }
        let key = request
            .headers()
            .get(http::header::SEC_WEBSOCKET_KEY)
            .and_then(|key| key.to_str().ok())
            .ok_or(StatusCode::BAD_REQUEST)?;
        Ok(Some(Self::H1(key.to_owned())))
    }

    pub(super) fn response(
        &self,
        headers: &[(Vec<u8>, Vec<u8>)],
    ) -> http::response::Builder {
        let mut response = match self {
            Self::H1(key) => Response::builder()
                .status(StatusCode::SWITCHING_PROTOCOLS)
                .header(http::header::UPGRADE, "websocket")
                .header(http::header::CONNECTION, "Upgrade")
                .header(
                    http::header::SEC_WEBSOCKET_ACCEPT,
                    claviron_http::upgrade::websocket_accept_key(key),
                ),
            Self::H2 => Response::builder().status(StatusCode::OK),
        };
        for (name, value) in headers {
            let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(name),
                http::HeaderValue::from_bytes(value),
            ) else {
                continue;
            };
            // The edge owns the wire handshake. Application cookies and the
            // selected subprotocol cross; H1 framing never leaks into H2.
            if matches!(
                name.as_str(),
                "connection"
                    | "upgrade"
                    | "keep-alive"
                    | "proxy-connection"
                    | "transfer-encoding"
                    | "content-length"
                    | "sec-websocket-accept"
            ) {
                continue;
            }
            response = response.header(name, value);
        }
        response
    }
}

/// What an accepted WebSocket needs from the request it arrived as.
pub(super) struct Upgrade {
    pub(super) on_upgrade: Option<hyper::upgrade::OnUpgrade>,
    pub(super) sessions: Option<claviron_http::session::SessionSpawner>,
    pub(super) handshake: Handshake,
}

/// The application said 101: finish the handshake and hand the connection to
/// a session that outlives this request.
///
/// The session is spawned before the 101 goes back, on the owner that joins it
/// at shutdown, as the upgrade crate does for its own routes.
pub(super) fn accept_websocket(
    correlation: u64,
    head: &ExchangeReply,
    upgrade: Upgrade,
    mut to_application: Input,
    from_application: LentRead,
) -> RouteResponse {
    let (Some(on_upgrade), Some(sessions)) =
        (upgrade.on_upgrade, upgrade.sessions)
    else {
        tracing::debug!(
            correlation,
            "accepted a WebSocket this connection cannot upgrade"
        );
        to_application.reset();
        return err_status(StatusCode::BAD_REQUEST);
    };
    let response = upgrade.handshake.response(&head.headers);
    let shutdown = sessions.clone();
    let session = async move {
        tokio::select! {
            biased;
            _ = shutdown.wait_graceful_shutdown() => {
                let mut to_application = to_application;
                let _ = to_application.shutdown().await;
            }
            upgraded = on_upgrade => match upgraded {
                Ok(upgraded) => {
                    serve_websocket(
                        correlation,
                        hyper_util::rt::TokioIo::new(upgraded),
                        to_application,
                        from_application,
                        shutdown.wait_graceful_shutdown(),
                    )
                    .await;
                }
                Err(error) => {
                    to_application.reset();
                    tracing::debug!(correlation, "WebSocket upgrade failed: {error}");
                }
            },
        }
    };
    if let Err(error) = sessions.spawn(session) {
        tracing::debug!(
            correlation,
            "WebSocket session was not started: {error}"
        );
        return err_status(StatusCode::SERVICE_UNAVAILABLE);
    }
    response.body(empty_body()).unwrap_or_else(|error| {
        tracing::debug!(correlation, "101 could not be built: {error}");
        err_status(StatusCode::INTERNAL_SERVER_ERROR)
    })
}

/// Speak the WebSocket protocol with the client, and pass whole messages to
/// and from the application as records on the slot.
///
/// Fragments are joined, pings answered and closes echoed here; the
/// application sees text, binary and the close that ends them. Records from
/// the application are read with a [`MessageReader`], which keeps its place
/// when `select!` drops a read halfway, so both directions are served from
/// this one task.
/// Listener shutdown stops reads, completes a message already being forwarded,
/// and sends a going-away close without awaiting either peer's close reply.
/// It ends this exchange, not the destination application process.
pub(super) async fn serve_websocket<S, W, R>(
    correlation: u64,
    stream: S,
    mut to_application: W,
    mut from_application: R,
    shutdown: impl Future<Output = ()>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    use fastwebsockets::{
        FragmentCollector, Frame, OpCode, Payload, Role, WebSocket,
    };

    let mut socket = WebSocket::after_handshake(stream, Role::Server);
    socket.set_auto_pong(true);
    socket.set_auto_close(true);
    let mut socket = FragmentCollector::new(socket);

    let mut records = MessageReader::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::debug!(correlation, "Orbit WebSocket listener shutting down");
                let _ = socket.write_frame(Frame::close(1001, b"")).await;
                break;
            }
            frame = socket.read_frame() => {
                let inbound = match frame {
                    Ok(frame) => match frame.opcode {
                        OpCode::Text => match String::from_utf8(frame.payload.to_vec()) {
                            Ok(text) => Message::Text(text),
                            Err(_) => Message::Close { code: 1007, reason: String::new() }
                        },
                        OpCode::Binary => Message::Binary(frame.payload.to_vec()),
                        OpCode::Close => {
                            let (code, reason) = close_details(&frame.payload);
                            Message::Close { code, reason }
                        }
                        _ => continue
                    },
                    // The client is gone without a close: 1006 is the code
                    // that says so, and it is never sent on the wire.
                    Err(_) => Message::Close { code: 1006, reason: String::new() }
                };
                let closing = matches!(inbound, Message::Close { .. });
                if write_message(&mut to_application, &inbound).await.is_err() || closing {
                    break;
                }
            }
            message = records.read(&mut from_application) => {
                // A malformed record ends the session as the application's
                // end would.
                let sent = match message.ok().flatten() {
                    Some(Message::Text(text)) => {
                        socket.write_frame(Frame::text(Payload::Owned(text.into_bytes()))).await
                    }
                    Some(Message::Binary(bytes)) => {
                        socket.write_frame(Frame::binary(Payload::Owned(bytes))).await
                    }
                    Some(Message::Close { code, reason }) => {
                        let _ = socket.write_frame(Frame::close(code, reason.as_bytes())).await;
                        break;
                    }
                    // The application ended without a close.
                    None => {
                        let _ = socket.write_frame(Frame::close(1011, b"")).await;
                        break;
                    }
                };
                if sent.is_err() {
                    break;
                }
            }
        }
    }
    // Hyper's H2 upgrade queues DATA through its connection driver. Finish
    // that writer before dropping the stream so the close is not discarded.
    let mut stream = socket.into_inner();
    let _ = stream.shutdown().await;
    let _ = to_application.shutdown().await;
    tracing::debug!(correlation, "WebSocket session ended");
}

/// A close frame's code and reason; 1005 when it carries none.
pub(super) fn close_details(payload: &[u8]) -> (u16, String) {
    if payload.len() < 2 {
        return (1005, String::new());
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    (code, String::from_utf8_lossy(&payload[2..]).into_owned())
}
