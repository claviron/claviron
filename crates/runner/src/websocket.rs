//! An owner's WebSocket on the runner's own sockets.
//!
//! Over Orbit the edge speaks the WebSocket protocol and the owner sees whole
//! messages; on the runner's sockets this speaks it, with `fastwebsockets`,
//! and hands the owner the same [`Message`]s — fragments joined, pings
//! answered, closes echoed here — so an owner never knows which door a socket
//! came through. What the owner decides is whether to accept, and what it
//! does with the messages; the protocol is this.
//!
//! The socket's session belongs to the connection that asked for it: it is
//! handed to the listener's session owner before the 101 goes, and the
//! listener joins it on shutdown rather than leaving it detached.

use std::future::Future;

use bytes::Bytes;
use claviron_http::session::SessionSpawner;
pub use claviron_orbit::transport::Message;
use fastwebsockets::upgrade::UpgradeFut;
use fastwebsockets::{FragmentCollector, Frame, OpCode, Payload};
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::mpsc;

use crate::routing::{AppRequest, AppResponse};

/// Where the client's messages go: the owner's side of the socket.
pub trait Deliver: Send + Sync + 'static {
    /// Hand one message over; `false` once the owner is gone.
    fn deliver(
        &self,
        message: Message,
    ) -> impl Future<Output = bool> + Send;

    /// The client is gone.
    fn end(&self);
}

/// Whether a request asks to become a WebSocket.
pub fn is_websocket(request: &AppRequest) -> bool {
    fastwebsockets::upgrade::is_upgrade_request(request)
}

/// Answer an accepted WebSocket's handshake with the owner's `headers`, and
/// carry messages between the client and the owner until either side closes:
/// the client's to `inbound`, the owner's from `outbound`.
pub fn accept<D: Deliver>(
    mut request: AppRequest,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    inbound: D,
    outbound: mpsc::Receiver<Message>,
) -> AppResponse {
    let Some(sessions) = request.extensions().get::<SessionSpawner>().cloned()
    else {
        // Only a connection that can hold a session may start one.
        inbound.end();
        return status(StatusCode::SERVICE_UNAVAILABLE);
    };
    let (response, upgrade) =
        match fastwebsockets::upgrade::upgrade(&mut request) {
            Ok(upgrade) => upgrade,
            Err(error) => {
                tracing::debug!("WebSocket handshake refused: {error}");
                inbound.end();
                return status(StatusCode::BAD_REQUEST);
            }
        };
    let mut response = response.map(|_| empty());
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(&name),
            http::HeaderValue::from_bytes(&value),
        ) {
            response.headers_mut().append(name, value);
        }
    }
    if let Err(error) = sessions.spawn(pump(upgrade, inbound, outbound)) {
        tracing::debug!("WebSocket session not started: {error}");
        return status(StatusCode::SERVICE_UNAVAILABLE);
    }
    response
}

async fn pump<D: Deliver>(
    upgrade: UpgradeFut,
    inbound: D,
    mut outbound: mpsc::Receiver<Message>,
) {
    let mut socket = match upgrade.await {
        Ok(socket) => socket,
        Err(error) => {
            tracing::debug!("WebSocket upgrade failed: {error}");
            inbound.end();
            return;
        }
    };
    socket.set_auto_pong(true);
    socket.set_auto_close(true);
    let mut socket = FragmentCollector::new(socket);
    loop {
        tokio::select! {
            frame = socket.read_frame() => {
                let message = match frame {
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
                    Err(_) => Message::Close { code: 1006, reason: String::new() }
                };
                let closing = matches!(message, Message::Close { .. });
                if !inbound.deliver(message).await || closing {
                    break;
                }
            }
            message = outbound.recv() => {
                let sent = match message {
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
    inbound.end();
}

/// A close frame's code and reason; 1005 when it carries none.
fn close_details(payload: &[u8]) -> (u16, String) {
    if payload.len() < 2 {
        return (1005, String::new());
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    (code, String::from_utf8_lossy(&payload[2..]).into_owned())
}

fn empty() -> claviron_http::RouteBody {
    http_body_util::Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed_unsync()
}

fn status(code: StatusCode) -> AppResponse {
    let mut response = Response::new(empty());
    *response.status_mut() = code;
    response
}
