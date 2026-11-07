//! An owner's WebSocket on a listener's own sockets.
//!
//! Over Orbit the edge speaks the WebSocket protocol and the owner sees whole
//! messages; on the sockets this speaks it, with `fastwebsockets`,
//! and hands the owner the same [`Message`]s — fragments joined, pings
//! answered, closes echoed here — so an owner never knows which door a socket
//! came through. What the owner decides is whether to accept, and what it
//! does with the messages; the protocol is this.
//!
//! The socket's session belongs to the connection that asked for it: the door
//! hands it to the listener's session owner before the handshake is answered,
//! and the listener joins it on shutdown rather than leaving it detached.

use std::future::Future;

use bytes::Bytes;
use claviron_http::RouteResponse as AppResponse;
use claviron_http::socket::{Offer, SocketIo};
pub use claviron_orbit::link::application::Message;
use fastwebsockets::{FragmentCollector, Frame, OpCode, Payload};
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;

use crate::owner::AppRequest;

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

/// Whether a request is an attempt at a WebSocket.
pub fn is_websocket(request: &AppRequest) -> bool {
    request.extensions().get::<Offer>().is_some()
}

/// What a socket does with its owner's side, whichever way the session ends.
struct Ending<D: Deliver>(D);

impl<D: Deliver> Drop for Ending<D> {
    fn drop(&mut self) {
        self.0.end();
    }
}

/// Propose an accepted WebSocket with the owner's `headers`. Once the door has
/// committed the handshake, messages travel between the client and the owner
/// until either side closes: the client's to `inbound`, the owner's from
/// `outbound`. If the proposal is cancelled instead, `inbound` is told it ended.
pub fn accept<D: Deliver>(
    request: AppRequest,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    inbound: D,
    outbound: mpsc::Receiver<Message>,
) -> AppResponse {
    let inbound = Ending(inbound);
    let Some(claim) =
        request.extensions().get::<Offer>().and_then(Offer::claim)
    else {
        // Only an attempt a door recognized can be accepted, and only once.
        return status(StatusCode::BAD_REQUEST);
    };
    let mut proposal = claim.accept_with(move |io| async move {
        if let Some(socket) = opened(io) {
            pump(socket, inbound, outbound).await;
        }
    });
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(&name),
            http::HeaderValue::from_bytes(&value),
        ) {
            proposal.headers_mut().append(name, value);
        }
    }
    proposal
}

type Socket = fastwebsockets::WebSocket<TokioIo<Upgraded>>;

/// The server's side of a handshake the door has committed.
fn opened(io: SocketIo) -> Option<Socket> {
    match io {
        SocketIo::Stream(upgraded) => {
            Some(fastwebsockets::WebSocket::after_handshake(
                TokioIo::new(upgraded),
                fastwebsockets::Role::Server,
            ))
        }
        SocketIo::Native(_) => {
            tracing::debug!("a WebSocket needs a byte stream");
            None
        }
    }
}

async fn pump<D: Deliver>(
    mut socket: Socket,
    inbound: Ending<D>,
    mut outbound: mpsc::Receiver<Message>,
) {
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
                if !inbound.0.deliver(message).await || closing {
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
}

/// A WebSocket for an owner that holds the messages itself. Fragments are
/// joined, pings answered and closes echoed here.
pub struct WebSocket {
    socket: FragmentCollector<TokioIo<Upgraded>>,
}

impl WebSocket {
    /// The next message. `None` once the connection has ended.
    pub async fn recv(&mut self) -> Option<Message> {
        loop {
            let Ok(frame) = self.socket.read_frame().await else {
                return None;
            };
            return Some(match frame.opcode {
                OpCode::Text => match String::from_utf8(frame.payload.to_vec())
                {
                    Ok(text) => Message::Text(text),
                    Err(_) => {
                        Message::Close { code: 1007, reason: String::new() }
                    }
                },
                OpCode::Binary => Message::Binary(frame.payload.to_vec()),
                OpCode::Close => {
                    let (code, reason) = close_details(&frame.payload);
                    Message::Close { code, reason }
                }
                _ => continue,
            });
        }
    }

    pub async fn send(
        &mut self,
        message: Message,
    ) -> Result<(), fastwebsockets::WebSocketError> {
        self.socket
            .write_frame(match &message {
                Message::Text(text) => {
                    Frame::text(Payload::Borrowed(text.as_bytes()))
                }
                Message::Binary(bytes) => {
                    Frame::binary(Payload::Borrowed(bytes))
                }
                Message::Close { code, reason } => {
                    Frame::close(*code, reason.as_bytes())
                }
            })
            .await
    }
}

/// The offer of a WebSocket, for a handler in a router. Taking it from a request
/// that is not an attempt at one answers `426`.
#[cfg(feature = "axum")]
pub struct WebSocketOffer(Offer);

#[cfg(feature = "axum")]
impl WebSocketOffer {
    /// Accept: `session` runs with the socket once the handshake is committed.
    pub fn accept<F, Fut>(
        self,
        session: F,
    ) -> axum::response::Response
    where
        F: FnOnce(WebSocket) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let Some(claim) = self.0.claim() else {
            return claviron_http::into_axum_response(status(
                StatusCode::BAD_REQUEST,
            ));
        };
        claviron_http::into_axum_response(claim.accept_with(
            move |io| async move {
                if let Some(mut socket) = opened(io) {
                    socket.set_auto_pong(true);
                    socket.set_auto_close(true);
                    session(WebSocket {
                        socket: FragmentCollector::new(socket),
                    })
                    .await;
                }
            },
        ))
    }
}

#[cfg(feature = "axum")]
impl<S: Send + Sync> axum::extract::FromRequestParts<S> for WebSocketOffer {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        _: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Offer>()
            .cloned()
            .map(Self)
            .ok_or(StatusCode::UPGRADE_REQUIRED)
    }
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
