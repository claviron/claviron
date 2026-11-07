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
use std::io;

use bytes::Bytes;
use claviron_http::RouteResponse as AppResponse;
pub use claviron_http::socket::{Carrier, OfferInfo, SocketKind};
use claviron_http::socket::{Offer, SocketIo};
pub use claviron_orbit::link::application::Message;
use fastwebsockets::{FragmentCollector, Frame, OpCode, Payload};
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;

use crate::orbit::LinkSession;
use crate::owner::AppRequest;

mod link;

/// The largest text or binary message either way, unless an owner says otherwise.
pub const DEFAULT_MESSAGE_LIMIT: usize = 1024 * 1024;

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

/// The session this request attempts to open, if it does. `None` is an ordinary
/// request.
pub fn attempt(request: &AppRequest) -> Option<OfferInfo> {
    request.extensions().get::<Offer>().map(Offer::info)
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
/// `limit` bounds a text or binary message on any door.
pub fn accept<D: Deliver>(
    request: AppRequest,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    inbound: D,
    mut outbound: mpsc::Receiver<Message>,
    limit: usize,
) -> AppResponse {
    let inbound = Ending(inbound);
    let Some(claim) =
        request.extensions().get::<Offer>().and_then(Offer::claim)
    else {
        // Only an attempt a door recognized can be accepted, and only once.
        return status(StatusCode::BAD_REQUEST);
    };
    let mut proposal = claim.accept_with(move |io| async move {
        match opened(io, limit) {
            Some(Opened::Socket(socket)) => {
                pump(socket, inbound, outbound).await;
            }
            Some(Opened::Link(session)) => {
                let LinkSession { mut incoming, mut outgoing, shutdown } =
                    session;
                link::relay(
                    &mut incoming,
                    &mut outgoing,
                    &inbound.0,
                    &mut outbound,
                    limit,
                    shutdown.cancelled(),
                )
                .await;
            }
            None => {}
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

/// A committed session's I/O, as this module speaks it.
enum Opened {
    Socket(Socket),
    Link(LinkSession),
}

fn opened(
    io: SocketIo,
    limit: usize,
) -> Option<Opened> {
    match io {
        SocketIo::Stream(upgraded) => {
            let mut socket = fastwebsockets::WebSocket::after_handshake(
                TokioIo::new(upgraded),
                fastwebsockets::Role::Server,
            );
            socket.set_max_message_size(limit);
            Some(Opened::Socket(socket))
        }
        SocketIo::Native(native) => match native.downcast::<LinkSession>() {
            Ok(session) => Some(Opened::Link(*session)),
            Err(_) => {
                tracing::debug!(
                    "a WebSocket has no use for this session's I/O"
                );
                None
            }
        },
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

/// A WebSocket for an owner that holds the messages itself, whichever door it
/// came through. Fragments are joined, pings answered and closes echoed before
/// it gets here.
pub struct WebSocket {
    io: Messages,
}

enum Messages {
    Socket(FragmentCollector<TokioIo<Upgraded>>),
    Link(link::LinkMessages),
}

impl WebSocket {
    fn link(messages: link::LinkMessages) -> Self {
        Self { io: Messages::Link(messages) }
    }

    /// The next message. `None` once the connection has ended. A close the
    /// client did not send (1001) says this process is retiring: finish what is
    /// owed and return.
    pub async fn recv(&mut self) -> Option<Message> {
        let socket = match &mut self.io {
            Messages::Socket(socket) => socket,
            Messages::Link(messages) => return messages.recv().await,
        };
        loop {
            let Ok(frame) = socket.read_frame().await else {
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
    ) -> io::Result<()> {
        let socket = match &mut self.io {
            Messages::Socket(socket) => socket,
            Messages::Link(messages) => return messages.send(message).await,
        };
        socket
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
            .map_err(io::Error::other)
    }
}

/// Propose a WebSocket for an owner that holds the messages itself: `session` runs
/// with the socket once the handshake is committed. `limit` bounds a text or binary
/// message on any door.
pub fn accept_socket<F, Fut>(
    offer: &Offer,
    limit: usize,
    session: F,
) -> AppResponse
where
    F: FnOnce(WebSocket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let Some(claim) = offer.claim() else {
        return status(StatusCode::BAD_REQUEST);
    };
    claim.accept_with(move |io| async move {
        match opened(io, limit) {
            Some(Opened::Socket(mut socket)) => {
                socket.set_auto_pong(true);
                socket.set_auto_close(true);
                session(WebSocket {
                    io: Messages::Socket(FragmentCollector::new(socket)),
                })
                .await;
            }
            Some(Opened::Link(link)) => {
                link::hold(link, limit, session).await;
            }
            None => {}
        }
    })
}

/// The offer of a WebSocket, for a handler in a router. Taking it from a request
/// that is not an attempt at one answers `426`. Route it with `any`: an HTTP/2
/// WebSocket is a CONNECT. A WebTransport stream an edge carries over Orbit is the
/// same whole messages and arrives here too; a native WebTransport session is not
/// one and does not.
#[cfg(feature = "axum")]
pub struct WebSocketOffer {
    offer: Offer,
    limit: usize,
}

#[cfg(feature = "axum")]
impl WebSocketOffer {
    /// Bound a text or binary message on any door, in place of
    /// [`DEFAULT_MESSAGE_LIMIT`].
    pub fn limit(
        mut self,
        limit: usize,
    ) -> Self {
        self.limit = limit;
        self
    }

    /// Accept: `session` runs with the socket once the handshake is committed.
    pub fn accept<F, Fut>(
        self,
        session: F,
    ) -> axum::response::Response
    where
        F: FnOnce(WebSocket) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let Self { offer, limit } = self;
        claviron_http::into_axum_response(accept_socket(&offer, limit, session))
    }
}

#[cfg(feature = "axum")]
impl<S: Send + Sync> axum::extract::FromRequestParts<S> for WebSocketOffer {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        _: &S,
    ) -> Result<Self, Self::Rejection> {
        let offer = parts
            .extensions
            .get::<Offer>()
            .filter(|offer| {
                let OfferInfo { kind, carrier } = offer.info();
                kind == SocketKind::WebSocket || carrier == Carrier::Orbit
            })
            .cloned()
            .ok_or(StatusCode::UPGRADE_REQUIRED)?;
        Ok(Self { offer, limit: DEFAULT_MESSAGE_LIMIT })
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
