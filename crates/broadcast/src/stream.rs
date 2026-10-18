//! Message transports for one Broadcast session. Framing belongs to the adapter;
//! channel authorization, publication and lifecycle belong to the session.
use std::future::Future;

pub use claviron_orbit::transport::Message;
use claviron_orbit::transport::{MessageReader, write_message};
use fastwebsockets::{
    FragmentCollector, Frame, OpCode, Payload, Role, WebSocket,
};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use orbitive::stream::{Endpoint, ReadHalf, WriteHalf};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// The same message limit used by the native WebSocket service.
pub(crate) const MAX_MESSAGE_SIZE: usize = 64 * 1024;

/// One session's messages, whatever carries them.
///
/// Used generically (`S: MessageStream`), never as `dyn`, so the futures are
/// returned as they are rather than boxed: a box per message would be an
/// allocation on the path every WebSocket message takes.
pub trait MessageStream: Send {
    /// Reads must retain partially received messages when cancelled by select.
    fn receive(
        &mut self
    ) -> impl Future<Output = crate::Result<Option<Message>>> + Send;
    fn send(
        &mut self,
        message: Message,
    ) -> impl Future<Output = crate::Result<()>> + Send;
}

pub struct FastWebSocketStream {
    socket: FragmentCollector<TokioIo<Upgraded>>,
}

impl FastWebSocketStream {
    pub fn new(upgraded: Upgraded) -> Self {
        let mut socket =
            WebSocket::after_handshake(TokioIo::new(upgraded), Role::Server);
        socket.set_auto_close(false);
        socket.set_auto_pong(true);
        socket.set_max_message_size(MAX_MESSAGE_SIZE);
        Self { socket: FragmentCollector::new(socket) }
    }
}

impl MessageStream for FastWebSocketStream {
    async fn receive(&mut self) -> crate::Result<Option<Message>> {
        loop {
            let frame = self.socket.read_frame().await?;
            return Ok(Some(match frame.opcode {
                OpCode::Text => Message::Text(
                    String::from_utf8(frame.payload.to_vec()).map_err(
                        |error| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                error,
                            )
                        },
                    )?,
                ),
                OpCode::Binary => Message::Binary(frame.payload.to_vec()),
                OpCode::Close => {
                    let (code, reason) = if frame.payload.len() >= 2 {
                        (
                            u16::from_be_bytes([
                                frame.payload[0],
                                frame.payload[1],
                            ]),
                            String::from_utf8_lossy(&frame.payload[2..])
                                .into_owned(),
                        )
                    } else {
                        (1005, String::new())
                    };
                    Message::Close { code, reason }
                }
                _ => continue,
            }));
        }
    }

    async fn send(
        &mut self,
        message: Message,
    ) -> crate::Result<()> {
        match message {
            Message::Text(text) => {
                self.socket
                    .write_frame(Frame::text(Payload::Owned(text.into_bytes())))
                    .await?
            }
            Message::Binary(bytes) => {
                self.socket
                    .write_frame(Frame::binary(Payload::Owned(bytes)))
                    .await?
            }
            Message::Close { code: 1006, .. } => {} // Never emit an abnormal-close pseudo-code.
            Message::Close { code: 1005, .. } => {
                self.socket
                    .write_frame(Frame::new(
                        true,
                        OpCode::Close,
                        None,
                        Payload::Owned(Vec::new()),
                    ))
                    .await?
            }
            Message::Close { code, reason } => {
                self.socket
                    .write_frame(Frame::close(code, reason.as_bytes()))
                    .await?
            }
        }
        Ok(())
    }
}

/// The existing edge/Gufi record format on an accepted Orbit duplex slot.
/// Dropping the halves ends this session's directions and wakes its peer.
pub struct OrbitMessageStream {
    incoming: ReadHalf,
    outgoing: WriteHalf,
    records: MessageReader,
}

impl OrbitMessageStream {
    pub fn new(endpoint: Endpoint) -> Self {
        let (incoming, outgoing) = endpoint.split();
        Self {
            incoming,
            outgoing,
            records: MessageReader::with_limit(MAX_MESSAGE_SIZE),
        }
    }
}

impl MessageStream for OrbitMessageStream {
    async fn receive(&mut self) -> crate::Result<Option<Message>> {
        Ok(self.records.read(&mut self.incoming).await?)
    }

    async fn send(
        &mut self,
        message: Message,
    ) -> crate::Result<()> {
        write_message(&mut self.outgoing, &message).await?;
        if matches!(message, Message::Close { .. }) {
            self.outgoing.shutdown().await?;
        }
        Ok(())
    }
}

/// A reliable WebTransport bidirectional stream carrying the existing
/// Text/Binary/Close records. The accepting H3 connection owns the IO.
pub struct WebTransportMessageStream<S> {
    stream: S,
    records: MessageReader,
}

impl<S> WebTransportMessageStream<S> {
    pub fn new(stream: S) -> Self {
        Self { stream, records: MessageReader::with_limit(MAX_MESSAGE_SIZE) }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> MessageStream
    for WebTransportMessageStream<S>
{
    async fn receive(&mut self) -> crate::Result<Option<Message>> {
        Ok(self.records.read(&mut self.stream).await?)
    }

    async fn send(
        &mut self,
        message: Message,
    ) -> crate::Result<()> {
        write_message(&mut self.stream, &message).await?;
        self.stream.flush().await?;
        if matches!(message, Message::Close { .. }) {
            self.stream.shutdown().await?;
        }
        Ok(())
    }
}
