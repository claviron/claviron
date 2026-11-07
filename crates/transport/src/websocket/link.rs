//! A session's messages over an Orbit slot, where the edge speaks the protocol.
//!
//! Whole messages cross as records, so all that is left here is the session's
//! end: a limit on what crosses, and retirement, which an owner hears as a close
//! it can finish its output after.

use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use claviron_orbit::link::application::{MessageReader, write_message};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{Deliver, Message, WebSocket};
use crate::orbit::{LinkSession, ReadHalf, SessionShutdown, WriteHalf};

fn retired() -> Message {
    Message::Close { code: 1001, reason: "server shutting down".into() }
}

/// What the slot's writer is left to do once the owner has dropped its socket.
struct Finish {
    outgoing: WriteHalf,
    /// Retired with no close of the owner's own, so one is still owed.
    close: bool,
}

/// The slot, for an owner that holds the messages itself.
pub(super) struct LinkMessages {
    incoming: ReadHalf,
    outgoing: Option<WriteHalf>,
    reader: MessageReader,
    limit: usize,
    shutdown: SessionShutdown,
    retiring: bool,
    closed: bool,
    finish: Option<oneshot::Sender<Finish>>,
}

impl LinkMessages {
    pub(super) async fn recv(&mut self) -> Option<Message> {
        tokio::select! {
            biased;
            () = self.shutdown.cancelled(), if !self.retiring => {
                self.retiring = true;
                Some(retired())
            }
            message = self.reader.read(&mut self.incoming) => {
                message.ok().flatten()
            }
        }
    }

    pub(super) async fn send(
        &mut self,
        message: Message,
    ) -> io::Result<()> {
        valid_message(&message, self.limit, false)?;
        let outgoing = self.outgoing.as_mut().expect("held until dropped");
        write_message(outgoing, &message).await?;
        self.closed |= matches!(message, Message::Close { .. });
        Ok(())
    }
}

impl Drop for LinkMessages {
    fn drop(&mut self) {
        if let (Some(outgoing), Some(finish)) =
            (self.outgoing.take(), self.finish.take())
        {
            let close = self.retiring && !self.closed;
            let _ = finish.send(Finish { outgoing, close });
        }
    }
}

/// Run an owner's session over the slot, and end the slot's direction after it
/// has let go of the socket: an end is async, and dropping a socket is not.
pub(super) async fn hold<F, Fut>(
    session: LinkSession,
    limit: usize,
    owner: F,
) where
    F: FnOnce(WebSocket) -> Fut,
    Fut: Future<Output = ()>,
{
    let LinkSession { incoming, outgoing, shutdown } = session;
    let (finish, finished) = oneshot::channel();
    owner(WebSocket::link(LinkMessages {
        incoming,
        outgoing: Some(outgoing),
        reader: MessageReader::for_application(limit),
        limit,
        shutdown,
        retiring: false,
        closed: false,
        finish: Some(finish),
    }))
    .await;
    if let Ok(Finish { mut outgoing, close }) = finished.await {
        if close && write_message(&mut outgoing, &retired()).await.is_err() {
            return;
        }
        let _ = outgoing.shutdown().await;
    }
}

struct EndOnDrop<'a, D: Deliver>(&'a D);

impl<D: Deliver> Drop for EndOnDrop<'_, D> {
    fn drop(&mut self) {
        self.0.end();
    }
}

/// Carry messages between the slot and an owner on two channels, until either
/// side is done.
pub(super) async fn relay<R, W, D>(
    incoming: &mut R,
    outgoing: &mut W,
    inbound: &D,
    outbound: &mut mpsc::Receiver<Message>,
    limit: usize,
    shutdown: impl Future<Output = ()>,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    D: Deliver,
{
    let application_done = CancellationToken::new();
    let peer_failed = CancellationToken::new();
    let retiring = AtomicBool::new(false);
    tokio::pin!(shutdown);

    // The client's messages to the owner. Its end says the client is gone.
    let from_client = async {
        let _end = EndOnDrop(inbound);
        // Legal control records fit even when an operator chooses a smaller
        // text/binary bound. The shared codec checks lengths before allocation.
        let mut records = MessageReader::with_limit(limit.max(125));
        loop {
            let result = tokio::select! {
                biased;
                () = application_done.cancelled() => break,
                () = &mut shutdown => {
                    retiring.store(true, Ordering::Release);
                    // Notify the owner in order, retaining its current work and
                    // allowing it to finish output before returning.
                    tokio::select! {
                        () = application_done.cancelled() => {},
                        _ = inbound.deliver(retired()) => {},
                    }
                    break;
                },
                result = records.read(incoming) => result,
            };
            let message = match result {
                Ok(Some(message))
                    if valid_message(&message, limit, true).is_ok() =>
                {
                    message
                }
                // FIN affects only input: the owner may still send its remaining
                // response before returning.
                Ok(None) => break,
                _ => {
                    peer_failed.cancel();
                    break;
                }
            };
            let closing = matches!(message, Message::Close { .. });
            let delivered = tokio::select! {
                biased;
                () = application_done.cancelled() => false,
                delivered = inbound.deliver(message) => delivered,
            };
            if !delivered || closing {
                break;
            }
        }
    };
    // The owner's messages to the client, until it closes or returns.
    let to_client = async {
        let _finished = application_done.clone().drop_guard();
        let mut closed = false;
        loop {
            let message = tokio::select! {
                biased;
                () = peer_failed.cancelled() => return,
                message = outbound.recv() => message,
            };
            let Some(message) = message else { break };
            if let Err(error) = valid_message(&message, limit, false) {
                tracing::debug!(%error, "an owner produced an invalid Link message");
                return;
            }
            let closing = matches!(message, Message::Close { .. });
            let written = tokio::select! {
                biased;
                () = peer_failed.cancelled() => return,
                result = write_message(outgoing, &message) => result,
            };
            if written.is_err() {
                return;
            }
            if closing {
                closed = true;
                break;
            }
        }
        if !closed && retiring.load(Ordering::Acquire) {
            // All earlier owner messages completed before this close.
            let close = retired();
            let result = tokio::select! {
                () = peer_failed.cancelled() => return,
                result = write_message(outgoing, &close) => result,
            };
            if result.is_err() {
                return;
            }
        }
        let _ = tokio::select! {
            () = peer_failed.cancelled() => Ok(()),
            result = outgoing.shutdown() => result,
        };
    };
    tokio::join!(from_client, to_client);
}

fn valid_message(
    message: &Message,
    limit: usize,
    observation: bool,
) -> io::Result<()> {
    let valid = match message {
        Message::Text(text) => text.len() <= limit,
        Message::Binary(bytes) => bytes.len() <= limit,
        Message::Close { code, reason } => {
            (observation && matches!(code, 1005 | 1006) && reason.is_empty())
                || Message::sendable_close(*code, reason)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid or oversized Link message",
        ))
    }
}

#[cfg(test)]
#[path = "../../tests/websocket/link.rs"]
mod tests;
