//! WebSocket messages over a stream slot, once an upgrade is accepted.
//!
//! A slot carries bytes; a WebSocket carries messages, and their boundaries
//! are the protocol above the slot's to keep. Each message crosses as one
//! record: its kind, its length and its payload.
//!
//! ```text
//! u8 kind | u32 length (little endian) | payload
//! ```
//!
//! The edge speaks the WebSocket protocol itself — framing, masking,
//! fragmentation, ping and pong stay on its side of the slot — so what crosses
//! is only what an application sees: whole text and binary messages, and the
//! close that ends them. The application never parses a WebSocket frame.

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const TEXT: u8 = 1;
const BINARY: u8 = 2;
const CLOSE: u8 = 8;

/// One WebSocket message, as an application sends and receives it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    Text(String),
    Binary(Vec<u8>),
    /// The close handshake: a status code (1000 is a normal close) and a
    /// reason, empty when none was given.
    Close {
        code: u16,
        reason: String,
    },
}

/// Write one message as one record.
pub async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Message,
) -> std::io::Result<()> {
    let (kind, payload): (u8, std::borrow::Cow<'_, [u8]>) = match message {
        Message::Text(text) => (TEXT, text.as_bytes().into()),
        Message::Binary(bytes) => (BINARY, bytes.as_slice().into()),
        Message::Close { code, reason } => {
            let mut payload = code.to_le_bytes().to_vec();
            payload.extend_from_slice(reason.as_bytes());
            (CLOSE, payload.into())
        }
    };
    let length = u32::try_from(payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a message longer than 4 GiB",
        )
    })?;
    // One write for the record: two would be two commits to the ring, and
    // a reader parked on it could be woken for the head alone.
    let mut record = Vec::with_capacity(5 + payload.len());
    record.push(kind);
    record.extend_from_slice(&length.to_le_bytes());
    record.extend_from_slice(&payload);
    writer.write_all(&record).await
}

/// Read one record. `None` is a clean end of the direction between records.
pub async fn read_message<R: AsyncRead + Unpin>(
    reader: &mut R
) -> std::io::Result<Option<Message>> {
    let mut head = [0u8; 5];
    match reader.read_exact(&mut head[..1]).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    }
    reader.read_exact(&mut head[1..]).await?;
    let length =
        u32::from_le_bytes([head[1], head[2], head[3], head[4]]) as usize;
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload).await?;
    decode(head[0], payload).map(Some)
}

/// Reads records one after another, and keeps its place when a read is
/// cancelled.
///
/// [`read_message`] is several reads, so a `select!` that drops it halfway
/// loses the bytes it had taken and the next record starts in the middle of
/// one. This reader keeps what it has read in its own buffer, so a dropped
/// [`MessageReader::read`] leaves nothing behind: the next call finishes the
/// record. It may read past the record it returns; the rest stays buffered for
/// the next call, so one reader is used for the whole direction.
#[derive(Debug, Default)]
pub struct MessageReader {
    buffer: BytesMut,
    limit: Option<usize>,
}

impl MessageReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enforce the consuming protocol's message limit before reserving payload
    /// storage. This does not change the shared record format.
    pub fn with_limit(limit: usize) -> Self {
        Self { buffer: BytesMut::new(), limit: Some(limit) }
    }

    /// Read the next record. Cancel safe. `None` is a clean end of the
    /// direction between records.
    pub async fn read<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> std::io::Result<Option<Message>> {
        loop {
            if let Some(message) = self.take()? {
                return Ok(Some(message));
            }
            if reader.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "a record cut short",
                ));
            }
        }
    }

    /// A whole record from the buffer, or `None` until one is there.
    fn take(&mut self) -> std::io::Result<Option<Message>> {
        let Some(head) = self.buffer.get(..5) else {
            return Ok(None);
        };
        let kind = head[0];
        let length =
            u32::from_le_bytes([head[1], head[2], head[3], head[4]]) as usize;
        if self.limit.is_some_and(|limit| length > limit) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WebSocket message exceeds the consumer's limit",
            ));
        }
        if self.buffer.len() < 5 + length {
            self.buffer.reserve(5 + length - self.buffer.len());
            return Ok(None);
        }
        self.buffer.advance(5);
        let payload = self.buffer.split_to(length).to_vec();
        decode(kind, payload).map(Some)
    }
}

fn decode(
    kind: u8,
    payload: Vec<u8>,
) -> std::io::Result<Message> {
    let malformed = |what: &'static str| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, what)
    };
    match kind {
        TEXT => String::from_utf8(payload)
            .map(Message::Text)
            .map_err(|_| malformed("a text message is not UTF-8")),
        BINARY => Ok(Message::Binary(payload)),
        CLOSE => {
            if payload.len() < 2 {
                return Err(malformed("a close record without a code"));
            }
            let code = u16::from_le_bytes([payload[0], payload[1]]);
            let reason = String::from_utf8(payload[2..].to_vec())
                .map_err(|_| malformed("a close reason is not UTF-8"))?;
            Ok(Message::Close { code, reason })
        }
        _ => Err(malformed("a record of an unknown kind")),
    }
}

#[cfg(test)]
#[path = "../../tests/transport/message.rs"]
mod tests;
