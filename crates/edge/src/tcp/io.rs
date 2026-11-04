//! Shared TCP readiness without a second descriptor or a competing reader.

use std::io::{self, IoSlice};
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// The protocol's side of an accepted socket: reads through Tokio's own read
/// half, writes and write shutdown on the same socket.
pub(crate) struct TcpIo(OwnedReadHalf);

/// A second handle on the same socket for its owner to await socket errors.
///
/// Used only by an H1 connection with half-close on, where the protocol
/// leaves its read side idle during a request and would not see one. Unused,
/// it is dropped and the socket stays with the protocol.
pub(crate) struct TcpErrors(Option<OwnedWriteHalf>);

impl TcpIo {
    pub(crate) fn split(stream: TcpStream) -> (Self, TcpErrors) {
        let (read, write) = stream.into_split();
        (Self(read), TcpErrors(Some(write)))
    }
}

impl TcpErrors {
    /// Observe an actual socket error, never ordinary input EOF or queued data.
    /// `async_io` clears false readiness on WouldBlock and parks on the reactor.
    /// No byte is consumed and no read/write readiness waiter is replaced.
    pub(crate) async fn failed(&self) -> io::Error {
        let stream = self.0.as_ref().expect("socket error handle").as_ref();
        stream
            .async_io(Interest::ERROR, || match stream.take_error() {
                Ok(Some(error)) => Ok(error),
                Ok(None) => Err(io::ErrorKind::WouldBlock.into()),
                Err(error) => Ok(error),
            })
            .await
            .unwrap_or_else(|error| error)
    }
}

impl Drop for TcpErrors {
    fn drop(&mut self) {
        // The protocol owns write shutdown, including after an HTTP upgrade.
        if let Some(write) = self.0.take() {
            write.forget();
        }
    }
}

impl AsyncRead for TcpIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        // Keep Tokio's platform-specific short-read readiness handling.
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let stream = self.0.as_ref();
        loop {
            ready!(stream.poll_write_ready(cx))?;
            match stream.try_write(buf) {
                Ok(written) => return Poll::Ready(Ok(written)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let stream = self.0.as_ref();
        loop {
            ready!(stream.poll_write_ready(cx))?;
            match stream.try_write_vectored(bufs) {
                Ok(written) => return Poll::Ready(Ok(written)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(
            socket2::SockRef::from(self.0.as_ref()).shutdown(Shutdown::Write),
        )
    }
}
