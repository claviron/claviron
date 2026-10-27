//! Shared TCP readiness without a second descriptor or a competing reader.

use std::io::{self, IoSlice};
use std::net::Shutdown;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};
use tokio::net::TcpStream;

/// The protocol owns reads/writes; an H1 connection owner may independently
/// await socket errors while half-close keeps the protocol's read side idle.
#[derive(Clone)]
pub(crate) struct TcpIo(Arc<TcpStream>);

impl TcpIo {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self(Arc::new(stream))
    }

    /// Observe an actual socket error, never ordinary input EOF or queued data.
    /// `async_io` clears false readiness on WouldBlock and parks on the reactor.
    /// No byte is consumed and no read/write readiness waiter is replaced.
    pub(crate) async fn failed(&self) -> io::Error {
        self.0
            .async_io(Interest::ERROR, || match self.0.take_error() {
                Ok(Some(error)) => Ok(error),
                Ok(None) => Err(io::ErrorKind::WouldBlock.into()),
                Err(error) => Ok(error),
            })
            .await
            .unwrap_or_else(|error| error)
    }
}

impl Deref for TcpIo {
    type Target = TcpStream;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsyncRead for TcpIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            ready!(self.0.poll_read_ready(cx))?;
            match self.0.try_read_buf(buf) {
                Ok(_) => return Poll::Ready(Ok(())),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }
}

impl AsyncWrite for TcpIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.0.poll_write_ready(cx))?;
            match self.0.try_write(buf) {
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
        loop {
            ready!(self.0.poll_write_ready(cx))?;
            match self.0.try_write_vectored(bufs) {
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
