use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use fastcgi_client::Client;
use fastcgi_client::conn::KeepAlive;
use fastcgi_client::io::TokioCompat;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;

use crate::error::{ConnectError, Connection as ConnKind};
use crate::events::Expiry;

pub trait FcgiStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> FcgiStream for T {}

/// Records that FPM has said something, so a failed request can tell
/// "never answered" from "answered and then broke".
///
/// The flag is cleared at the start of each request rather than kept for
/// the connection's life, which is safe here and is not in a client with
/// a background driver: `Client::execute` reads inside the call, so
/// between two requests on a keep-alive connection there is no read
/// outstanding to race the clear. A per-connection flag would veto every
/// replay after the first successful response, which on a connection
/// configured to serve 900 of them is the same as having no replay.
struct Observed<I> {
    inner: I,
    received: Arc<AtomicBool>,
}

impl<I: AsyncRead + Unpin> AsyncRead for Observed<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buffer);
        if buffer.filled().len() > before {
            self.received.store(true, Ordering::Release);
        }
        result
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for Observed<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

pub type DynStream = Box<dyn FcgiStream>;
pub type FcgiClient = Client<TokioCompat<DynStream>, KeepAlive>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FcgiEndpoint {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

impl FcgiEndpoint {
    pub(crate) async fn open(
        &self,
        connect_timeout: Duration,
    ) -> Result<DynStream, ConnectError> {
        match self {
            FcgiEndpoint::Unix(path) => {
                let s = timeout(connect_timeout, UnixStream::connect(path))
                    .await
                    .map_err(|_| {
                        ConnectError(
                            ConnKind::Unix,
                            io::Error::from(io::ErrorKind::TimedOut),
                        )
                    })?
                    .map_err(|e| ConnectError(ConnKind::Unix, e))?;
                Ok(Box::new(s) as DynStream)
            }
            FcgiEndpoint::Tcp(addr) => {
                let s = timeout(connect_timeout, TcpStream::connect(*addr))
                    .await
                    .map_err(|_| {
                        ConnectError(
                            ConnKind::Tcp,
                            io::Error::from(io::ErrorKind::TimedOut),
                        )
                    })?
                    .map_err(|e| ConnectError(ConnKind::Tcp, e))?;
                Ok(Box::new(s) as DynStream)
            }
        }
    }
}

pub struct FcgiConnection {
    /// Process-wide monotonic id assigned by the owning pool at open time.
    /// Read-only after construction (never reassigned), so callers can
    /// embed it in CGI vars / logs / metrics without needing
    /// synchronisation. Pairing this with `used` lets PHP / observers
    /// answer "same conn?" (id stable) and "same worker still?" (id +
    /// used both stable since the last sample).
    id: u64,
    client: FcgiClient,
    created_at: Instant,
    last_used_at: Instant,
    used: u32,
    /// Set the moment FPM writes anything for the request in flight, and
    /// cleared by `begin_request`. See [`Observed`].
    received: Arc<AtomicBool>,
}

impl FcgiConnection {
    pub async fn open(
        endpoint: &FcgiEndpoint,
        connect_timeout: Duration,
        id: u64,
    ) -> Result<Self, ConnectError> {
        let stream = endpoint.open(connect_timeout).await?;
        let received = Arc::new(AtomicBool::new(false));
        let stream: DynStream = Box::new(Observed {
            inner: stream,
            received: Arc::clone(&received),
        });
        let now = Instant::now();
        Ok(Self {
            id,
            client: Client::new_keep_alive_tokio(stream),
            received,
            created_at: now,
            last_used_at: now,
            used: 0,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn client_mut(&mut self) -> &mut FcgiClient {
        &mut self.client
    }

    /// Start a request's evidence from nothing.
    ///
    /// Called immediately before the request is written, while no read is
    /// outstanding. Everything FPM says from here belongs to this request.
    pub fn begin_request(&mut self) {
        self.received.store(false, Ordering::Release);
    }

    /// Whether FPM wrote anything for the request in flight.
    ///
    /// `false` is what makes a replay legal: FPM never answered, so the
    /// worst case is that it received the request and died before saying
    /// so. `true` forbids one — the response had started, and sending the
    /// request again would be a second execution of work that already
    /// happened.
    ///
    /// No fence is needed before reading it, unlike a client whose
    /// connection is driven by a background task: `Client::execute` has
    /// returned by the time a caller asks, so nothing can still be
    /// reading into the flag.
    pub fn saw_response_byte(&self) -> bool {
        self.received.load(Ordering::Acquire)
    }

    pub fn mark_used(&mut self) {
        self.used = self.used.saturating_add(1);
        self.last_used_at = Instant::now();
    }

    /// Number of completed requests this keep-alive connection has served
    /// since it was opened. Read at request-build time to inject the
    /// "request index on this conn" CGI variable so the paired PHP/FPM
    /// child can tell whether it is on its 1st (cold) or Nth (warm)
    /// request without reaching for a separate FPM status endpoint.
    pub fn used(&self) -> u32 {
        self.used
    }

    /// Which of the three limits has retired this connection, if any.
    ///
    /// The reason is returned rather than folded into a bool because the
    /// three say different things about a deployment — a rotation working
    /// as configured, a connection outliving its welcome, a pool wider
    /// than its traffic — and an eviction counter that cannot tell them
    /// apart answers none of the questions it is read for.
    pub fn expiry(
        &self,
        max_lifetime: Option<Duration>,
        max_requests: Option<u32>,
        idle_timeout: Option<Duration>,
    ) -> Option<Expiry> {
        if let Some(max) = max_lifetime
            && self.created_at.elapsed() >= max
        {
            return Some(Expiry::Lifetime);
        }
        if let Some(max) = max_requests
            && self.used >= max
        {
            return Some(Expiry::Requests);
        }
        if let Some(max) = idle_timeout
            && self.last_used_at.elapsed() >= max
        {
            return Some(Expiry::Idle);
        }
        None
    }

    pub fn is_stale(
        &self,
        max_lifetime: Option<Duration>,
        max_requests: Option<u32>,
        idle_timeout: Option<Duration>,
    ) -> bool {
        self.expiry(max_lifetime, max_requests, idle_timeout).is_some()
    }
}
