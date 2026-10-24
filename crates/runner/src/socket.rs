//! The runner's own sockets: TCP and Unix, HTTP/1 and HTTP/2, TLS on TCP.
//!
//! It turns a connection into [`AppRequest`]s for the one [`Application`] and
//! writes back what it answers; everything between is the application's, which
//! is why the same service runs unchanged under the Orbit ingress, where there
//! is no socket at all. There is no vhost and no HTTP/3 here: one application,
//! reached over a loopback, a private network or a Unix socket, where QUIC
//! would buy a UDP stack and a congestion controller for nothing.
//!
//! What it attaches is the edge's: the accepted connection's facts in
//! `ConnCtx` (none for a Unix socket, which has no addresses), the client
//! after [`Trust`] in `RealIpExt`, a trusted proxy's word in `Forwarded`, and
//! when the request arrived in `ReceivedAt`.
//!
//! Shutdown stops accepting, asks every connection to finish, and joins them
//! and the sessions they started, with no deadline of its own.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use claviron_http::ext::{ConnCtx, RealIpExt, ReceivedAt, TlsInfo};
use claviron_http::session::{ConnectionScope, SessionOwner};
use claviron_http::trust::Trust;
use claviron_runtime::{
    AppState, Component, Gate, RunContext, RunMode, Runnable,
    ServiceStartPolicy, registry,
};
use claviron_sys::net::{
    AcceptErrorClass, PortSharing, classify_accept_error, create_tcp_listener,
};
use http::Request;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::routing::{AppRequest, Application, RequestBody};

/// Where a listener listens.
#[derive(Clone, Debug)]
pub enum Bind {
    Tcp(SocketAddr),
    Uds(PathBuf),
}

impl Bind {
    fn describe(
        &self,
        secure: bool,
    ) -> String {
        match self {
            Self::Tcp(address) if secure => format!("https://{address}"),
            Self::Tcp(address) => format!("http://{address}"),
            Self::Uds(path) => format!("unix:{}", path.display()),
        }
    }
}

/// Which HTTP versions the sockets accept. Naming neither is the same as
/// naming both: whatever the client speaks.
#[derive(Clone, Copy, Debug)]
pub struct Protocols {
    pub h1: bool,
    pub h2: bool,
}

impl Default for Protocols {
    fn default() -> Self {
        Self { h1: true, h2: true }
    }
}

impl Protocols {
    pub fn resolve(
        h1: bool,
        h2: bool,
    ) -> Self {
        if h1 || h2 { Self { h1, h2 } } else { Self::default() }
    }

    #[cfg(feature = "tls")]
    fn alpn(self) -> Vec<&'static [u8]> {
        let mut alpn: Vec<&'static [u8]> = Vec::new();
        if self.h2 {
            alpn.push(b"h2");
        }
        if self.h1 {
            alpn.push(b"http/1.1");
        }
        alpn
    }
}

/// Every socket the application answers on, as one runnable: they share a
/// lifetime, a cancellation and an application, and differ only in where
/// they accept.
pub struct SocketIngress {
    binds: Vec<Bind>,
    protocols: Protocols,
    #[cfg(feature = "tls")]
    tls: Option<crate::Tls>,
}

impl SocketIngress {
    pub fn new(
        binds: Vec<Bind>,
        protocols: Protocols,
    ) -> Self {
        Self {
            binds,
            protocols,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Serve TLS on the TCP sockets. A Unix socket ignores it: its boundary is
    /// the filesystem's already.
    #[cfg(feature = "tls")]
    pub fn tls(
        mut self,
        tls: Option<crate::Tls>,
    ) -> Self {
        self.tls = tls;
        self
    }
}

impl Component<AppState> for SocketIngress {
    type Deps = (Arc<Application>, Option<Arc<Trust>>);

    fn name(&self) -> &'static str {
        "SocketIngress"
    }

    /// A Unix socket file is one process's: workers cannot each bind it the
    /// way they share a TCP port. Refused at launch, in the master, rather
    /// than as a worker that fails and is restarted forever.
    fn validate(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        if !matches!(state.run_mode(), RunMode::Standalone)
            && let Some(Bind::Uds(path)) =
                self.binds.iter().find(|bind| matches!(bind, Bind::Uds(_)))
        {
            return Err(registry::Error::msg(format!(
                "unix:{} cannot be served by several workers; serve it standalone, or reach \
                 the workers over TCP or Orbit",
                path.display()
            )));
        }
        Ok(())
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

/// A Unix socket file this ingress made. Removed on the way out only if it is
/// still the same file: a later process may have put its own in its place.
struct SocketFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketFile {
    fn owned(path: PathBuf) -> std::io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::symlink_metadata(&path)?;
        Ok(Self { path, device: meta.dev(), inode: meta.ino() })
    }
}

impl Drop for SocketFile {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        if let Ok(meta) = std::fs::symlink_metadata(&self.path)
            && meta.dev() == self.device
            && meta.ino() == self.inode
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Bind a Unix socket. A file already at the path is left alone if something
/// answers on it — it belongs to a live process — and removed if nothing
/// does: a process that is gone left it behind.
fn bind_unix(path: &PathBuf) -> std::io::Result<UnixListener> {
    if std::fs::symlink_metadata(path).is_ok() {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!(
                        "{}: a live process is listening on it",
                        path.display()
                    ),
                ));
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::ConnectionRefused =>
            {
                std::fs::remove_file(path)?;
            }
            Err(error) => return Err(error),
        }
    }
    UnixListener::bind(path)
}

enum Listener {
    Tcp(TcpListener),
    /// The file is held for its `Drop`: it goes when the listener does.
    Uds {
        listener: UnixListener,
        _file: SocketFile,
    },
}

#[async_trait]
impl Runnable<AppState, <Self as Component<AppState>>::Deps> for SocketIngress {
    /// A master with workers serves nothing; its workers do. Standalone is not
    /// a master and still runs.
    fn start_policy(
        &self,
        state: &AppState,
    ) -> ServiceStartPolicy {
        if state.is_master() {
            ServiceStartPolicy::Unavailable
        } else {
            ServiceStartPolicy::Automatic
        }
    }

    async fn run(
        self: Arc<Self>,
        (application, trust): <Self as Component<AppState>>::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let trust = trust.unwrap_or_default();

        // Read the certificate before any socket opens: a bad path or a key
        // that does not match is a boot failure, not a failed handshake later.
        #[cfg(feature = "tls")]
        let acceptor = self
            .tls
            .as_ref()
            .map(|tls| tls.acceptor(&self.protocols.alpn()))
            .transpose()
            .map_err(failure)?;

        // Workers share a TCP port, as Claviron's own do; one process holds it
        // alone.
        let sharing = match state.run_mode() {
            RunMode::Worker(_) => PortSharing::Shared,
            RunMode::Standalone | RunMode::Master => PortSharing::Exclusive,
        };

        // Every socket is bound before any is served, so one already taken is
        // a boot failure rather than a process that half serves.
        let mut listeners = Vec::with_capacity(self.binds.len());
        for bind in &self.binds {
            let listener = match bind {
                Bind::Tcp(address) => Listener::Tcp(
                    create_tcp_listener(*address, sharing)
                        .await
                        .map_err(failure)?,
                ),
                Bind::Uds(path) => {
                    let listener = bind_unix(path).map_err(failure)?;
                    Listener::Uds {
                        listener,
                        _file: SocketFile::owned(path.clone())
                            .map_err(failure)?,
                    }
                }
            };
            let bound = match &listener {
                // Port zero becomes a real port; report the one taken.
                Listener::Tcp(listener) => {
                    Bind::Tcp(listener.local_addr().map_err(failure)?)
                }
                Listener::Uds { .. } => bind.clone(),
            };
            #[cfg(feature = "tls")]
            let secure = acceptor.is_some() && matches!(bound, Bind::Tcp(_));
            #[cfg(not(feature = "tls"))]
            let secure = false;
            tracing::info!(
                trusted = if trust.is_empty() {
                    "nobody"
                } else {
                    "registered peers"
                },
                "listening on {}",
                bound.describe(secure)
            );
            listeners.push(listener);
        }

        let sessions = Arc::new(SessionOwner::new(
            "runner".parse().expect("a valid listener name"),
        ));
        // Never waits: the gate carries shutdown, and keeps a connection's
        // lease alive past an upgrade.
        let gate = Gate::new(None, std::time::Duration::ZERO);
        let stop = CancellationToken::new();
        let _stop_on_drop = stop.clone().drop_guard();
        let shared =
            Arc::new(Shared { application, trust, protocols: self.protocols });

        let mut accepting = JoinSet::new();
        for listener in listeners {
            let shared = Arc::clone(&shared);
            let stop = stop.clone();
            let sessions = Arc::clone(&sessions);
            let gate = gate.clone();
            #[cfg(feature = "tls")]
            let acceptor = acceptor.clone();
            accepting.spawn(async move {
                accept_loop(
                    listener,
                    shared,
                    stop,
                    sessions,
                    gate,
                    #[cfg(feature = "tls")]
                    acceptor,
                )
                .await
            });
        }

        let mut failure_seen = None;
        tokio::select! {
            () = context.cancelled() => {}
            Some(finished) = accepting.join_next() => {
                if let Ok(Err(error)) | Err(error) = finished.map_err(std::io::Error::other) {
                    failure_seen = Some(error);
                }
            }
        }
        // Stop accepting, ask every connection to finish, then join them and
        // the sessions they started.
        stop.cancel();
        while let Some(finished) = accepting.join_next().await {
            if let Ok(Err(error)) = finished
                && failure_seen.is_none()
            {
                failure_seen = Some(error);
            }
        }
        gate.graceful_shutdown(None);
        // Every accept loop has returned, so this is the last holder.
        if let Some(sessions) = Arc::into_inner(sessions) {
            sessions.close().join().await;
        }
        match failure_seen {
            Some(error) => Err(failure(error)),
            None => Ok(()),
        }
    }
}

struct Shared {
    application: Arc<Application>,
    trust: Arc<Trust>,
    protocols: Protocols,
}

async fn accept_loop(
    listener: Listener,
    shared: Arc<Shared>,
    stop: CancellationToken,
    sessions: Arc<SessionOwner>,
    gate: Gate,
    #[cfg(feature = "tls")] acceptor: Option<crate::tls::TlsAcceptor>,
) -> std::io::Result<()> {
    let mut connections = JoinSet::new();
    let result = loop {
        let accepted = tokio::select! {
            biased;
            () = stop.cancelled() => break Ok(()),
            Some(done) = connections.join_next(), if !connections.is_empty() => {
                report(done);
                continue;
            }
            accepted = accept(&listener) => accepted
        };
        let (stream, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => match classify_accept_error(&error) {
                // A peer gone before its accept completed: the listener is fine.
                AcceptErrorClass::Transient => continue,
                AcceptErrorClass::ResourcePressure
                | AcceptErrorClass::Fatal => break Err(error),
            },
        };
        let scope = match gate.try_enter() {
            Ok(permit) => sessions.bind(permit),
            // The gate closes only on shutdown.
            Err(_) => break Ok(()),
        };
        let shared = Arc::clone(&shared);
        let stop = stop.clone();
        match stream {
            Accepted::Tcp(stream, local) => {
                #[cfg(feature = "tls")]
                if let Some(acceptor) = acceptor.clone() {
                    connections.spawn(async move {
                        // The handshake belongs to this loop's tasks too, and
                        // cancellation ends an idle one with no guessed timeout.
                        let stream = tokio::select! {
                            biased;
                            () = stop.cancelled() => return Ok(()),
                            result = acceptor.accept(stream) => result?
                        };
                        let tls =
                            crate::tls::connection_info(stream.get_ref().1);
                        let connection = connection(peer, local, Some(tls));
                        serve(stream, peer, connection, shared, stop, scope)
                            .await
                    });
                    continue;
                }
                let connection = connection(peer, local, None);
                connections.spawn(serve(
                    stream, peer, connection, shared, stop, scope,
                ));
            }
            Accepted::Uds(stream) => {
                connections
                    .spawn(serve(stream, None, None, shared, stop, scope));
            }
        }
    };
    // Every connection was asked to finish when `stop` fired; wait for them.
    while let Some(done) = connections.join_next().await {
        report(done);
    }
    drop(listener);
    result
}

enum Accepted {
    Tcp(tokio::net::TcpStream, Option<SocketAddr>),
    Uds(tokio::net::UnixStream),
}

async fn accept(
    listener: &Listener
) -> std::io::Result<(Accepted, Option<SocketAddr>)> {
    Ok(match listener {
        Listener::Tcp(listener) => {
            let (stream, peer) = listener.accept().await?;
            let local = stream.local_addr().ok();
            (Accepted::Tcp(stream, local), Some(peer))
        }
        Listener::Uds { listener, .. } => {
            let (stream, _) = listener.accept().await?;
            (Accepted::Uds(stream), None)
        }
    })
}

fn report(done: Result<std::io::Result<()>, tokio::task::JoinError>) {
    if !matches!(done, Ok(Ok(()))) {
        tracing::debug!(?done, "connection ended");
    }
}

/// The accepted connection's own facts, the edge's type for them. A Unix
/// socket has no addresses and gets none rather than placeholders.
fn connection(
    peer: Option<SocketAddr>,
    local: Option<SocketAddr>,
    tls: Option<TlsInfo>,
) -> Option<Arc<ConnCtx>> {
    let (Some(remote), Some(local)) = (peer, local) else { return None };
    Some(Arc::new(ConnCtx {
        state: Default::default(),
        listener: "runner".parse().expect("a valid listener name"),
        proto: None,
        remote,
        local_ip: local.ip(),
        local_port: local.port(),
        real_ip: None,
        country_code: None,
        tls,
    }))
}

async fn serve<I>(
    stream: I,
    peer: Option<SocketAddr>,
    connection: Option<Arc<ConnCtx>>,
    shared: Arc<Shared>,
    stop: CancellationToken,
    scope: ConnectionScope,
) -> std::io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let spawner = scope.sessions();
    let protocols = shared.protocols;
    let service = service_fn(move |request: Request<Incoming>| {
        let shared = Arc::clone(&shared);
        let connection = connection.clone();
        let spawner = spawner.clone();
        async move {
            let request =
                prepare(request, peer, connection, &shared.trust, spawner);
            Ok::<_, Infallible>(shared.application.call(request).await)
        }
    });
    let io = TokioIo::new(stream);
    let result = match (protocols.h1, protocols.h2) {
        (true, false) => {
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .with_upgrades();
            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => result.map_err(std::io::Error::other),
                () = stop.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await.map_err(std::io::Error::other)
                }
            }
        }
        (false, true) => {
            let connection =
                hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service);
            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => result.map_err(std::io::Error::other),
                () = stop.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await.map_err(std::io::Error::other)
                }
            }
        }
        // Whatever the client speaks: HTTP/1 with an upgrade, or HTTP/2 with
        // prior knowledge.
        _ => {
            let builder = hyper_util::server::conn::auto::Builder::new(
                TokioExecutor::new(),
            );
            let connection =
                builder.serve_connection_with_upgrades(io, service);
            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => result.map_err(|error| std::io::Error::other(error.to_string())),
                () = stop.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await.map_err(|error| std::io::Error::other(error.to_string()))
                }
            }
        }
    };
    drop(scope);
    result
}

/// The request as the application sees it, with what the connection and a
/// trusted proxy say about it in its extensions.
fn prepare(
    request: Request<Incoming>,
    peer: Option<SocketAddr>,
    connection: Option<Arc<ConnCtx>>,
    trust: &Trust,
    sessions: claviron_http::session::SessionSpawner,
) -> AppRequest {
    let resolved = trust.resolve(peer, request.headers());
    let (mut parts, body) = request.into_parts();
    // A trusted proxy reported the host the client asked for; an application
    // reads `Host`, and behind a proxy the socket's is the proxy's own.
    if let Some(host) = resolved.host.as_deref()
        && let Ok(value) = http::HeaderValue::from_str(host)
    {
        parts.headers.insert(http::header::HOST, value);
    }
    let mut request = Request::from_parts(parts, RequestBody::new(body));
    let extensions = request.extensions_mut();
    extensions.insert(ReceivedAt(SystemTime::now()));
    if let Some(client) = resolved.client {
        extensions.insert(RealIpExt(client.ip()));
    }
    if let Some(forwarded) = resolved.forwarded(peer) {
        extensions.insert(forwarded);
    }
    if let Some(connection) = connection {
        extensions.insert(connection);
    }
    extensions.insert(sessions);
    request
}

fn failure(error: impl std::fmt::Display) -> registry::Error {
    registry::Error::msg(error.to_string())
}
