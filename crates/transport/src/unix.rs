//! HTTP over a Unix socket: a door whose boundary is the filesystem's, for a
//! proxy on the same host. A connection has no addresses and therefore no
//! connection facts, and TLS has nothing to add here.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use claviron_http::config::{
    H1Config, ListenerSettings, SocketGroup, UdsConfig,
};
use claviron_http::session::{ConnectionScope, SessionOwner, SessionSpawner};
use claviron_runtime::{AppState, RunContext};
use futures_util::stream::poll_fn;
use futures_util::{StreamExt, stream::SelectAll};

mod shared;
use claviron_runtime::gate::{Gate, GateDrainOutcome};
use claviron_runtime::registry::{Error, Result};
use claviron_sys::net::{AcceptErrorClass, classify_accept_error};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
pub use shared::{prepare, retire};
use tokio::net::UnixStream;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::tcp::listener::TcpListenerSnapshot;
use crate::tcp::prior::{PriorKnowledge, drive};
use crate::tcp::{Requests, plain};

/// What one Unix socket serves. It shares the listener's limits, drain and
/// HTTP settings with a TCP listener; only the socket differs.
pub trait Unix: TcpListenerSnapshot + Send + Sync + 'static {
    type Requests: Requests;

    fn h1(&self) -> &H1Config;

    fn prior_knowledge(&self) -> Option<&PriorKnowledge> {
        None
    }

    /// Called once for each accepted connection, before its first request.
    fn connection(
        self: &Arc<Self>,
        sessions: SessionSpawner,
    ) -> Self::Requests;
}

impl<S: plain::Cleartext> Unix for S {
    type Requests = S::Requests;

    fn h1(&self) -> &H1Config {
        plain::Cleartext::h1(self)
    }
    fn prior_knowledge(&self) -> Option<&PriorKnowledge> {
        plain::Cleartext::prior_knowledge(self)
    }
    fn connection(
        self: &Arc<Self>,
        sessions: SessionSpawner,
    ) -> Self::Requests {
        let local = std::net::Ipv4Addr::LOCALHOST;
        let facts = Arc::new(claviron_http::ext::ConnCtx {
            transport: claviron_http::listener::ListenerTransport::Unix,
            state: Default::default(),
            listener: self.listener().id.clone(),
            proto: if plain::Cleartext::prior_knowledge(self.as_ref()).is_none()
            {
                Some(claviron_http::listener::HttpProtocol::H1)
            } else {
                None
            },
            // Local ingress identity, not a forwarded client's address.
            remote: (local, 0).into(),
            local_ip: local.into(),
            local_port: 0,
            real_ip: None,
            country_code: None,
            tls: None,
        });
        plain::Cleartext::connection(self, facts, sessions)
    }
}

/// Removes the socket file when the listener goes, unless another process has
/// since put its own at that path.
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
/// does: a process that is gone left it behind. `uds.backlog` sizes the
/// queue (unwritten, the operating system's maximum, as `UnixListener::bind`
/// does); `uds.mode` and `uds.group` are applied to the file before the
/// listener is handed back, so it never accepts with other permissions.
fn bind(
    path: &Path,
    uds: &UdsConfig,
) -> std::io::Result<std::os::unix::net::UnixListener> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        use std::os::unix::fs::FileTypeExt;
        if !metadata.file_type().is_socket() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                "Unix listener path is not a socket",
            ));
        }
        let probe = socket2::Socket::new(
            socket2::Domain::UNIX,
            socket2::Type::STREAM,
            None,
        )?;
        probe.set_nonblocking(true)?;
        match probe.connect(&socket2::SockAddr::unix(path)?) {
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
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.raw_os_error() == Some(libc::EINPROGRESS) =>
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    "a live process owns the Unix socket",
                ));
            }
            Err(error) => return Err(error),
        }
    }
    let socket = socket2::Socket::new(
        socket2::Domain::UNIX,
        socket2::Type::STREAM,
        None,
    )?;
    socket.bind(&socket2::SockAddr::unix(path)?)?;
    if let Err(error) = permit(path, uds) {
        let _ = std::fs::remove_file(path);
        return Err(error);
    }
    // SOMAXCONN is the OS queue ceiling, matching UnixListener::bind.
    let backlog =
        uds.backlog.map_or(libc::SOMAXCONN, |value| value.get() as i32);
    if let Err(error) =
        socket.listen(backlog).and_then(|()| socket.set_nonblocking(true))
    {
        let _ = std::fs::remove_file(path);
        return Err(error);
    }
    let listener = std::os::unix::net::UnixListener::from(socket);

    Ok(listener)
}

/// Give the socket file its configured mode and group.
fn permit(
    path: &Path,
    uds: &UdsConfig,
) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        path,
        std::fs::Permissions::from_mode(uds.mode.bits()),
    )?;
    let Some(group) = &uds.group else {
        return Ok(());
    };
    let gid = match group {
        SocketGroup::Id(id) => *id,
        SocketGroup::Name(name) => claviron_sys::group::group_id(name)?,
    };
    std::os::unix::fs::chown(path, None, Some(gid)).map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!("hand the socket to group {gid}: {error}"),
        )
    })
}

/// Accept on the declared Unix sockets, using descriptors inherited by workers.
/// Publication may change between accepts; existing connections retain theirs.
pub async fn serve_current<S, C>(
    current: C,
    initial: Arc<S>,
    state: &AppState,
    context: &RunContext,
) -> Result<()>
where
    S: Unix,
    C: Fn() -> Option<Arc<S>>,
{
    let listener = initial.listener();
    let ListenerSettings::Tcp(settings) = &listener.settings else {
        return Err(Error::msg("Unix listener requires stream settings"));
    };
    let mut sockets = SelectAll::new();
    let mut files = Vec::new();
    for (index, address) in listener.listen.iter().enumerate() {
        let claviron_http::listener::ListenAddress::Unix(path) = address else {
            return Err(Error::msg("Unix listener requires Unix addresses"));
        };
        let (socket, file) =
            shared::acquire(state, listener, index, path, &settings.uds)?;
        files.extend(file);
        sockets.push(poll_fn(move |cx| socket.poll_accept(cx).map(Some)));
    }
    tracing::info!(listener = %listener.id, "Unix listener accepting");
    let gate = Gate::new(
        listener.limits.max_connections.map(|value| value.get()),
        listener.limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(listener.id.clone());
    let mut connections = JoinSet::new();
    let mut failed = None;
    loop {
        let accepted = tokio::select! {
            biased;
            _ = context.cancelled() => break,
            Some(done) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = done {
                    warn!(listener = %listener.id, %error, "Unix connection task failed");
                }
                continue;
            }
            accepted = sockets.next() => match accepted {
                Some(accepted) => accepted,
                None => break,
            },
        };
        let stream = match accepted {
            Ok((stream, _)) => stream,
            Err(error) => match classify_accept_error(&error) {
                AcceptErrorClass::Transient => continue,
                AcceptErrorClass::ResourcePressure
                | AcceptErrorClass::Fatal => {
                    failed = Some(Error::msg(format!(
                        "listener '{}': accept: {error}",
                        listener.id
                    )));
                    break;
                }
            },
        };
        let Some(snapshot) = current() else { break };
        let permit = tokio::select! {
            biased;
            _ = context.cancelled() => break,
            entered = gate.enter() => match entered {
                Ok(permit) => permit,
                Err(error) => {
                    debug!(listener = %listener.id, ?error, "Unix admission rejected");
                    continue;
                }
            }
        };
        connections.spawn(serve_stream(
            stream,
            snapshot,
            sessions.bind(permit),
        ));
    }
    drop(sockets);
    gate.graceful_shutdown(listener.limits.graceful_timeout);
    if let GateDrainOutcome::Forced { remaining } = gate.wait_all_done().await {
        warn!(listener = %listener.id, remaining, "Unix drain expired; forcing accepted tasks");
    }
    while let Some(done) = connections.join_next().await {
        if let Err(error) = done
            && !error.is_cancelled()
        {
            warn!(listener = %listener.id, %error, "Unix task failed during drain");
        }
    }
    sessions.close().join().await;
    drop(files);
    failed.map_or(Ok(()), Err)
}

/// Serve one accepted stream until its peer completes or its scope drains.
pub async fn serve_stream<S: Unix>(
    stream: UnixStream,
    snapshot: Arc<S>,
    scope: ConnectionScope,
) {
    let requests = snapshot.connection(scope.sessions());
    let service = service_fn(move |request| requests.call(request));
    let listener = &snapshot.listener().id;
    let result = match snapshot.prior_knowledge() {
        Some(prior) => {
            drive(stream, service, prior, snapshot.h1(), &scope).await
        }
        None => {
            let connection = crate::tcp::protocol::h1(snapshot.h1())
                .serve_connection(TokioIo::new(stream), service);
            #[cfg(feature = "websocket")]
            let connection = connection.with_upgrades();
            tokio::pin!(connection);
            tokio::select! {
                biased;
                _ = scope.permit().wait_graceful_shutdown() => {
                    connection.as_mut().graceful_shutdown();
                    tokio::select! {
                        result = connection.as_mut() => Some(result.map_err(Into::into)),
                        _ = scope.permit().wait_forced_shutdown() => None
                    }
                }
                result = connection.as_mut() => Some(result.map_err(Into::into))
            }
        }
    };
    if let Some(Err(error)) = result {
        debug!(%listener, %error, "Unix connection ended");
    }
}
