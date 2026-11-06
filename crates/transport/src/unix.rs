//! HTTP over a Unix socket: a door whose boundary is the filesystem's, for a
//! proxy on the same host. A connection has no addresses and therefore no
//! connection facts, and TLS has nothing to add here.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use claviron_http::config::H1Config;
use claviron_http::session::{ConnectionScope, SessionOwner, SessionSpawner};
use claviron_runtime::RunContext;
use claviron_runtime::gate::{Gate, GateDrainOutcome};
use claviron_runtime::registry::{Error, Result};
use claviron_sys::net::{AcceptErrorClass, classify_accept_error};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::owner::Owner;
use crate::tcp::listener::TcpListenerSnapshot;
use crate::tcp::prior::{PriorKnowledge, drive};
use crate::tcp::{OwnerRequests, Requests, plain};

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

impl<O: Owner> Unix for plain::Single<O> {
    type Requests = OwnerRequests<O>;

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
        self.requests(None, sessions)
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
/// does: a process that is gone left it behind.
fn bind(path: &Path) -> std::io::Result<UnixListener> {
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

/// Bind `path` and serve until `context` is cancelled and every accepted
/// connection, and the sessions they started, have drained.
pub async fn serve<S: Unix>(
    path: &Path,
    snapshot: Arc<S>,
    context: &RunContext,
) -> Result<()> {
    let listener = snapshot.listener();
    let failure = |error: std::io::Error| {
        Error::msg(format!(
            "listener '{}': bind {}: {error}",
            listener.id,
            path.display()
        ))
    };
    let socket = bind(path).map_err(failure)?;
    let _file = SocketFile::owned(path.to_path_buf()).map_err(failure)?;
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
            accepted = socket.accept() => accepted,
        };
        let stream = match accepted {
            Ok((stream, _)) => stream,
            Err(error) => match classify_accept_error(&error) {
                // A peer gone before its accept completed: the socket is fine.
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
            snapshot.clone(),
            sessions.bind(permit),
        ));
    }
    drop(socket);
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

#[cfg(test)]
#[path = "../tests/unix.rs"]
mod tests;
