//! TCP bind/admission/drain owner shared by the cleartext and TLS drivers.

use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use claviron_http::config::{ListenerConfig, ListenerSettings};
use claviron_http::session::{ConnectionScope, SessionOwner};
use claviron_runtime::gate::{Gate, GateDrainOutcome};
use claviron_runtime::registry::{Error, Result};
use claviron_runtime::{RunContext, RunMode};
use claviron_sys::net::{PortSharing, create_tcp_listener};
use futures_util::stream::{SelectAll, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

mod accept;
use accept::{AcceptSource, SocketAccept};

pub trait TcpListenerSnapshot {
    fn listener(&self) -> &ListenerConfig;
}

/// Serve one snapshot for the listener's whole life.
pub async fn serve<S, F, Fut>(
    snapshot: Arc<S>,
    context: &RunContext,
    run_mode: RunMode,
    handle: F,
) -> Result<()>
where
    S: TcpListenerSnapshot + Send + Sync + 'static,
    F: Fn(Arc<S>, TcpStream, SocketAddr, ConnectionScope) -> Fut + Clone,
    Fut: Future<Output = ()> + Send + 'static,
{
    let current = snapshot.clone();
    serve_current(
        move || Some(current.clone()),
        snapshot,
        context,
        run_mode,
        handle,
    )
    .await
}

/// Serve what `current` holds when each connection arrives, for a listener
/// whose snapshot is replaced while it runs. Sockets, admission and drain stay
/// those of `initial`.
pub async fn serve_current<S, C, F, Fut>(
    current: C,
    initial: Arc<S>,
    context: &RunContext,
    run_mode: RunMode,
    handle: F,
) -> Result<()>
where
    S: TcpListenerSnapshot + Send + Sync + 'static,
    C: Fn() -> Option<Arc<S>>,
    F: Fn(Arc<S>, TcpStream, SocketAddr, ConnectionScope) -> Fut + Clone,
    Fut: Future<Output = ()> + Send + 'static,
{
    // Bind everything before starting any accept task. A later bind failure
    // drops earlier sockets; it cannot leave detached partial listeners.
    let config = initial.listener();
    let ListenerSettings::Tcp(tcp) = &config.settings else {
        return Err(Error::msg(format!(
            "listener '{}': not a TCP listener",
            config.id
        )));
    };
    let mut sockets = Vec::new();
    for addr in &config.listen {
        let socket = bind_socket(*addr, run_mode, tcp.tcp.backlog)
            .await
            .map_err(|error| {
                Error::msg(format!(
                    "listener '{}': bind {addr}: {error}",
                    config.id
                ))
            })?;
        if matches!(run_mode, RunMode::Worker(_)) {
            debug!(listener = %config.id, address = %socket.local_addr()?, "🌐 TCP listener bound");
        } else {
            info!(listener = %config.id, address = %socket.local_addr()?, "🌐 TCP listener bound");
        }
        sockets.push(socket);
    }
    let owned = context.child();
    serve_group(current, initial, sockets, &owned, handle).await
}

async fn bind_socket(
    addr: SocketAddr,
    run_mode: RunMode,
    backlog: NonZeroU32,
) -> std::io::Result<TcpListener> {
    let sharing = match run_mode {
        RunMode::Worker(_) => PortSharing::Shared,
        RunMode::Standalone | RunMode::Master => PortSharing::Exclusive,
    };
    create_tcp_listener(addr, sharing, backlog).await
}

async fn serve_group<S, C, A, F, Fut>(
    current: C,
    initial: Arc<S>,
    sockets: Vec<A>,
    context: &RunContext,
    handle: F,
) -> Result<()>
where
    S: TcpListenerSnapshot + Send + Sync + 'static,
    C: Fn() -> Option<Arc<S>>,
    A: AcceptSource,
    F: Fn(Arc<S>, TcpStream, SocketAddr, ConnectionScope) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let config = initial.listener();
    let ListenerSettings::Tcp(tcp) = &config.settings else {
        context.cancellation_token().cancel();
        return Err(Error::msg(format!(
            "listener '{}': not a TCP listener",
            config.id
        )));
    };
    let mut accepts = SelectAll::new();
    for socket in sockets {
        match SocketAccept::new(
            socket,
            config.id.clone(),
            tcp.tcp.accept_retry.clone(),
        ) {
            Ok(socket) => accepts.push(socket),
            Err(error) => {
                context.cancellation_token().cancel();
                return Err(error.into());
            }
        }
    }
    let gate = Gate::new(
        config.limits.max_connections.map(|value| value.get()),
        config.limits.acquire_timeout,
    );
    let mut connections = JoinSet::new();
    let sessions = SessionOwner::new(config.id.clone());
    let mut failure = None;
    loop {
        tokio::select! {
            biased;
            _ = context.cancelled() => break,
            finished = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = finished { error!(listener = %config.id, %error, "TCP connection task failed"); }
            }
            Some(result) = accepts.next() => {
                let (stream, peer) = match result {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        failure = Some(error.into());
                        context.cancellation_token().cancel();
                        break;
                    }
                };
                // A single admission owner for all sockets in the group.
                // Independent groups continue accepting while this gate waits.
                let permit = tokio::select! {
                    biased;
                    _ = context.cancelled() => break,
                    entered = async {
                        if config.limits.max_connections.is_some() {
                            gate.enter().await
                        } else {
                            // Unlimited listeners track ownership for drain only.
                            gate.try_enter()
                        }
                    } => match entered {
                        Ok(permit) => permit,
                        Err(error) => { debug!(listener = %config.id, ?error, "TCP admission rejected"); continue; }
                    }
                };
                let Some(snapshot) = current() else { continue; };
                #[cfg(feature = "metrics")]
                claviron_http::metrics::SERVER_METRICS.tcp_accept();
                connections.spawn(handle(snapshot, stream, peer, sessions.bind(permit)));
            }
        }
    }
    drop(accepts);
    let mut sessions = sessions.close();
    gate.graceful_shutdown(config.limits.graceful_timeout);
    if let GateDrainOutcome::Forced { remaining } = gate.wait_all_done().await {
        warn!(listener = %config.id, remaining, "TCP graceful drain expired; forcing accepted tasks");
        connections.abort_all();
        sessions.abort_all();
    }
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result
            && !error.is_cancelled()
        {
            error!(listener = %config.id, %error, "TCP connection task failed during drain");
        }
    }
    sessions.join().await;
    debug!(listener = %config.id, "🔸 TCP listener stopped");
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
