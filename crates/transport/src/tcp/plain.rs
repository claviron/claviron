//! Driving one cleartext HTTP/1.1 connection.
//!
//! The listener's owner publishes a [`Cleartext`] snapshot. For each accepted
//! stream this module sets up the socket, completes the connection's facts,
//! asks the snapshot for that connection's [`Requests`] and drives Hyper until
//! the peer or a shutdown ends it. Which host a request belongs to, if hosts
//! exist at all, is the snapshot's business.

use std::net::SocketAddr;
use std::sync::Arc;

use claviron_http::config::{
    H1Config, ListenerConfig, ListenerSettings, TcpConfig, TcpSecurity,
};
use claviron_http::ext::ConnCtx;
use claviron_http::listener::HttpProtocol;
use claviron_http::session::{ConnectionScope, SessionSpawner};
use claviron_runtime::registry::{Error, Result};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tracing::{debug, warn};

use super::OwnerRequests;
pub use super::Requests;
use super::listener::TcpListenerSnapshot;
use super::prior::PriorKnowledge;
use crate::owner::Owner;

/// What one cleartext listener serves, published as a whole and retained by
/// the connections accepted under it.
pub trait Cleartext: TcpListenerSnapshot + Send + Sync + 'static {
    /// One accepted connection's requests.
    type Requests: Requests;

    fn tcp(&self) -> &TcpConfig;
    fn h1(&self) -> &H1Config;

    /// What this listener accepts besides plain HTTP/1.1. A listener without
    /// any keeps the HTTP/1.1 driver alone.
    fn prior_knowledge(&self) -> Option<&PriorKnowledge> {
        None
    }

    /// Called once for each accepted connection, with its completed facts and
    /// before its first request. Work done here is not repeated per request.
    /// `sessions` owns what outlives a request on this connection.
    fn connection(
        self: &Arc<Self>,
        facts: Arc<ConnCtx>,
        sessions: SessionSpawner,
    ) -> Self::Requests;
}

/// A cleartext listener that gives every request to one [`Owner`], whatever
/// authority the request names. No host is selected.
///
/// The driver applies no request policy; that is the owner's. A compiled site
/// enforces the listener's as it was prepared with it, and a
/// [`Handler`](crate::owner::Handler) that needs a body limit enforces its own.
pub struct Single<O> {
    listener: Arc<ListenerConfig>,
    tcp: TcpConfig,
    h1: H1Config,
    prior: Option<PriorKnowledge>,
    owner: Arc<O>,
}

impl<O: Owner> Single<O> {
    /// `listener` must be a cleartext TCP listener; its socket and HTTP/1.1
    /// settings are taken as declared.
    pub fn new(
        listener: Arc<ListenerConfig>,
        owner: O,
    ) -> Result<Self> {
        Self::shared(listener, Arc::new(owner))
    }

    /// The same, for an owner shared with other listeners.
    pub fn shared(
        listener: Arc<ListenerConfig>,
        owner: Arc<O>,
    ) -> Result<Self> {
        let ListenerSettings::Tcp(config) = &listener.settings else {
            return Err(Error::msg(format!(
                "listener '{}': not a TCP listener",
                listener.id
            )));
        };
        if !matches!(config.security, TcpSecurity::Cleartext { .. }) {
            return Err(Error::msg(format!(
                "listener '{}': not a cleartext listener",
                listener.id
            )));
        }
        let (tcp, h1) = (config.tcp.clone(), config.h1.clone());
        Ok(Self { listener, tcp, h1, prior: None, owner })
    }
}

impl<O: Owner> Single<O> {
    /// Accept cleartext HTTP/2 from clients that speak it from their first
    /// byte; `h1` keeps HTTP/1.1 on the same socket.
    pub fn prior_knowledge(
        mut self,
        h1: bool,
    ) -> Self {
        let ListenerSettings::Tcp(config) = &self.listener.settings else {
            unreachable!("checked when constructed");
        };
        self.prior = Some(PriorKnowledge { h2: config.h2.clone(), h1 });
        self
    }

    pub(crate) fn requests(
        &self,
        connection: Option<Arc<ConnCtx>>,
        sessions: SessionSpawner,
    ) -> OwnerRequests<O> {
        OwnerRequests::new(self.owner.clone(), connection, sessions)
    }
}

impl<O: Owner> TcpListenerSnapshot for Single<O> {
    fn listener(&self) -> &ListenerConfig {
        &self.listener
    }
}

impl<O: Owner> Cleartext for Single<O> {
    type Requests = OwnerRequests<O>;

    fn tcp(&self) -> &TcpConfig {
        &self.tcp
    }
    fn h1(&self) -> &H1Config {
        &self.h1
    }
    fn connection(
        self: &Arc<Self>,
        facts: Arc<ConnCtx>,
        sessions: SessionSpawner,
    ) -> Self::Requests {
        self.requests(Some(facts), sessions)
    }
    fn prior_knowledge(&self) -> Option<&PriorKnowledge> {
        self.prior.as_ref()
    }
}

/// Serve one accepted stream until its peer completes or its scope drains.
/// The caller owns acceptance, connection capacity and session drain; nothing
/// is bound and no new permit is taken here.
pub async fn serve<S: Cleartext>(
    stream: TcpStream,
    peer: SocketAddr,
    snapshot: Arc<S>,
    scope: ConnectionScope,
) {
    let listener = &snapshot.listener().id;
    if let Err(error) = super::protocol::socket(&stream, snapshot.tcp()) {
        warn!(%listener, %error, "TCP socket setup failed");
        return;
    }
    let local = match stream.local_addr() {
        Ok(local) => local,
        Err(error) => {
            warn!(%listener, %error, "H1 local address unavailable");
            return;
        }
    };
    let ctx = Arc::new(ConnCtx {
        state: Default::default(),
        listener: listener.clone(),
        proto: Some(HttpProtocol::H1),
        remote: peer,
        local_ip: local.ip(),
        local_port: local.port(),
        real_ip: None,
        country_code: crate::geo::country_code(peer.ip()),
        tls: None,
    });
    let requests = snapshot.connection(ctx, scope.sessions());
    let service = service_fn(move |request| requests.call(request));
    if let Some(prior) = snapshot.prior_knowledge() {
        let result =
            super::prior::drive(stream, service, prior, snapshot.h1(), &scope)
                .await;
        if let Some(Err(error)) = result {
            debug!(%listener, %error, "cleartext connection ended");
        }
        return;
    }
    let (stream, errors) = super::io::TcpIo::split(stream);
    let connection = super::protocol::h1(snapshot.h1())
        .serve_connection(TokioIo::new(stream), service);
    #[cfg(feature = "websocket")]
    let connection = connection.with_upgrades();
    tokio::pin!(connection);
    let serving = async {
        tokio::select! {
            biased;
            _ = scope.permit().wait_graceful_shutdown() => {
                connection.as_mut().graceful_shutdown();
                tokio::select! {
                    result = connection.as_mut() => Some(result),
                    _ = scope.permit().wait_forced_shutdown() => None
                }
            }
            result = connection.as_mut() => Some(result)
        }
    };
    // Hyper reads the socket in the middle of a request unless half-close is
    // on, and then sees an error itself. Only half-close needs the observer.
    let result = if snapshot.h1().half_close {
        tokio::select! {
            biased;
            error = errors.failed() => {
                debug!(%listener, %error, "H1 socket failed");
                None
            },
            result = serving => result,
        }
    } else {
        drop(errors);
        serving.await
    };
    if let Some(Err(error)) = result
        && !error.is_user()
    {
        debug!(%listener, %error, "H1 connection ended");
    }
}

#[cfg(test)]
#[path = "../../tests/tcp/plain.rs"]
mod tests;
