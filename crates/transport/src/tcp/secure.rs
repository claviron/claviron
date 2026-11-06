//! TLS accept, ALPN selection and Hyper HTTP/1.1 and HTTP/2 connection driving.
//!
//! The listener's owner publishes a [`Secure`] snapshot. A ClientHello's server
//! name selects a TLS configuration and a [`SecureHost`] together, so the host
//! a connection belongs to cannot change underneath it. After the handshake
//! the host's protocol policy and the negotiated ALPN choose the protocol, the
//! connection's facts are completed and the host returns that connection's
//! [`Requests`].

use std::convert::Infallible;
use std::fmt::Display;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use claviron_http::OtherHandler;
use claviron_http::config::transport::TransportPolicy;
use claviron_http::config::{
    ListenerConfig, ListenerSettings, TcpListenerConfig, TcpSecurity,
};
use claviron_http::ext::{ClientAuthStatus, ConnCtx, TlsInfo};
#[cfg(feature = "metrics")]
use claviron_http::metrics::SERVER_METRICS;
use claviron_http::session::{ConnectionScope, SessionSpawner};
use claviron_runtime::registry::{Error, Result};
use claviron_tls::Proto::{self, H1, H2};
use claviron_types::ssl::peer::peer_identity;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::LazyConfigAcceptor;
use tokio_rustls::rustls;
use tokio_rustls::server::TlsStream;
use tracing::{debug, error, warn};

use super::io::{TcpErrors, TcpIo};
use super::listener::TcpListenerSnapshot;
use super::protocol::h1 as make_h1_builder;
use super::{HandlerRequests, Requests};

/// What one TLS listener serves, published as a whole and retained by the
/// connections accepted under it.
pub trait Secure: TcpListenerSnapshot + Send + Sync + 'static {
    /// The owner of one accepted connection, selected by its ClientHello.
    type Host: SecureHost;

    /// The listener's socket, protocol and handshake settings.
    fn config(&self) -> &TcpListenerConfig;

    /// Choose the TLS configuration and the connection's host from the
    /// server name the client offered. An error refuses the handshake.
    fn select(
        self: &Arc<Self>,
        server_name: Option<&str>,
    ) -> io::Result<(Arc<rustls::ServerConfig>, Self::Host)>;
}

/// The owner a TLS connection was accepted for.
pub trait SecureHost: Send + 'static {
    type Requests: Requests;
    type Rejection: Display;

    /// Whether this listener serves the host at all. A refusal drops the
    /// connection after the handshake without an HTTP response.
    fn admit(&self) -> Result<(), Self::Rejection>;

    /// Which protocols the host accepts and how HTTP/2 is offered.
    fn transport(&self) -> &TransportPolicy;

    /// Whether a connection that negotiated no ALPN is refused.
    fn require_alpn(&self) -> bool;

    /// Completes when the connection should end gracefully because its host
    /// was replaced or removed; never, for a host that cannot change.
    fn retired(
        &self,
        protocol: Proto,
    ) -> impl Future<Output = ()> + Send + 'static;

    /// Take the connection's completed facts and return its request owner.
    /// Called once, after the protocol is known and before the first request.
    /// `sessions` owns what outlives a request on this connection.
    fn bind(
        self,
        facts: ConnCtx,
        sessions: SessionSpawner,
    ) -> Result<Self::Requests, Self::Rejection>;
}

/// Why a TLS connection ended without being served to completion.
#[derive(Debug)]
pub enum ConnectionError {
    /// The selected host refused the connection.
    Host(String),
    /// Hyper-level serving error
    Serve(hyper::Error),
    /// Physical TCP failure observed while an H1 response is idle.
    Socket(io::Error),
    /// Timeout in http
    Timeout,
    /// Host policy requires ALPN but client did not negotiate one
    AlpnRequired,
    /// Negotiated/assumed protocol is not allowed by host policy
    ProtocolNotAllowed,
}

impl From<hyper::Error> for ConnectionError {
    fn from(err: hyper::Error) -> Self {
        if err.is_timeout() { Self::Timeout } else { Self::Serve(err) }
    }
}

impl Display for ConnectionError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            ConnectionError::Host(reason) => f.write_str(reason),
            ConnectionError::Socket(err) => {
                write!(f, "Connection socket error: {err}")
            }
            ConnectionError::Serve(err) => {
                write!(f, "Connection serve error: {}", err)
            }
            ConnectionError::AlpnRequired => {
                write!(f, "ALPN is required by host policy")
            }
            ConnectionError::ProtocolNotAllowed => {
                write!(f, "Protocol not allowed by host policy")
            }
            ConnectionError::Timeout => {
                write!(f, "Connection timeout")
            }
        }
    }
}

/// A TLS listener that gives every connection one TLS configuration and every
/// request one handler, whatever server name or authority the client names.
/// No host is selected and nothing is attached to the request.
///
/// A server name selecting a certificate is not a host being selected: the
/// configuration's own certificate resolver may still choose among several
/// certificates by server name, and refuse a name it has none for. What this
/// listener never does is pick a different owner, policy or client
/// authentication for a name.
///
/// The configuration's ALPN list decides what is negotiated; the listener's
/// declared protocols bound it. The listener's request policy is not applied
/// here, and such a connection is never retired by a host change.
pub struct Single<H> {
    listener: Arc<ListenerConfig>,
    config: TcpListenerConfig,
    tls: Arc<rustls::ServerConfig>,
    transport: TransportPolicy,
    handler: Arc<H>,
}

impl<H: OtherHandler> Single<H> {
    /// `listener` must be a TLS TCP listener.
    pub fn new(
        listener: Arc<ListenerConfig>,
        tls: Arc<rustls::ServerConfig>,
        handler: H,
    ) -> Result<Self> {
        let ListenerSettings::Tcp(config) = &listener.settings else {
            return Err(Error::msg(format!(
                "listener '{}': not a TCP listener",
                listener.id
            )));
        };
        if !matches!(config.security, TcpSecurity::Tls { .. }) {
            return Err(Error::msg(format!(
                "listener '{}': not a TLS listener",
                listener.id
            )));
        }
        let config = config.clone();
        Ok(Self {
            listener,
            config,
            tls,
            transport: TransportPolicy::default(),
            handler: Arc::new(handler),
        })
    }
}

impl<H: OtherHandler> TcpListenerSnapshot for Single<H> {
    fn listener(&self) -> &ListenerConfig {
        &self.listener
    }
}

impl<H: OtherHandler> Secure for Single<H> {
    type Host = SingleHost<H>;

    fn config(&self) -> &TcpListenerConfig {
        &self.config
    }
    fn select(
        self: &Arc<Self>,
        _server_name: Option<&str>,
    ) -> io::Result<(Arc<rustls::ServerConfig>, Self::Host)> {
        Ok((self.tls.clone(), SingleHost(self.clone())))
    }
}

/// The owner of every connection of a [`Single`] listener.
pub struct SingleHost<H>(Arc<Single<H>>);

impl<H: OtherHandler> SecureHost for SingleHost<H> {
    type Requests = HandlerRequests<H>;
    type Rejection = Infallible;

    fn admit(&self) -> Result<(), Infallible> {
        Ok(())
    }
    fn transport(&self) -> &TransportPolicy {
        &self.0.transport
    }
    fn require_alpn(&self) -> bool {
        false
    }
    fn retired(
        &self,
        _protocol: Proto,
    ) -> impl Future<Output = ()> + Send + 'static {
        std::future::pending()
    }
    fn bind(
        self,
        _facts: ConnCtx,
        sessions: SessionSpawner,
    ) -> Result<Self::Requests, Infallible> {
        Ok(HandlerRequests { handler: self.0.handler.clone(), sessions })
    }
}

struct AcceptedTls<H> {
    stream: TlsStream<TcpIo>,
    errors: TcpErrors,
    host: H,
}

/// Read the ClientHello, let the snapshot select configuration and host from
/// its server name, then complete the handshake with that configuration.
async fn accept<S: Secure>(
    snapshot: &Arc<S>,
    stream: TcpStream,
) -> io::Result<AcceptedTls<S::Host>> {
    let (stream, errors) = TcpIo::split(stream);
    let start =
        LazyConfigAcceptor::new(rustls::server::Acceptor::default(), stream)
            .await?;
    let hello = start.client_hello();
    debug!(
        listener = ?snapshot.listener().id,
        server_name = hello.server_name().unwrap_or("<absent>"),
        offered_alpn = ?hello.alpn().map(|protocols| {
            protocols.map(String::from_utf8_lossy).collect::<Vec<_>>()
        }),
        "TLS ClientHello ALPN offered",
    );
    let (config, host) = snapshot.select(hello.server_name())?;
    Ok(AcceptedTls { stream: start.into_stream(config).await?, errors, host })
}

/// Decide HTTP protocol (H1/H2) using negotiated ALPN and per-host RequestPolicy
fn decide_protocol(
    alpn: Option<&[u8]>,
    allow_h1: bool,
    allow_h2: bool,
    require_alpn: bool,
) -> Result<Proto, ConnectionError> {
    match (alpn, allow_h1, allow_h2, require_alpn) {
        (Some(b"h2"), _h1, true, _req) => Ok(H2),
        (Some(b"http/1.1"), true, _h2, _req) => Ok(H1),
        (None, _h1, _h2, true) => Err(ConnectionError::AlpnRequired),
        (None, true, _h2, _req) => Ok(H1),
        _ => Err(ConnectionError::ProtocolNotAllowed),
    }
}

/// The physical TLS facts of one accepted connection.
fn tls_info<IO>(tls_stream: &TlsStream<IO>) -> TlsInfo {
    let conn = tls_stream.get_ref().1;
    let protocol_version = conn.protocol_version();
    let cipher_suite =
        conn.negotiated_cipher_suite().map(|suite| suite.suite());
    // A presented certificate was verified by the handshake. Its names are
    // parsed once; a certificate whose names do not parse stays verified
    // but carries no identity facts.
    let leaf = conn.peer_certificates().and_then(|certs| certs.first());
    let client_auth = if leaf.is_some() {
        ClientAuthStatus::Verified
    } else {
        ClientAuthStatus::NotProvided
    };
    let client_cert =
        leaf.and_then(|cert| peer_identity(cert.as_ref()).ok()).map(Arc::new);

    TlsInfo { protocol_version, cipher_suite, client_auth, client_cert }
}

/// Unified stream handler for both HTTP/1.1 and HTTP/2
async fn handle_stream<S: Secure>(
    tls_stream: TlsStream<TcpIo>,
    errors: TcpErrors,
    host: S::Host,
    peer: SocketAddr,
    local: SocketAddr,
    scope: ConnectionScope,
    snapshot: Arc<S>,
) -> Result<ConnEnd, ConnectionError> {
    let policy = host.transport();
    let cfg = snapshot.config();

    // Decide protocol using ALPN + per-host RequestPolicy
    let tls_ref = tls_stream.get_ref().1;
    let alpn = tls_ref.alpn_protocol().map(AsRef::as_ref);
    let proto = decide_protocol(
        alpn,
        policy.allows_protocol(H1) && cfg.protocols.contains(&H1),
        policy.allows_protocol(H2) && cfg.protocols.contains(&H2),
        host.require_alpn(),
    )?;

    // Read before the connection facts are completed and handed on.
    let h2_connect_protocol = policy.h2.connect_protocol;
    debug!("🎭 ALPN negotiated {}", proto.version());
    let scope = scope.with_graceful_signal(host.retired(proto));

    // The actual port the connection arrived on is authoritative for
    // alt-svc and redirect targets without consulting configuration.
    let facts = ConnCtx {
        state: Default::default(),
        listener: snapshot.listener().id.clone(),
        proto: Some(proto),
        remote: peer,
        local_ip: local.ip(),
        local_port: local.port(),
        real_ip: None,
        tls: Some(tls_info(&tls_stream)),
        country_code: crate::geo::country_code(peer.ip()),
    };
    let requests = host
        .bind(facts, scope.sessions())
        .map_err(|refusal| ConnectionError::Host(refusal.to_string()))?;
    let vhost_service = service_fn(move |request| requests.call(request));
    let stream = TokioIo::new(tls_stream);

    // Drive the connection based on protocol.
    let serving = async {
        match proto {
            H2 => {
                let conn_fut =
                    super::protocol::h2(&cfg.h2, h2_connect_protocol)
                        .serve_connection(stream, vhost_service);
                tokio::pin!(conn_fut);

                tokio::select! {
                    biased;
                    () = scope.wait_graceful_shutdown() => {
                        #[cfg(debug_assertions)]
                        debug!("♻️  {} graceful shutdown: sending GOAWAY", proto.version());
                        conn_fut.as_mut().graceful_shutdown();
                        tokio::select! {
                            biased;
                            result = conn_fut.as_mut() => {
                                #[cfg(debug_assertions)]
                                debug!(
                                    "♻️  {} graceful shutdown: connection future finished",
                                    proto.version()
                                );
                                result.map(|_| ConnEnd::Finished).map_err(Into::into)
                            },
                            _ = scope.permit().wait_forced_shutdown() => {
                                #[cfg(debug_assertions)]
                                debug!(
                                    "❗ {} forced shutdown before graceful connection finished",
                                    proto.version()
                                );
                                Ok(ConnEnd::DroppedForceShutdown)
                            }
                        }
                    }
                    result = conn_fut.as_mut() => result.map(|_| ConnEnd::Finished).map_err(Into::into)
                }
            }
            H1 => {
                let conn_fut = make_h1_builder(&cfg.h1)
                    .serve_connection(stream, vhost_service);
                #[cfg(feature = "websocket")]
                let conn_fut = conn_fut.with_upgrades();
                tokio::pin!(conn_fut);

                tokio::select! {
                    biased;
                    () = scope.wait_graceful_shutdown() => {
                        conn_fut.as_mut().graceful_shutdown();
                        tokio::select! {
                            biased;
                            result = conn_fut.as_mut() => result.map(|_| ConnEnd::Finished).map_err(Into::into),
                            _ = scope.permit().wait_forced_shutdown() => Ok(ConnEnd::DroppedForceShutdown)
                        }
                    }
                    result = conn_fut.as_mut() => result.map(|_| ConnEnd::Finished).map_err(Into::into)
                }
            }
            Proto::H3 => {
                unreachable!("H3 cannot appear in TLS/TCP connections")
            }
        }
    };
    // Only an H1 connection with half-close leaves its read side idle
    // during a request; every other case sees a socket error itself.
    if proto == H1 && cfg.h1.half_close {
        tokio::select! {
            biased;
            error = errors.failed() => Err(ConnectionError::Socket(error)),
            result = serving => result,
        }
    } else {
        serving.await
    }
}

/// Serve one accepted stream until its peer completes or its scope drains.
/// The caller owns acceptance, connection capacity and session drain; nothing
/// is bound and no new permit is taken here.
pub async fn serve<S: Secure>(
    tcp_stream: TcpStream,
    socket_addr: SocketAddr,
    snap: Arc<S>,
    scope: ConnectionScope,
) {
    if let Err(error) = super::protocol::socket(&tcp_stream, &snap.config().tcp)
    {
        error!(listener = %snap.listener().id, %error, "TCP socket setup failed");
        return;
    }
    let local_addr = match tcp_stream.local_addr() {
        Ok(addr) => addr,
        Err(error) => {
            error!(%error, "TCP local address unavailable");
            return;
        }
    };
    let TcpSecurity::Tls { handshake_timeout } = snap.config().security else {
        unreachable!("TLS group required")
    };

    // TLS handshake with timeout
    let tls_result = tokio::select! {
        biased;
        _ = scope.permit().wait_forced_shutdown() => return,
        result = tokio::time::timeout(handshake_timeout, accept(&snap, tcp_stream)) => result
    };

    match tls_result {
        Ok(Ok(accepted)) => {
            let tls_stream = accepted.stream;
            debug!(
                "🔐 TLS handshake complete, client connected from {}",
                socket_addr
            );

            let host = accepted.host;
            if let Err(e) = host.admit() {
                #[cfg(feature = "metrics")]
                SERVER_METRICS.vhost_rejected();
                debug!("🚫 Connection from {} dropped {}", socket_addr, e);

                // Intentionally drop the connection without sending any HTTP response.
                // This is nginx-444-like at the HTTP layer (TLS has already completed).
                return;
            }

            // Track connection metrics with RAII guard
            #[cfg(feature = "metrics")]
            let _metrics_guard = SERVER_METRICS.conn_open();

            // Use the unified handle_stream function
            let res = handle_stream(
                tls_stream,
                accepted.errors,
                host,
                socket_addr,
                local_addr,
                scope,
                snap.clone(),
            )
            .await;

            match res {
                Ok(ConnEnd::Finished) => {
                    debug!(
                        "🔹 Connection served successfully to {}",
                        socket_addr
                    )
                }
                Ok(ConnEnd::DroppedForceShutdown) => {
                    warn!(
                        "⚠️  HTTPS connection dropped from {} due to forced shutdown",
                        socket_addr
                    );
                }
                Err(err) => {
                    debug!(
                        "❌ HTTPS Connection closed from {} error: {}",
                        socket_addr, err
                    );
                }
            }
        }
        Ok(Err(e)) => {
            #[cfg(feature = "metrics")]
            SERVER_METRICS.tls_failed();
            debug!("🚫 handshake failed from {}: {}", socket_addr, e);
        }
        Err(_) => {
            #[cfg(feature = "metrics")]
            SERVER_METRICS.tls_failed();
            debug!(
                "⏱️  Handshake timeout from {} (timeout: {:?})",
                socket_addr, handshake_timeout
            );
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConnEnd {
    Finished,
    DroppedForceShutdown, // dropped due to forced shutdown
}

#[cfg(test)]
#[path = "../../tests/tcp/secure.rs"]
mod tests;
