use std::net::SocketAddr;
use std::sync::Arc;

use crate::transport::HostBinding;
use claviron_http::config::{H2Config, TcpSecurity};
#[cfg(feature = "metrics")]
use claviron_http::metrics::SERVER_METRICS;
use claviron_http::session::ConnectionScope;
use claviron_tls::Proto::{self, H1, H2};
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tracing::{debug, error, info, warn};

use super::HttpsServer;
use super::conn::TlsHostConnection;
use super::error::ConnectionError;
use super::server::ServerSnapshot;
use crate::tcp::protocol::h1 as make_h1_builder;

/// Build the hyper HTTP/2 connection builder from a snapshot config slice.
fn make_h2_builder(
    h2_cfg: &H2Config,
    enable_connect_protocol: bool,
) -> http2::Builder<TokioExecutor> {
    let mut builder = http2::Builder::new(TokioExecutor::new());
    builder
        .auto_date_header(true)
        .timer(TokioTimer::new())
        .keep_alive_timeout(h2_cfg.keep_alive_timeout)
        .keep_alive_interval(h2_cfg.keep_alive_interval)
        .max_concurrent_streams(h2_cfg.max_concurrent_streams)
        .adaptive_window(true)
        .max_frame_size(h2_cfg.max_frame_size)
        .max_header_list_size(h2_cfg.max_header_list_size)
        .max_pending_accept_reset_streams(
            h2_cfg.max_pending_accept_reset_streams,
        )
        .max_local_error_reset_streams(h2_cfg.max_local_error_reset_streams)
        .max_send_buf_size(h2_cfg.max_send_buf_size);
    if enable_connect_protocol {
        builder.enable_connect_protocol();
    }
    builder
}

/// Complete when this connection's host binding retires; never without one.
async fn host_retired(
    binding: Option<HostBinding>,
    proto: Proto,
) {
    match binding {
        Some(mut binding) => {
            binding.retired().await;
            info!(
                protocol = %proto.version(),
                host = %binding.hostname(),
                canonical = %binding.canonical(),
                "🔀 host mapping or configuration changed: retiring this connection gracefully"
            );
        }
        None => std::future::pending().await,
    }
}

impl HttpsServer {
    /// Decide HTTP protocol (H1/H2) using negotiated ALPN and per-host RequestPolicy
    fn decide_protocol(
        &self,
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

    /// Unified stream handler for both HTTP/1.1 and HTTP/2
    async fn handle_stream(
        self: Arc<Self>,
        tls_stream: TlsStream<crate::tcp::io::TcpIo>,
        vhost_conn: TlsHostConnection,
        scope: ConnectionScope,
        snapshot: Arc<ServerSnapshot>,
    ) -> Result<ConnEnd, ConnectionError> {
        let policy = vhost_conn.host_config().transport_policy();
        let cfg = &snapshot.config;

        // Decide protocol using ALPN + per-host RequestPolicy
        let tls_ref = tls_stream.get_ref().1;
        let alpn = tls_ref.alpn_protocol().map(AsRef::as_ref);
        let proto = self.decide_protocol(
            alpn,
            policy.allows_protocol(H1) && cfg.protocols.contains(&H1),
            policy.allows_protocol(H2) && cfg.protocols.contains(&H2),
            vhost_conn.host_config().tls.require_alpn,
        )?;

        debug!("🎭 ALPN negotiated {}", proto.version());
        let scope = scope.with_graceful_signal(host_retired(
            vhost_conn.binding(&self.state),
            proto,
        ));

        // HostService checks the request head before this transport hands off
        // an H2 CONNECT or adapts Incoming for ordinary application dispatch.
        let vhost_service = vhost_conn
            .create_service(
                &self.state,
                proto,
                &snapshot.listener,
                snapshot.deny.clone(),
                scope.sessions(),
            )?
            .into_service();
        let errors = tls_stream.get_ref().0.clone();
        let stream = TokioIo::new(tls_stream);

        // Drive the connection based on protocol.
        let serving = async {
            match proto {
                H2 => {
                    let conn_fut =
                        make_h2_builder(&cfg.h2, policy.h2.connect_protocol)
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
                    if !policy.h1.upgrade {
                        // No upgrades: plain http1::Connection
                        let conn_fut = make_h1_builder(&cfg.h1)
                            .serve_connection(stream, vhost_service);
                        tokio::pin!(conn_fut);

                        tokio::select! {
                            biased;
                            () = scope.wait_graceful_shutdown() => {
                                #[cfg(debug_assertions)]
                                debug!("♻️ {} graceful shutdown", proto.version());
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
                    } else {
                        // Upgrades enabled: http1::UpgradeableConnection
                        let conn_fut = make_h1_builder(&cfg.h1)
                            .serve_connection(stream, vhost_service)
                            .with_upgrades();

                        tokio::pin!(conn_fut);

                        tokio::select! {
                            biased;
                            () = scope.wait_graceful_shutdown() => {
                                #[cfg(debug_assertions)]
                                debug!("♻️ {} graceful shutdown", proto.version());
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
                }
                Proto::H3 => {
                    unreachable!("H3 cannot appear in TLS/TCP connections")
                }
            }
        };
        if proto == H1 {
            tokio::select! {
                biased;
                error = errors.failed() => Err(ConnectionError::Socket(error)),
                result = serving => result,
            }
        } else {
            serving.await
        }
    }

    pub(crate) async fn handle_connection(
        self: Arc<Self>,
        snap: Arc<ServerSnapshot>,
        tcp_stream: TcpStream,
        socket_addr: SocketAddr,
        scope: ConnectionScope,
    ) {
        if let Err(error) =
            crate::tcp::protocol::socket(&tcp_stream, &snap.config.tcp)
        {
            error!(listener = %snap.listener.id, %error, "TCP socket setup failed");
            return;
        }
        let local_addr = match tcp_stream.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                error!(%error, "TCP local address unavailable");
                return;
            }
        };
        let TcpSecurity::Tls { handshake_timeout } = snap.config.security
        else {
            unreachable!("TLS group required")
        };

        // TLS handshake with timeout
        let tls_result = tokio::select! {
            biased;
            _ = scope.permit().wait_forced_shutdown() => return,
            result = tokio::time::timeout(handshake_timeout, snap.tls_accept.accept(tcp_stream)) => result
        };

        match tls_result {
            Ok(Ok(accepted)) => {
                let tls_stream = accepted.stream;
                debug!(
                    "🔐 TLS handshake complete, client connected from {}",
                    socket_addr
                );

                let vhost_conn = match TlsHostConnection::from_tls_stream(
                    &tls_stream,
                    socket_addr,
                    local_addr,
                    &snap.listener,
                    accepted.host,
                ) {
                    Err(e) => {
                        #[cfg(feature = "metrics")]
                        SERVER_METRICS.vhost_rejected();
                        debug!(
                            "🚫 Connection from {} dropped {}",
                            socket_addr, e
                        );

                        // Intentionally drop the connection without sending any HTTP response.
                        // This is nginx-444-like at the HTTP layer (TLS has already completed).
                        return;
                    }
                    Ok(conn) => conn,
                };

                // Track connection metrics with RAII guard
                #[cfg(feature = "metrics")]
                let _metrics_guard = SERVER_METRICS.conn_open();

                // Use the unified handle_stream function
                let res = self
                    .clone()
                    .handle_stream(tls_stream, vhost_conn, scope, snap.clone())
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
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ConnEnd {
    Finished,
    DroppedForceShutdown, // dropped due to forced shutdown
}
