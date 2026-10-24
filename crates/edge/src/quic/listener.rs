//! QUIC group bind/admission/drain and connection/stream ownership.
//! Each accepted connection retains its named group snapshot through TLS,
//! host resolution and dispatch. Request body adaptation remains in `request`.

mod drain;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::transport::{HostIngress, HostService, RequestArrival};
use bytes::Bytes;
use claviron_http::config::ListenerSettings;
use claviron_http::ext::{
    ClientAuthStatus, ConnCtx, TlsClientCertInfo, TlsInfo,
};
#[cfg(feature = "metrics")]
use claviron_http::metrics::SERVER_METRICS;
use claviron_runtime::gate::{Gate, GateDrainOutcome, Permit};
use claviron_runtime::registry::{Error, Result};
use claviron_runtime::{RunContext, RunMode};
use claviron_sys::net::{PortSharing, create_udp_socket};
use claviron_tls::Proto::H3;
use claviron_types::ssl::{
    parsers::take_first_cert_from_der, peer::peer_identity,
};
use futures_util::FutureExt;
use futures_util::stream::{FuturesUnordered, StreamExt};
use h3_quinn::Connection as H3QuinnConnection;
use h3_webtransport::server::{AcceptedBi, IncomingStream};
use http::{Method, StatusCode};
use quinn::crypto::rustls::HandshakeData;
use quinn::{Connection as QuinnConnection, Endpoint, VarInt};
use rustls::ProtocolVersion;
use rustls::pki_types::CertificateDer;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::quic::request::{
    H3RequestHandler, RequestBodyMode, send_host_response, send_status_response,
};
use crate::quic::types::{H3Server, ServerSnapshot};

enum StreamCompletion {
    Request,
    Session(h3_webtransport::SessionId),
}

fn bind_socket(
    addr: SocketAddr,
    run_mode: RunMode,
) -> std::io::Result<std::net::UdpSocket> {
    let sharing = match run_mode {
        RunMode::Worker(_) => PortSharing::Shared,
        RunMode::Standalone | RunMode::Master => PortSharing::Exclusive,
    };
    create_udp_socket(addr, sharing)
}

impl H3Server {
    pub(crate) async fn run_listeners(
        self: Arc<Self>,
        deps: Arc<crate::quic::types::Deps>,
        context: RunContext,
    ) -> Result<()> {
        let groups = crate::quic::types::build_snapshots(
            self.state.config_reader().snapshot::<Self>()?.as_ref(),
            &deps,
        )?;
        let generation = self.current.start(groups.clone())?;
        crate::quic::types::prepare_vhosts(&deps, &groups)?;
        let mut bound = Vec::new();
        // Complete all binds before any task can accept traffic.
        for snapshot in generation.groups().values() {
            let mut endpoints = Vec::new();
            for addr in &snapshot.listener.listen {
                let socket = bind_socket(*addr, self.state.run_mode())
                    .map_err(|error| {
                        Error::msg(format!(
                            "H3 listener '{}': bind {addr}: {error}",
                            snapshot.listener.id
                        ))
                    })?;
                let endpoint = Endpoint::new(
                    Default::default(),
                    Some(snapshot.tls_accept.base_config()),
                    socket,
                    Arc::new(quinn::TokioRuntime),
                )
                .map_err(|error| {
                    Error::msg(format!(
                        "H3 listener '{}': endpoint {addr}: {error}",
                        snapshot.listener.id
                    ))
                })?;
                if self.state.is_worker() {
                    debug!(listener = %snapshot.listener.id, address = %endpoint.local_addr()?, "🌐 QUIC listener bound");
                } else {
                    info!(listener = %snapshot.listener.id, address = %endpoint.local_addr()?, "🌐 QUIC listener bound");
                }
                endpoints.push(endpoint);
            }
            bound.push((snapshot.clone(), endpoints));
        }
        if bound.is_empty() {
            context.cancelled().await;
            return Ok(());
        }
        let mut running = FuturesUnordered::new();
        for (snapshot, endpoints) in bound {
            running.push(self.run_group(
                snapshot,
                endpoints,
                &context,
                deps.clone(),
            ));
        }
        while running.next().await.is_some() {}
        Ok(())
    }

    async fn run_group(
        &self,
        initial: Arc<ServerSnapshot>,
        endpoints: Vec<Endpoint>,
        context: &RunContext,
        deps: Arc<crate::quic::types::Deps>,
    ) {
        let listener = &initial.listener;
        let gate = Gate::new(
            listener.limits.max_connections.map(|value| value.get()),
            listener.limits.acquire_timeout,
        );
        let mut accepts = FuturesUnordered::new();
        let mut connections = JoinSet::new();
        for endpoint in &endpoints {
            accepts.push(accept(endpoint));
        }
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => break,
                result = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = result { error!(listener = %listener.id, %error, "QUIC connection task failed"); }
                }
                Some((endpoint, incoming)) = accepts.next() => {
                    let Some(incoming) = incoming else { break; };
                    accepts.push(accept(endpoint));
                    let permit = tokio::select! {
                        biased;
                        _ = context.cancelled() => { incoming.refuse(); break; },
                        result = gate.enter() => match result {
                            Ok(permit) => permit,
                            Err(error) => { debug!(listener = %listener.id, ?error, "QUIC admission refused"); incoming.refuse(); continue; }
                        }
                    };
                    let Some(snapshot) = self.current.get(&listener.id) else { incoming.refuse(); continue; };
                    let local_addr = match endpoint.local_addr() {
                        Ok(addr) => addr,
                        Err(error) => { error!(%error, "QUIC local address unavailable"); incoming.refuse(); continue; }
                    };
                    #[cfg(feature = "metrics")]
                    SERVER_METRICS.udp_accept();
                    let state = self.state.clone();
                    let deps = deps.clone();
                    connections.spawn(async move {
                        let handshake_timeout = snapshot.config.handshake_timeout;
                        let result = tokio::select! {
                            biased;
                            _ = permit.wait_forced_shutdown() => return,
                            result = tokio::time::timeout(handshake_timeout, snapshot.tls_accept.accept(incoming)) => result
                        };
                        let conn = match result {
                            Ok(Ok(conn)) => conn,
                            Ok(Err(error)) => {
                                #[cfg(feature = "metrics")]
                                SERVER_METRICS.tls_failed();
                                debug!(listener = %snapshot.listener.id, %error, "H3 handshake rejected");
                                return;
                            }
                            Err(_) => {
                                #[cfg(feature = "metrics")]
                                SERVER_METRICS.tls_failed();
                                debug!(listener = %snapshot.listener.id, ?handshake_timeout, "H3 handshake timeout");
                                return;
                            }
                        };
                        Self::handle_connection(state, deps, conn, permit, local_addr, snapshot).await;
                    });
                }
            }
        }
        drop(accepts);
        // Stop accepting new handshakes, but keep endpoints alive while accepted
        // connections drain. Closing here would bypass graceful H3 shutdown.
        for endpoint in &endpoints {
            endpoint.set_server_config(None);
        }
        gate.graceful_shutdown(listener.limits.graceful_timeout);
        if let GateDrainOutcome::Forced { remaining } =
            gate.wait_all_done().await
        {
            warn!(listener = %listener.id, remaining, "QUIC graceful drain expired; forcing accepted tasks");
            // Connection owners observe the forced signal themselves. Aborting
            // them here would drop their JoinSets before child cleanup is joined.
        }
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result
                && !error.is_cancelled()
            {
                error!(%error, "QUIC task failed during drain");
            }
        }
        for endpoint in endpoints {
            endpoint.close(VarInt::from_u32(0), b"listener stopped");
            endpoint.wait_idle().await;
        }
        info!(listener = %listener.id, "🔸 QUIC listener stopped");
    }

    /// Drive a single QUIC connection: SNI → vhost → AppKind dispatch.
    /// Snapshot, dependencies and bound address are captured at admission.
    async fn handle_connection(
        state: claviron_runtime::AppState,
        deps: Arc<crate::quic::types::Deps>,
        accepted: crate::quic::tls::AcceptedQuic,
        permit: Permit,
        local_addr: SocketAddr,
        snapshot: Arc<ServerSnapshot>,
    ) {
        let conn_for_close = accepted.connection.clone();
        let permit = Arc::new(permit);
        let mut streams = JoinSet::new();
        #[cfg(feature = "metrics")]
        let _metrics_guard = SERVER_METRICS.conn_open();
        // The driver only borrows its children. Force or unwind drops the
        // driver first, then the outer owner aborts/joins every stream/session.
        let exit = drain::drive(
            Self::accept_streams(
                state,
                deps,
                accepted,
                &permit,
                local_addr,
                snapshot,
                &mut streams,
            ),
            &permit,
        )
        .await;
        drain::finish(streams, &permit, exit, || {
            conn_for_close.close(VarInt::from_u32(0), b"connection stopped");
        })
        .await;
        // Capacity is retained until all owned child futures have been dropped.
        drop(permit);
    }

    async fn accept_streams(
        state: claviron_runtime::AppState,
        deps: Arc<crate::quic::types::Deps>,
        accepted: crate::quic::tls::AcceptedQuic,
        permit: &Arc<Permit>,
        local_addr: SocketAddr,
        snapshot: Arc<ServerSnapshot>,
        streams: &mut JoinSet<StreamCompletion>,
    ) {
        use crate::quic::wt::{
            H3Connection, SessionStreams, authorize_web_transport,
            is_web_transport_connect, serve_web_transport,
        };

        let conn = accepted.connection;
        let direct_client = snapshot.listener.proxy.is_none();

        let conn_for_close = conn.clone();
        let remote: SocketAddr = conn.remote_address();
        let local_ip = conn.local_ip().unwrap_or(local_addr.ip());

        debug!("🆕 QUIC connection accepted from={}", remote.ip());

        // Pull SNI out of the QUIC/TLS handshake. Keep the downcasted snapshot
        // available so TLS metadata can be derived without another call to
        // `handshake_data()`.
        let handshake_data = conn
            .handshake_data()
            .and_then(|d| d.downcast::<HandshakeData>().ok());
        let server_name: Option<Arc<str>> = handshake_data
            .as_ref()
            .and_then(|hd| hd.server_name.as_deref().map(Arc::<str>::from));

        let vctx = match accepted.hosts.resolve_sni(server_name.as_deref()) {
            Ok(ext) => {
                if !ext.vhost_config.allows_transport(&snapshot.listener.id, H3)
                {
                    warn!(
                        "H3 not allowed for vhost '{}', dropping connection",
                        server_name.as_deref().unwrap_or("unknown")
                    );
                    let code = VarInt::try_from(
                        h3::error::Code::H3_REQUEST_REJECTED.value(),
                    )
                    .unwrap_or_else(|_| VarInt::from_u32(0));
                    conn_for_close.close(code, b"HTTP/3 not allowed");
                    return;
                }
                ext
            }
            Err(e) => {
                #[cfg(feature = "metrics")]
                SERVER_METRICS.vhost_rejected();
                debug!("H3 vhost resolution failed: {e}, dropping connection");
                conn_for_close.close(VarInt::from_u32(0), b"SNI required");
                return;
            }
        };

        let app = match vctx
            .application
            .clone()
            .or_else(|| deps.app_resolver.resolve(&vctx.vhost_key))
        {
            Some(app) => app,
            None => {
                warn!(
                    "H3 vhost key has no registered app: {:?}",
                    vctx.vhost_key
                );
                conn_for_close.close(VarInt::from_u32(0), b"vhost has no app");
                return;
            }
        };

        let conn_ctx = Arc::new(ConnCtx {
            state: Default::default(),
            listener: snapshot.listener.id.clone(),
            proto: Some(H3),
            local_ip,
            remote,
            local_port: local_addr.port(),
            real_ip: if direct_client { Some(remote.ip()) } else { None },
            country_code: country_code_for(remote),
            tls: Some(h3_tls_info(&conn, handshake_data.as_deref())),
        });

        let host_service = HostService::new(
            app,
            conn_ctx,
            vctx.clone(),
            HostIngress {
                proxy: snapshot.listener.proxy.clone(),
                deny: snapshot.deny.clone(),
            },
            deps.vhost_resolver.clone(),
            snapshot.listener.policy(),
        );
        // Listener-level now, not H3's own: the same question is asked of
        // HTTP/1.1 and HTTP/2, and asking it per transport is what left them
        // without an answer. The host's own narrowing is resolved per request
        // on `PreparedRequest`, so the handler no longer carries a number.
        let request_handler = H3RequestHandler::new(host_service.clone());
        let web_transport_enabled =
            deps.web_transport.has_host(&vctx.vhost_key.0);
        let quinn_conn = H3QuinnConnection::new(conn);
        let mut h3_builder = h3::server::builder();
        let ListenerSettings::Quic(settings) = &snapshot.listener.settings
        else {
            error!("QUIC listener has no QUIC settings");
            return;
        };
        h3_builder.max_field_section_size(settings.quic.max_field_section_size);
        let Some(input_capacity) = std::num::NonZeroUsize::new(
            settings.quic.max_concurrent_bidi_streams as usize,
        ) else {
            error!("QUIC listener has no bidi stream capacity");
            return;
        };
        if web_transport_enabled {
            h3_builder
                .enable_webtransport(true)
                .enable_extended_connect(true)
                .enable_datagram(true);
        }
        let mut h3_conn = match h3_builder.build::<_, Bytes>(quinn_conn).await {
            Ok(c) => H3Connection::new(c),
            Err(e) => {
                error!("❌ Failed to build H3 connection: {e:?}");
                return;
            }
        };

        // DO NOT move `resolver.resolve().await` back into the `accept()` arm.
        //
        // `accept()` hands over a stream as soon as the peer opens it; its
        // HEADERS frame may arrive much later, or never. Awaiting it inside
        // the accept arm stalls the whole loop on that one stream: no new
        // request on this connection is accepted and finished tasks are not
        // reaped until it resolves. One slow or hostile stream then holds
        // every other request on the connection (head-of-line blocking,
        // slowloris-style). So headers are awaited here, concurrently, in
        // `resolving`, next to `accept()`.
        //
        // Resolution classifies HTTP requests and WT data streams. This loop
        // retains the H3 driver throughout both kinds of work; a WT session
        // never takes it over. Authorization and CONNECT response I/O are also
        // independent futures, so neither can stop other HTTP acceptance.
        //
        // Dropping a pending resolver is safe: h3 releases its request entry
        // on drop (see `RequestResolver::resolve_request`). The QUIC
        // concurrent-stream limit bounds how many can pend at once.
        //
        // `accepting` turns off on graceful shutdown (GOAWAY) or peer close.
        // Streams accepted before that are below the GOAWAY id, so the peer
        // expects them served: their headers are still read and they are
        // still dispatched. The driver stays alive until their authorization,
        // request tasks and session tasks have all completed.
        let mut resolving = FuturesUnordered::new();
        let mut authorizing = FuturesUnordered::new();
        let mut sessions = SessionStreams::new(input_capacity);
        let mut accepting = true;
        let mut transport_open = true;
        // The routing captured above — host, app, ingress policy — holds for
        // this connection's life. If its host moves to another vhost (or is
        // gone, or its captured config replaced), it retires like shutdown:
        // GOAWAY, no new streams, the accepted ones still served. The request
        // path never consults the binding; it only wakes on a publication.
        let mut binding = deps.vhost_resolver.bind_config(
            Arc::clone(&vctx.hostname.0),
            Arc::clone(&vctx.vhost_key.0),
            vctx.vhost_config.clone(),
        );
        let retired = async move {
            binding.retired().await;
            info!(protocol = "HTTP/3", host = %binding.hostname(), canonical = %binding.canonical(), "🔀 host mapping or configuration changed: retiring this connection gracefully");
        }.boxed().shared();
        loop {
            if !accepting
                && resolving.is_empty()
                && authorizing.is_empty()
                && streams.is_empty()
            {
                break;
            }
            tokio::select! {
                biased;
                _ = permit.wait_graceful_shutdown(), if accepting => {
                    debug!("🔸 H3 graceful shutdown: stop accepting new streams");
                    let _ = h3_conn.shutdown(usize::MAX).await;
                    accepting = false;
                }
                () = retired.clone(), if accepting => {
                    let _ = h3_conn.shutdown(usize::MAX).await;
                    accepting = false;
                }
                result = streams.join_next(), if !streams.is_empty() => {
                    match result {
                        Some(Ok(StreamCompletion::Session(id))) => sessions.remove(id),
                        Some(Err(error)) => {
                            sessions.prune_closed();
                            error!(%error, "H3 stream task failed");
                        }
                        _ => {},
                    }
                }
                Some(authorized) = authorizing.next(), if !authorizing.is_empty() => {
                    let authorized: Option<crate::quic::wt::AuthorizedSession> = authorized;
                    if let Some(authorized) = authorized {
                        let id = authorized.id();
                        // Register before sending 200: the peer may immediately
                        // open a WT stream after receiving the response.
                        let incoming = sessions.register(id);
                        let serving = serve_web_transport(authorized, &mut h3_conn, incoming);
                        streams.spawn(async move {
                            serving.await;
                            StreamCompletion::Session(id)
                        });
                    }
                }
                Some(resolved) = resolving.next(), if !resolving.is_empty() => {
                    let (h3_head, mut stream): (
                        http::Request<()>,
                        crate::quic::request::H3RequestStream<H3QuinnConnection>
                    ) = match resolved {
                        Ok(AcceptedBi::Request(request, stream)) => (request, stream),
                        Ok(AcceptedBi::BidiStream(id, stream)) => {
                            if let Err(stream) = sessions.route(id, stream) {
                                // No receiver/capacity: refuse this stream, not
                                // the connection and not another session's work.
                                drop(stream);
                                debug!(?id, "WebTransport bidi stream has no available session input");
                            }
                            continue;
                        }
                        Err(err) => {
                            super::error::log_stream_error(&err, "resolve_request");
                            continue;
                        }
                    };

                    if is_web_transport_connect(&h3_head) {
                        if stream.is_0rtt() {
                            streams.spawn(async move {
                                send_status_response(&mut stream, StatusCode::TOO_EARLY).await;
                                StreamCompletion::Request
                            });
                            continue;
                        }

                        let prepared = match host_service.prepare(h3_head, RequestArrival::Established) {
                            Ok(prepared) => prepared,
                            Err(reject) => {
                                streams.spawn(async move {
                                    send_host_response(&mut stream, reject).await;
                                    StreamCompletion::Request
                                });
                                continue;
                            }
                        };

                        if !web_transport_enabled {
                            streams.spawn(async move {
                                send_status_response(&mut stream, StatusCode::NOT_FOUND).await;
                                StreamCompletion::Request
                            });
                            continue;
                        }

                        let Some(binding) = deps
                            .web_transport
                            .resolve(&vctx.vhost_key.0, prepared.request().uri().path())
                        else {
                            streams.spawn(async move {
                                send_status_response(&mut stream, StatusCode::NOT_FOUND).await;
                                StreamCompletion::Request
                            });
                            continue;
                        };

                        let mut auth = prepared.auth_context();
                        auth.admission = Some(permit.clone());
                        auth.graceful = Some(retired.clone());
                        let request = prepared.into_request();
                        authorizing.push(authorize_web_transport(
                            binding, state.clone(), auth, request, stream,
                        ));
                        continue;
                    }

                    let handler = request_handler.clone();
                    let mode = if requires_streamed_request_body(&h3_head) {
                        RequestBodyMode::Streaming
                    } else {
                        RequestBodyMode::Buffered
                    };
                    streams.spawn(async move {
                        handler.serve::<H3QuinnConnection>(h3_head, stream, mode).await;
                        StreamCompletion::Request
                    });
                }
                // Keep control processing alive while admitted sessions and
                // response streams drain. Grace stops new HTTP admission, not
                // the driver required by already accepted work.
                res = h3_conn.accept(), if transport_open => {
                    match res {
                        Ok(Some(IncomingStream::Bidirectional(resolver))) => {
                            if accepting {
                                resolving.push(resolver.resolve());
                            }
                            // After GOAWAY, do not retain newly opened streams
                            // or await their headers. Earlier resolvers survive.
                        }
                        Ok(Some(IncomingStream::Unidirectional(_, stream))) => {
                            // Current handlers consume a reliable bidi stream;
                            // uni streams have no registered receiver.
                            drop(stream);
                        }
                        Ok(None) => {
                            debug!("🔹 H3 peer closed from={}", remote.ip());
                            accepting = false;
                            transport_open = false;
                            sessions.close();
                        }
                        Err(err) => {
                            if super::error::normal_connection_close(&err) {
                                debug!("🔹 H3 connection closed from={}", remote.ip());
                            } else {
                                warn!(
                                    "⚠️  H3 connection closed from={} with error: {err:?}",
                                    remote.ip()
                                );
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn accept(endpoint: &Endpoint) -> (&Endpoint, Option<quinn::Incoming>) {
    (endpoint, endpoint.accept().await)
}

fn h3_tls_info(
    conn: &QuinnConnection,
    _handshake_data: Option<&HandshakeData>,
) -> TlsInfo {
    let client_cert = conn
        .peer_identity()
        .and_then(|identity| {
            identity.downcast::<Vec<CertificateDer<'static>>>().ok()
        })
        .and_then(|certs| certs.first().cloned())
        .and_then(|cert| {
            let parsed = take_first_cert_from_der(cert.as_ref()).ok()?;
            Some(TlsClientCertInfo {
                subject_dn: parsed.subject().to_string(),
                issuer_dn: parsed.issuer().to_string(),
                identity: peer_identity(cert.as_ref()).ok().map(Arc::new),
            })
        });
    let client_auth = if client_cert.is_some() {
        ClientAuthStatus::Verified
    } else {
        ClientAuthStatus::NotProvided
    };

    // QUIC always runs over TLS 1.3. Quinn exposes the verified peer identity,
    // but not the negotiated cipher suite through its public connection API.
    TlsInfo {
        protocol_version: Some(ProtocolVersion::TLSv1_3),
        cipher_suite: None,
        client_auth,
        client_cert,
    }
}

fn requires_streamed_request_body(head: &http::Request<()>) -> bool {
    match *head.method() {
        Method::POST | Method::PUT | Method::PATCH => {
            content_length(head).is_none_or(|len| len > 0)
        }
        _ => content_length(head).is_some_and(|len| len > 0),
    }
}

fn content_length(head: &http::Request<()>) -> Option<u64> {
    head.headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

#[cfg(feature = "geoip")]
fn country_code_for(remote: SocketAddr) -> Option<String> {
    Some(claviron_geoip::geoip_lookup(remote.ip()))
}

#[cfg(not(feature = "geoip"))]
fn country_code_for(_remote: SocketAddr) -> Option<String> {
    None
}

#[cfg(test)]
#[path = "../../tests/quic/listener.rs"]
mod tests;
