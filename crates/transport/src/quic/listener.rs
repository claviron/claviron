//! QUIC group bind/admission/drain and connection/stream ownership.
//! Each accepted connection retains its snapshot through its handshake, host
//! selection and every request. Request body adaptation remains in `request`.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use claviron_http::config::transport::EarlyDataPolicy;
use claviron_http::config::{ListenerConfig, QuicListenerConfig};
use claviron_http::ext::{ClientAuthStatus, ConnCtx, RequestArrival, TlsInfo};
#[cfg(feature = "metrics")]
use claviron_http::metrics::SERVER_METRICS;
use claviron_http::session::GracefulSignal;
use claviron_http::{RouteBody, RouteResponse};
use claviron_runtime::gate::{Gate, GateDrainOutcome, Permit};
use claviron_runtime::registry::{Error, Result};
use claviron_runtime::{RunContext, RunMode};
use claviron_sys::net::{PortSharing, create_udp_socket};
use claviron_tls::Proto::H3;
use claviron_types::ssl::peer::peer_identity;
use futures_util::FutureExt;
use futures_util::stream::{FuturesUnordered, StreamExt};
use h3_quinn::Connection as H3QuinnConnection;
use h3_webtransport::server::{AcceptedBi, IncomingStream};
use http::{Request, StatusCode};
use quinn::crypto::rustls::HandshakeData;
use quinn::{Connection as QuinnConnection, Endpoint, Incoming, VarInt};
use rustls::ProtocolVersion;
use rustls::pki_types::CertificateDer;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use super::drain;
use super::request::{check_early_data, send_status_response};
use super::wt::streams::SessionStreams;
use super::wt::{
    Authorized, AuthorizedSession, H3Connection, QuicSession, authorize,
    is_web_transport_connect, serve_session,
};

type SessionOf<S> =
    <<<S as Quic>::Host as QuicHost>::Requests as QuicRequests>::Session;

/// What one QUIC listener serves, published as a whole and retained by the
/// connections accepted under it.
pub trait Quic: Send + Sync + 'static {
    /// The owner of one accepted connection, selected by its server name.
    type Host: QuicHost;

    fn listener(&self) -> &ListenerConfig;
    fn settings(&self) -> &QuicListenerConfig;

    /// The configuration an endpoint binds with, before any connection exists.
    fn endpoint_config(&self) -> quinn::ServerConfig;

    /// The configuration this publication completes a handshake with.
    fn handshake_config(&self) -> Arc<quinn::ServerConfig>;

    /// Choose the connection's host once its handshake has completed. A
    /// refusal closes the connection with the code and reason it carries.
    fn select(
        self: &Arc<Self>,
        server_name: Option<&str>,
    ) -> std::result::Result<Self::Host, Refusal>;
}

/// The owner a QUIC connection was accepted for.
pub trait QuicHost: Send + 'static {
    type Requests: QuicRequests;

    /// Whether this host accepts 0-RTT requests, and which. The listener's own
    /// settings bound this: without early data enabled none are accepted.
    fn early_data(&self) -> EarlyDataPolicy;

    /// Whether WebTransport is offered on this connection.
    fn web_transport(&self) -> bool;

    /// Completes when the connection should end gracefully because its host
    /// was replaced or removed; never, for a host that cannot change.
    fn retired(&self) -> impl Future<Output = ()> + Send + 'static;

    /// Take the connection's completed facts and return its request owner.
    /// Called once, before the first request.
    fn bind(
        self,
        facts: ConnCtx,
    ) -> std::result::Result<Self::Requests, Refusal>;
}

/// The owner of one H3 connection's requests. It lives on the connection's
/// driver and is called from it; what it returns runs apart from the driver.
pub trait QuicRequests: Send + 'static {
    type Session: QuicSession;

    /// Admit one request before any of its body is read.
    fn admit(
        &mut self,
        head: &Request<()>,
    ) -> std::result::Result<(), StatusCode>;

    /// Answer one request.
    fn call(
        &self,
        request: Request<RouteBody>,
        arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + 'static;

    /// Decide who takes a WebTransport CONNECT. The returned future authorizes
    /// it alongside the connection's other work and never stops it.
    fn connect(
        &self,
        head: Request<()>,
        scope: SessionScope,
    ) -> std::result::Result<
        impl Future<
            Output = std::result::Result<Authorized<Self::Session>, StatusCode>,
        > + Send
        + 'static,
        StatusCode,
    >;
}

/// What a long-lived session keeps of its connection.
pub struct SessionScope {
    /// Connection admission: observes listener drain.
    pub permit: Arc<Permit>,
    /// Completes when the connection's host retires.
    pub retired: GracefulSignal,
}

/// Why a connection is closed without being served.
#[derive(Debug, Clone, Copy)]
pub struct Refusal {
    pub code: VarInt,
    pub reason: &'static [u8],
}

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

/// Bind `initial`'s addresses and serve until `context` is cancelled and every
/// accepted connection has drained.
pub async fn serve<S: Quic>(
    snapshots: &ArcSwapOption<S>,
    initial: Arc<S>,
    context: &RunContext,
    run_mode: RunMode,
) -> Result<()> {
    let listener = initial.listener();
    let mut endpoints = Vec::new();
    // Complete all addresses before accepting traffic on this listener.
    for addr in &listener.listen {
        let socket = bind_socket(*addr, run_mode).map_err(|error| {
            Error::msg(format!(
                "H3 listener '{}': bind {addr}: {error}",
                listener.id
            ))
        })?;
        let endpoint = endpoint(initial.as_ref(), socket).map_err(|error| {
            Error::msg(format!(
                "H3 listener '{}': endpoint {addr}: {error}",
                listener.id
            ))
        })?;
        if matches!(run_mode, RunMode::Worker(_)) {
            debug!(listener = %listener.id, address = %endpoint.local_addr()?, "🌐 QUIC listener bound");
        } else {
            info!(listener = %listener.id, address = %endpoint.local_addr()?, "🌐 QUIC listener bound");
        }
        endpoints.push(endpoint);
    }
    serve_endpoints(snapshots, initial, endpoints, context).await;
    Ok(())
}

/// An endpoint for a socket the caller bound.
pub fn endpoint<S: Quic>(
    snapshot: &S,
    socket: std::net::UdpSocket,
) -> std::io::Result<Endpoint> {
    Endpoint::new(
        Default::default(),
        Some(snapshot.endpoint_config()),
        socket,
        Arc::new(quinn::TokioRuntime),
    )
}

/// Serve already bound endpoints until `context` is cancelled and every
/// accepted connection has drained.
pub async fn serve_endpoints<S: Quic>(
    snapshots: &ArcSwapOption<S>,
    initial: Arc<S>,
    endpoints: Vec<Endpoint>,
    context: &RunContext,
) {
    let listener = initial.listener();
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
                let Some(snapshot) = snapshots.load_full() else { incoming.refuse(); continue; };
                let local_addr = match endpoint.local_addr() {
                    Ok(addr) => addr,
                    Err(error) => { error!(%error, "QUIC local address unavailable"); incoming.refuse(); continue; }
                };
                #[cfg(feature = "metrics")]
                SERVER_METRICS.udp_accept();
                connections.spawn(serve_incoming(incoming, permit, local_addr, snapshot));
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
    if let GateDrainOutcome::Forced { remaining } = gate.wait_all_done().await {
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

/// Complete one handshake under the publication it was admitted by, then own
/// the connection. The snapshot moves through it; routing cannot load a newer
/// view.
async fn serve_incoming<S: Quic>(
    incoming: Incoming,
    permit: Permit,
    local_addr: SocketAddr,
    snapshot: Arc<S>,
) {
    let handshake_timeout = snapshot.settings().handshake_timeout;
    let handshake = async {
        incoming
            .accept_with(snapshot.handshake_config())
            .map_err(|error| Error::msg(format!("H3 accept: {error}")))?
            .await
            .map_err(|error| Error::msg(format!("H3 handshake: {error}")))
    };
    let result = tokio::select! {
        biased;
        _ = permit.wait_forced_shutdown() => return,
        result = tokio::time::timeout(handshake_timeout, handshake) => result
    };
    let connection = match result {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => {
            #[cfg(feature = "metrics")]
            SERVER_METRICS.tls_failed();
            debug!(listener = %snapshot.listener().id, %error, "H3 handshake rejected");
            return;
        }
        Err(_) => {
            #[cfg(feature = "metrics")]
            SERVER_METRICS.tls_failed();
            debug!(listener = %snapshot.listener().id, ?handshake_timeout, "H3 handshake timeout");
            return;
        }
    };
    let conn_for_close = connection.clone();
    let permit = Arc::new(permit);
    let mut streams = JoinSet::new();
    #[cfg(feature = "metrics")]
    let _metrics_guard = SERVER_METRICS.conn_open();
    // The driver only borrows its children. Force or unwind drops the
    // driver first, then the outer owner aborts/joins every stream/session.
    let exit = drain::drive(
        accept_streams(connection, &permit, local_addr, snapshot, &mut streams),
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

/// Drive a single QUIC connection: server name → host → request owner.
async fn accept_streams<S: Quic>(
    conn: QuinnConnection,
    permit: &Arc<Permit>,
    local_addr: SocketAddr,
    snapshot: Arc<S>,
    streams: &mut JoinSet<StreamCompletion>,
) {
    let remote: SocketAddr = conn.remote_address();
    let local_ip = conn.local_ip().unwrap_or(local_addr.ip());

    debug!("🆕 QUIC connection accepted from={}", remote.ip());

    // Pull SNI out of the QUIC/TLS handshake.
    let handshake_data =
        conn.handshake_data().and_then(|d| d.downcast::<HandshakeData>().ok());
    let server_name =
        handshake_data.as_ref().and_then(|data| data.server_name.as_deref());

    let host = match snapshot.select(server_name) {
        Ok(host) => host,
        Err(refusal) => return conn.close(refusal.code, refusal.reason),
    };

    let facts = ConnCtx {
        state: Default::default(),
        listener: snapshot.listener().id.clone(),
        proto: Some(H3),
        local_ip,
        remote,
        local_port: local_addr.port(),
        real_ip: None,
        country_code: crate::geo::country_code(remote.ip()),
        tls: Some(h3_tls_info(&conn)),
    };
    let settings = snapshot.settings();
    let early_data = if settings.quic.enable_early_data {
        host.early_data()
    } else {
        EarlyDataPolicy::DenyAll
    };
    let web_transport_enabled = host.web_transport();
    // The routing captured here — host, owner, ingress policy — holds for this
    // connection's life. If its host moves to another vhost (or is gone, or
    // its captured config replaced), it retires like shutdown: GOAWAY, no new
    // streams, the accepted ones still served. The request path never consults
    // this; it only wakes on a publication.
    let retired: GracefulSignal = host.retired().boxed().shared();
    let mut requests = match host.bind(facts) {
        Ok(requests) => requests,
        Err(refusal) => return conn.close(refusal.code, refusal.reason),
    };

    let quinn_conn = H3QuinnConnection::new(conn);
    let mut h3_builder = h3::server::builder();
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
                let authorized: Option<AuthorizedSession<SessionOf<S>>> = authorized;
                if let Some(authorized) = authorized {
                    let id = authorized.id();
                    // Register before sending 200: the peer may immediately
                    // open a WT stream after receiving the response.
                    let incoming = sessions.register(id);
                    let serving = serve_session(authorized, &mut h3_conn, incoming);
                    streams.spawn(async move {
                        serving.await;
                        StreamCompletion::Session(id)
                    });
                }
            }
            Some(resolved) = resolving.next(), if !resolving.is_empty() => {
                let (h3_head, mut stream): (
                    http::Request<()>,
                    super::request::H3RequestStream<H3QuinnConnection>
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

                if let Err(status) = requests.admit(&h3_head) {
                    streams.spawn(async move {
                        send_status_response(&mut stream, status).await;
                        stream.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
                        StreamCompletion::Request
                    });
                    continue;
                }

                let arrival = if stream.is_0rtt() {
                    if let Err(status) = check_early_data(early_data, h3_head.method()) {
                        streams.spawn(async move {
                            send_status_response(&mut stream, status).await;
                            stream.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
                            StreamCompletion::Request
                        });
                        continue;
                    }
                    RequestArrival::EarlyData
                } else {
                    RequestArrival::Established
                };

                if is_web_transport_connect(&h3_head) {
                    if arrival == RequestArrival::EarlyData {
                        streams.spawn(async move {
                            send_status_response(&mut stream, StatusCode::TOO_EARLY).await;
                            StreamCompletion::Request
                        });
                        continue;
                    }

                    if !web_transport_enabled {
                        streams.spawn(async move {
                            send_status_response(&mut stream, StatusCode::NOT_FOUND).await;
                            StreamCompletion::Request
                        });
                        continue;
                    }

                    let scope = SessionScope { permit: permit.clone(), retired: retired.clone() };
                    match requests.connect(h3_head, scope) {
                        Ok(deciding) => authorizing.push(authorize(deciding, stream)),
                        Err(status) => {
                            streams.spawn(async move {
                                send_status_response(&mut stream, status).await;
                                StreamCompletion::Request
                            });
                        }
                    }
                    continue;
                }

                let serving = super::request::serve::<H3QuinnConnection, _, _>(
                    h3_head,
                    stream,
                    |request| requests.call(request, arrival),
                );
                streams.spawn(async move {
                    serving.await;
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

async fn accept(endpoint: &Endpoint) -> (&Endpoint, Option<quinn::Incoming>) {
    (endpoint, endpoint.accept().await)
}

fn h3_tls_info(conn: &QuinnConnection) -> TlsInfo {
    // A presented certificate was verified by the handshake. Its names are
    // parsed once; a certificate whose names do not parse stays verified
    // but carries no identity facts.
    let leaf = conn
        .peer_identity()
        .and_then(|identity| {
            identity.downcast::<Vec<CertificateDer<'static>>>().ok()
        })
        .and_then(|certs| certs.first().cloned());
    let client_auth = if leaf.is_some() {
        ClientAuthStatus::Verified
    } else {
        ClientAuthStatus::NotProvided
    };
    let client_cert =
        leaf.and_then(|cert| peer_identity(cert.as_ref()).ok()).map(Arc::new);

    // QUIC always runs over TLS 1.3. Quinn exposes the verified peer identity,
    // but not the negotiated cipher suite through its public connection API.
    TlsInfo {
        protocol_version: Some(ProtocolVersion::TLSv1_3),
        cipher_suite: None,
        client_auth,
        client_cert,
    }
}
