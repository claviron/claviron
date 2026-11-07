use std::collections::HashMap;
use std::sync::Arc;

use claviron_edge::host::HostAppRegistry;
use claviron_edge::transport::{
    OtherHandler, OtherService, VhostResolver, VhostTable,
};
use claviron_http::config::HttpSnapshot;
use claviron_http::session::{SessionOwner, SessionSpawner};
use claviron_http::{RouteBody, RouteFuture, bytes_body};
use claviron_runtime::AppState;
use claviron_runtime::gate::Gate;
use http::{Request, Response};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::sign::CertifiedKey;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;

const HOST: &str = "example.test";

/// Reports whether the request can hand a session to the connection's owner.
struct Reports;

impl OtherHandler for Reports {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let owned = request.extensions().get::<SessionSpawner>().is_some();
        let answer = if owned { "owned" } else { "ordinary" };
        Box::pin(std::future::ready(Ok(Response::new(bytes_body(
            answer.into(),
        )))))
    }
}

fn prepared(fleet: &str) -> (AppState, Arc<VhostResolver>) {
    let state = AppState::standalone_in_fleet("edge-sessions", fleet, ".");
    let apps = Arc::new(HostAppRegistry::default());
    state.registry_ref().insert(apps.clone()).unwrap();
    apps.register_other(HOST, |_, _, _| Ok(OtherService::new(Reports)));
    let table: VhostTable = serde_yaml::from_str(&format!(
        "{HOST}: {{root: ., transport: {{non_ssl: serve}}}}\n"
    ))
    .unwrap();
    let vhosts = Arc::new(VhostResolver::default());
    for (canonical, record) in table.canonical_entries() {
        vhosts.upsert_host(canonical.clone(), record.config.clone());
    }
    (state, vhosts)
}

/// One request per connection, answered through `serve`.
async fn ask<S>(
    mut stream: S,
    upgrade: bool,
) -> String
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let upgrade = if upgrade {
        "Connection: Upgrade, close\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    } else {
        "Connection: close\r\n"
    };
    stream
        .write_all(
            format!("GET / HTTP/1.1\r\nHost: {HOST}\r\n{upgrade}\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut answer = Vec::new();
    // The peer may close without a TLS close notification after its response.
    let _ = stream.read_to_end(&mut answer).await;
    String::from_utf8_lossy(&answer).into_owned()
}

/// A request that can take over its connection reaches its owner with the
/// session owner; an ordinary one pays nothing for that.
#[tokio::test]
async fn a_cleartext_upgrade_request_carries_its_session_owner() {
    use crate::edge::tcp::plain::{HttpServer, HttpServerConfig};

    let (state, vhosts) = prepared("esp");
    let http: HttpSnapshot = serde_yaml::from_str(
        "listeners:\n  plain:\n    transport: tcp\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n",
    )
    .unwrap();
    let id = "plain".parse().unwrap();
    let listener = http.listeners.get(&id).unwrap().clone();
    let config = HttpServerConfig::try_from(http).unwrap();
    let handler = HttpServer::empty(state)
        .connection_handler(&config, &id, vhosts)
        .unwrap();
    let gate = Gate::new(
        listener.limits.max_connections.map(|value| value.get()),
        listener.limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(listener.id.clone());

    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    for (upgrade, expected) in [(false, "ordinary"), (true, "owned")] {
        let (client, accepted) =
            tokio::join!(TcpStream::connect(address), socket.accept());
        let (stream, peer) = accepted.unwrap();
        let scope = sessions.bind(gate.try_enter().unwrap());
        let serving = tokio::spawn(handler.clone().serve(stream, peer, scope));
        let answer = ask(client.unwrap(), upgrade).await;
        serving.await.unwrap();
        assert!(answer.ends_with(expected), "{answer}");
    }
}

/// The same over TLS, where the connection's host was selected by SNI.
#[tokio::test]
async fn a_tls_upgrade_request_carries_its_session_owner() {
    use crate::edge::tcp::secure::{HttpsServer, HttpsServerConfig};
    use claviron_tls::orbit::FleetServerSessions;

    let (state, vhosts) = prepared("est");
    let http: HttpSnapshot = serde_yaml::from_str(
        "listeners:\n  secure:\n    transport: tcp\n    tls: true\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n",
    )
    .unwrap();
    let id = "secure".parse().unwrap();
    let listener = http.listeners.get(&id).unwrap().clone();
    let config = HttpsServerConfig::try_from(http).unwrap();

    let issued =
        rcgen::generate_simple_self_signed(vec![HOST.to_owned()]).unwrap();
    let trust = CertificateDer::from(issued.cert.der().to_vec());
    let key =
        PrivateKeyDer::try_from(issued.signing_key.serialize_der()).unwrap();
    let signing = claviron_tls::policy::provider()
        .key_provider
        .load_private_key(key)
        .unwrap();
    let certified = Arc::new(CertifiedKey::new(vec![trust.clone()], signing));
    let fleet = orbitive::Fleet::join(&format!("ess{}", std::process::id()), 1)
        .unwrap();
    let tls_sessions =
        Arc::new(FleetServerSessions::open(Arc::new(fleet)).unwrap());
    let handler = HttpsServer::empty(state)
        .connection_handler(
            &config,
            &id,
            Arc::new(claviron_tls::CertResolver::new(
                HashMap::from([(HOST.to_owned(), certified.clone())]),
                certified,
            )),
            vhosts,
            tls_sessions.clone(),
            Arc::default(),
        )
        .unwrap();
    let gate = Gate::new(
        listener.limits.max_connections.map(|value| value.get()),
        listener.limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(listener.id.clone());

    let mut roots = rustls::RootCertStore::empty();
    roots.add(trust).unwrap();
    let mut client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    client.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = TlsConnector::from(Arc::new(client));

    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    for (upgrade, expected) in [(false, "ordinary"), (true, "owned")] {
        let (client, accepted) =
            tokio::join!(TcpStream::connect(address), socket.accept());
        let (stream, peer) = accepted.unwrap();
        let scope = sessions.bind(gate.try_enter().unwrap());
        let serving = tokio::spawn(handler.clone().serve(stream, peer, scope));
        let stream = connector
            .connect(ServerName::try_from(HOST).unwrap(), client.unwrap())
            .await
            .unwrap();
        let answer = ask(stream, upgrade).await;
        serving.await.unwrap();
        assert!(answer.ends_with(expected), "{answer}");
    }
    tls_sessions.unlink().unwrap();
}
