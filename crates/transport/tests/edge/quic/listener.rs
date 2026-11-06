use std::collections::HashMap;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Buf;
use claviron_edge::pipeline::HostAppRegistry;
use claviron_edge::transport::{
    OtherHandler, OtherService, VhostResolver, VhostTable,
};
use claviron_http::config::HttpSnapshot;
use claviron_http::{RouteBody, RouteFuture, bytes_body};
use claviron_runtime::AppState;
use claviron_runtime::RunContext;
use claviron_tls::CertResolver;
use claviron_tls::orbit::FleetServerSessions;
use http::{Method, Request, Response};
use http_body_util::BodyExt;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::sign::CertifiedKey;

use super::*;

const HTTP: &str = "listeners:\n  h3:\n    transport: quic\n    protocols: h3\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n";
const HOST: &str = "example.test";
const UNHOSTED: &str = "unhosted.test";

/// Answers with the request's method and body, so a response proves the head
/// and the lazily adapted body both arrived.
struct Echo;

impl OtherHandler for Echo {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(async move {
            let (head, body) = request.into_parts();
            let body = body.collect().await.expect("request body").to_bytes();
            let mut answer = head.method.as_str().as_bytes().to_vec();
            answer.push(b' ');
            answer.extend_from_slice(&body);
            Ok(Response::new(bytes_body(answer.into())))
        })
    }
}

/// A certificate for both names, so a refused name is the server's decision
/// and not the client's failure to verify.
fn certificate() -> (CertificateDer<'static>, Arc<CertifiedKey>) {
    let issued = rcgen::generate_simple_self_signed(vec![
        HOST.to_owned(),
        UNHOSTED.to_owned(),
    ])
    .unwrap();
    let certificate = CertificateDer::from(issued.cert.der().to_vec());
    let key =
        PrivateKeyDer::try_from(issued.signing_key.serialize_der()).unwrap();
    let signing = claviron_tls::policy::provider()
        .key_provider
        .load_private_key(key)
        .unwrap();
    let certified = CertifiedKey::new(vec![certificate.clone()], signing);
    (certificate, Arc::new(certified))
}

fn client_endpoint(trust: CertificateDer<'static>) -> Endpoint {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(trust).unwrap();
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut endpoint =
        Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint
        .set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
    endpoint
}

type Client = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// Send one request and read its whole answer.
async fn exchange(
    client: &mut Client,
    request: Request<()>,
    body: Option<&'static [u8]>,
) -> (StatusCode, Vec<u8>) {
    let mut stream = client.send_request(request).await.unwrap();
    if let Some(body) = body {
        stream.send_data(Bytes::from_static(body)).await.unwrap();
    }
    stream.finish().await.unwrap();
    let status = stream.recv_response().await.unwrap().status();
    let mut answer = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await.unwrap() {
        answer
            .extend_from_slice(chunk.copy_to_bytes(chunk.remaining()).as_ref());
    }
    (status, answer)
}

fn request(
    method: Method,
    authority: &str,
) -> http::request::Builder {
    Request::builder().method(method).uri(format!("https://{authority}/"))
}

/// The native listener over a real QUIC connection: SNI selects the host, a
/// request reaches its owner with its body, the shared admission refuses
/// another authority and H3's own framing rule holds, an unhosted name is
/// refused in the handshake, and cancellation drains to a stopped listener.
#[tokio::test]
async fn the_h3_listener_serves_a_host_and_drains_over_a_real_connection() {
    let (trust, certified) = certificate();

    let state = AppState::standalone_in_fleet(
        "h3-listener",
        format!("h3t{}", std::process::id()).as_str(),
        ".",
    );
    let apps = Arc::new(HostAppRegistry::default());
    state.registry_ref().insert(apps.clone()).unwrap();
    apps.register_other(HOST, |_, _, _| Ok(OtherService::new(Echo)));

    let table: VhostTable =
        serde_yaml::from_str(&format!("{HOST}: {{root: .}}\n")).unwrap();
    let vhosts = Arc::new(VhostResolver::default());
    for (canonical, record) in table.canonical_entries() {
        vhosts.upsert_host(canonical.clone(), record.config.clone());
    }

    let http: HttpSnapshot = serde_yaml::from_str(HTTP).unwrap();
    let id: claviron_http::ListenerId = "h3".parse().unwrap();
    let input =
        claviron_edge::transport::publication::select_listener(&http, &id)
            .unwrap();
    let hosts = Arc::new(
        claviron_edge::transport::publication::prepare_hosts(
            &state,
            &vhosts.snapshot(),
            &input,
            None,
        )
        .unwrap(),
    );

    let fleet = orbitive::Fleet::join(&format!("h3s{}", std::process::id()), 1)
        .unwrap();
    let sessions =
        Arc::new(FleetServerSessions::open(Arc::new(fleet)).unwrap());
    let deps = Arc::new(crate::edge::quic::server::Deps {
        cert_resolver: Arc::new(CertResolver::new(
            HashMap::from([(HOST.to_owned(), certified.clone())]),
            certified,
        )),
        vhost_resolver: vhosts,
        web_transport: Arc::default(),
        tls_sessions: sessions.clone(),
        revocations: Arc::default(),
    });
    let snapshot =
        crate::edge::quic::server::build_snapshots(&input, &deps, hosts, None)
            .unwrap()
            .remove(&id)
            .unwrap();

    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let endpoint = Endpoint::new(
        Default::default(),
        Some(snapshot.tls_accept.base_config()),
        socket,
        Arc::new(quinn::TokioRuntime),
    )
    .unwrap();
    let listener = Arc::new(H3Listener {
        id: id.clone(),
        name: "Listener:h3".into(),
        state,
        current: Arc::new(ArcSwapOption::from(Some(snapshot.clone()))),
        input: ArcSwap::from_pointee(input),
    });
    let cancellation = tokio_util::sync::CancellationToken::new();
    let context = RunContext::new(cancellation.clone());
    let serving = tokio::spawn(async move {
        listener.run_group(snapshot, vec![endpoint], &context, deps).await;
    });

    let client = client_endpoint(trust);

    // Not hosted: the handshake is refused, by the server.
    let refused = client.connect(address, UNHOSTED).unwrap().await;
    assert!(refused.is_err(), "an unhosted name must not complete");

    let connection = client.connect(address, HOST).unwrap().await.unwrap();
    let (mut driver, mut requests) =
        h3::client::new(h3_quinn::Connection::new(connection)).await.unwrap();
    let closed = tokio::spawn(async move {
        std::future::poll_fn(|cx| driver.poll_close(cx)).await
    });

    let (status, answer) = exchange(
        &mut requests,
        request(Method::GET, HOST).body(()).unwrap(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer, b"GET ");

    let (status, answer) = exchange(
        &mut requests,
        request(Method::POST, HOST).body(()).unwrap(),
        Some(b"payload"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer, b"POST payload");

    // This connection's SNI host does not answer for another authority.
    let (status, _) = exchange(
        &mut requests,
        request(Method::GET, "other.example.test").body(()).unwrap(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);

    // No parser stands in front of this ingress; the driver holds the rule.
    let (status, _) = exchange(
        &mut requests,
        request(Method::GET, HOST)
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .unwrap(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    cancellation.cancel();
    drop(requests);
    serving.await.unwrap();
    closed.await.unwrap();
    sessions.unlink().unwrap();
}
