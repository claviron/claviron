use std::collections::HashMap;

use arc_swap::ArcSwapOption;
use claviron_edge::pipeline::HostAppRegistry;
use claviron_edge::transport::{OtherHandler, OtherService, VhostTable};
use claviron_http::config::HttpSnapshot;
use claviron_http::{RouteFuture, bytes_body};
use claviron_runtime::RunContext;
use claviron_tls::CertResolver;
use claviron_tls::orbit::FleetServerSessions;
use http::{Method, Response};
use http_body_util::BodyExt;
use rustls::sign::CertifiedKey;

use super::*;
use crate::quic::listener::{endpoint, serve_endpoints};
use crate::quic::test_client::{self, exchange, request};

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

/// The native listener over a real QUIC connection: SNI selects the host, a
/// request reaches its owner with its body, the shared admission refuses
/// another authority and H3's own framing rule holds, an unhosted name is
/// refused in the handshake, and cancellation drains to a stopped listener.
#[tokio::test]
async fn the_h3_listener_serves_a_host_and_drains_over_a_real_connection() {
    // Both names, so a refused name is the server's decision and not the
    // client's failure to verify.
    let (trust, key) = test_client::issued(&[HOST, UNHOSTED]);
    let signing = claviron_tls::policy::provider()
        .key_provider
        .load_private_key(key)
        .unwrap();
    let certified = Arc::new(CertifiedKey::new(vec![trust.clone()], signing));

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
    let snapshot = crate::edge::quic::server::build_snapshots(
        &state, &input, &deps, hosts, None,
    )
    .unwrap()
    .remove(&id)
    .unwrap();

    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let endpoint = endpoint(snapshot.as_ref(), socket).unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let context = RunContext::new(cancellation.clone());
    let serving = tokio::spawn(async move {
        let snapshots = ArcSwapOption::from(Some(snapshot.clone()));
        serve_endpoints(&snapshots, snapshot, vec![endpoint], &context).await;
    });

    let client = test_client::endpoint(trust);

    // Not hosted: the handshake is refused, by the server.
    let refused = client.connect(address, UNHOSTED).unwrap().await;
    assert!(refused.is_err(), "an unhosted name must not complete");

    let (mut requests, closed) =
        test_client::connect(&client, address, HOST).await;

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
