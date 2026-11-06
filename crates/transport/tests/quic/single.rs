use arc_swap::ArcSwapOption;
use claviron_http::config::HttpSnapshot;
use claviron_http::{RouteFuture, bytes_body};
use claviron_runtime::RunContext;
use http::{Method, Response};
use http_body_util::BodyExt;

use super::*;
use crate::quic::listener::{endpoint, serve_endpoints};
use crate::quic::test_client::{self, exchange, request};

const HTTP: &str = "listeners:\n  h3:\n    transport: quic\n    protocols: h3\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n";

/// Answers with the request's method, authority and body: nothing else
/// reached it, and nothing was attached on the way.
struct Echo;

impl OtherHandler for Echo {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(async move {
            let (head, body) = request.into_parts();
            let body = body.collect().await.expect("request body").to_bytes();
            let mut answer =
                format!("{} {} ", head.method, head.uri.authority().unwrap())
                    .into_bytes();
            answer.extend_from_slice(&body);
            Ok(Response::new(bytes_body(answer.into())))
        })
    }
}

fn tls(
    certificate: rustls::pki_types::CertificateDer<'static>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
) -> Arc<rustls::ServerConfig> {
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate], key)
    .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    Arc::new(tls)
}

/// The listener serves HTTP/3 without any host table: one configuration
/// completes the handshake and the one handler answers every authority, over
/// a real QUIC connection, and cancellation drains to a stopped listener.
#[tokio::test]
async fn a_single_handler_serves_h3_without_a_host_table() {
    let (certificate, key) = test_client::issued(&["example.test"]);
    let config: HttpSnapshot = serde_yaml::from_str(HTTP).unwrap();
    let listener = config.listeners.values().next().unwrap().clone();
    let snapshot = Arc::new(
        Single::new(listener, tls(certificate.clone(), key), Echo).unwrap(),
    );

    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let endpoint = endpoint(snapshot.as_ref(), socket).unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let context = RunContext::new(cancellation.clone());
    let serving = tokio::spawn(async move {
        let snapshots = ArcSwapOption::from(Some(snapshot.clone()));
        serve_endpoints(&snapshots, snapshot, vec![endpoint], &context).await;
    });

    let client = test_client::endpoint(certificate);
    let (mut requests, closed) =
        test_client::connect(&client, address, "example.test").await;

    let (status, answer) = exchange(
        &mut requests,
        request(Method::POST, "first.example.test").body(()).unwrap(),
        Some(b"payload"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer, b"POST first.example.test payload");

    let (status, answer) = exchange(
        &mut requests,
        request(Method::GET, "second.example.test").body(()).unwrap(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer, b"GET second.example.test ");

    cancellation.cancel();
    drop(requests);
    serving.await.unwrap();
    closed.await.unwrap();
}

/// A configuration that cannot speak H3 is refused when the listener is
/// prepared, not at every connection's handshake.
#[test]
fn a_configuration_without_h3_is_refused_when_prepared() {
    let (certificate, key) = test_client::issued(&["example.test"]);
    let mut without = tls(certificate, key).as_ref().clone();
    without.alpn_protocols = vec![b"http/1.1".to_vec()];
    let config: HttpSnapshot = serde_yaml::from_str(HTTP).unwrap();
    let listener = config.listeners.values().next().unwrap().clone();
    let refused = Single::new(listener, Arc::new(without), Echo);
    assert!(refused.is_err());
}
