use super::*;

use claviron_http::config::HttpSnapshot;
use claviron_http::session::SessionOwner;
use claviron_http::{OtherHandler, RouteBody, RouteFuture, bytes_body};
use claviron_runtime::gate::Gate;
use hyper::Request;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, ServerName,
};

struct Echo;

impl OtherHandler for Echo {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        // Nothing was attached on the way here.
        assert!(request.extensions().is_empty());
        let authority = request
            .headers()
            .get(http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        Box::pin(async move {
            Ok(http::Response::new(bytes_body(bytes::Bytes::from(authority))))
        })
    }
}

/// The TLS listener serves without any host table: one configuration
/// completes the handshake for whatever server name is offered, and the one
/// handler answers a request naming yet another authority.
#[tokio::test]
async fn a_single_handler_serves_tls_without_a_host_table() {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let issued =
        rcgen::generate_simple_self_signed(vec!["example.test".to_owned()])
            .unwrap();
    let certificate = CertificateDer::from(issued.cert.der().to_vec());
    let key =
        PrivateKeyDer::try_from(issued.signing_key.serialize_der()).unwrap();
    let mut server =
        rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key)
            .unwrap();
    server.alpn_protocols = vec![b"http/1.1".to_vec()];

    let config: HttpSnapshot = serde_yaml::from_str(
        "listeners:\n  example:\n    transport: tcp\n    tls: true\n    protocols: [h1, h2]\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n",
    )
    .unwrap();
    let listener = config.listeners.values().next().unwrap().clone();
    let gate = Gate::new(
        listener.limits.max_connections.map(|value| value.get()),
        listener.limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(listener.id.clone());
    let snapshot = Arc::new(
        Single::new(listener, Arc::new(server), crate::owner::Handler(Echo))
            .unwrap(),
    );

    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let serving = tokio::spawn(async move {
        let (stream, peer) = socket.accept().await.unwrap();
        let scope = sessions.bind(gate.try_enter().unwrap());
        serve(stream, peer, snapshot, scope).await;
    });

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let mut client = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"http/1.1".to_vec()];
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut stream = TlsConnector::from(Arc::new(client))
        .connect(ServerName::try_from("example.test").unwrap(), stream)
        .await
        .unwrap();
    stream
        .write_all(
            b"GET / HTTP/1.1\r\nHost: other.example.test\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut answer = Vec::new();
    // The peer may close without a TLS close notification after its response.
    let _ = stream.read_to_end(&mut answer).await;
    serving.await.unwrap();

    let answer = String::from_utf8_lossy(&answer);
    assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
    assert!(answer.ends_with("other.example.test"), "{answer}");
}
