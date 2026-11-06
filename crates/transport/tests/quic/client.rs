//! An H3 client and a certificate for exercising a listener over a real
//! connection.

use std::sync::Arc;

use bytes::{Buf, Bytes};
use http::{Method, Request, StatusCode};
use quinn::Endpoint;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// A self-signed certificate for `names`, with its private key.
pub(crate) fn issued(
    names: &[&str]
) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let issued = rcgen::generate_simple_self_signed(
        names.iter().map(|name| (*name).to_owned()).collect::<Vec<_>>(),
    )
    .unwrap();
    (
        CertificateDer::from(issued.cert.der().to_vec()),
        PrivateKeyDer::try_from(issued.signing_key.serialize_der()).unwrap(),
    )
}

pub(crate) fn endpoint(trust: CertificateDer<'static>) -> Endpoint {
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

pub(crate) type Client = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// Open an H3 connection; the returned task ends when the connection does.
pub(crate) async fn connect(
    endpoint: &Endpoint,
    address: std::net::SocketAddr,
    name: &str,
) -> (Client, tokio::task::JoinHandle<()>) {
    let connection = endpoint.connect(address, name).unwrap().await.unwrap();
    let (mut driver, client) =
        h3::client::new(h3_quinn::Connection::new(connection)).await.unwrap();
    let closed = tokio::spawn(async move {
        std::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });
    (client, closed)
}

pub(crate) fn request(
    method: Method,
    authority: &str,
) -> http::request::Builder {
    Request::builder().method(method).uri(format!("https://{authority}/"))
}

/// Send one request and read its whole answer.
pub(crate) async fn exchange(
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
