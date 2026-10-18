use std::sync::atomic::Ordering;

use bytes::Bytes;
use http::{Request, StatusCode, Version};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;

use super::support::{TEST_TIMEOUT, TlsFixture};

#[tokio::test]
async fn failed_tls_churn_releases_capacity_without_entering_application() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        for index in 0..16 {
            let name = format!("unhosted-{index}.test");
            assert!(fixture.connect(&name, b"http/1.1").await.is_err());
            fixture.reap_connection().await;
            let mut peer = fixture.tcp().await;
            peer.write_all(b"not a TLS ClientHello\r\n").await.unwrap();
            peer.shutdown().await.unwrap();
            drop(peer);
            fixture.reap_connection().await;
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        let mut healthy = fixture.connect("app.test", b"http/1.1").await.unwrap();
        healthy
            .write_all(b"GET / HTTP/1.1\r\nHost: app.test\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        healthy.take(4096).read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200 "));
        assert!(response.ends_with(b"healthy"));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        fixture.finish().await;
    })
    .await
    .expect("TLS churn fixture must terminate");
}

#[tokio::test]
async fn h1_wrong_authority_with_unread_body_replies_and_closes() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        let mut peer = fixture.connect("app.test", b"http/1.1").await.unwrap();
        // Never send the declared body or EOF. Early rejection must not wait
        // for arbitrary attacker data, or reuse those bytes as another request.
        peer.write_all(b"POST / HTTP/1.1\r\nHost: wrong.test\r\nContent-Length: 1024\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        peer.take(4096).read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 421 "));
        assert!(body.is_empty());
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        fixture.finish().await;
    })
    .await
    .expect("unread-body rejection must not hold the connection");
}

#[tokio::test]
async fn h2_wrong_authority_rejects_only_its_stream() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        let peer = fixture.connect("app.test", b"h2").await.unwrap();
        assert_eq!(peer.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
        let (mut sender, connection) =
            hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
                TokioExecutor::new(),
                TokioIo::new(peer),
            )
            .await
            .unwrap();
        let mut drivers = JoinSet::new();
        drivers.spawn(connection);
        for (host, expected) in [
            ("wrong.test", StatusCode::MISDIRECTED_REQUEST),
            ("app.test", StatusCode::OK),
            ("another.test", StatusCode::MISDIRECTED_REQUEST),
            ("app.test", StatusCode::OK),
        ] {
            let response = sender
                .send_request(
                    Request::builder()
                        .version(Version::HTTP_2)
                        .uri(format!("https://{host}/"))
                        .body(Empty::<Bytes>::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.version(), Version::HTTP_2);
            assert_eq!(response.status(), expected);
            assert!(!response.headers().contains_key(http::header::CONNECTION));
            let body = response.into_body().collect().await.unwrap().to_bytes();
            if expected == StatusCode::OK {
                assert_eq!(body, "healthy");
            } else {
                assert!(body.is_empty());
            }
            assert_eq!(
                fixture.gate.count(),
                1,
                "other streams retain the connection"
            );
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
        drop(sender);
        fixture.finish().await;
        while let Some(result) = drivers.join_next().await {
            result.unwrap().unwrap();
        }
    })
    .await
    .expect("H2 rejection must leave the connection usable and drainable");
}
