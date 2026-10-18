use std::sync::atomic::Ordering;

use bytes::Bytes;
use http::{Request, StatusCode, Version};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use super::framing_cases::read_closed;
use super::support::{TEST_TIMEOUT, TlsFixture};

#[tokio::test]
async fn invalid_host_rejects_before_tls_dispatch() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        for headers in [
            "",
            "Host: app.test\r\nHost: app.test\r\n",
            "Host: app.test\r\nHost: wrong.test\r\n",
            "Host: app.test:invalid\r\n",
            "Host: user@app.test\r\n",
        ] {
            let mut peer =
                fixture.connect("app.test", b"http/1.1").await.unwrap();
            peer.write_all(
                format!(
                    "POST / HTTP/1.1\r\n{headers}Content-Length: 5\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
            let response = read_closed(&mut peer).await;
            assert!(
                response.starts_with("HTTP/1.1 400 "),
                "{headers:?}: {response}"
            );
            assert!(response.split_once("\r\n\r\n").unwrap().1.is_empty());
            drop(peer);
            fixture.reap_connection().await;
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        fixture.finish().await;
    })
    .await
    .expect("invalid authority must reject without reading a body");
}

#[tokio::test]
async fn http10_without_host_can_use_established_tls_binding() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        let mut peer = fixture.connect("app.test", b"http/1.1").await.unwrap();
        peer.write_all(b"GET / HTTP/1.0\r\n\r\n").await.unwrap();
        let response = read_closed(&mut peer).await;
        assert!(response.starts_with("HTTP/1.0 200 "), "{response}");
        assert!(response.ends_with("healthy"));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        drop(peer);
        fixture.finish().await;
    })
    .await
    .expect("HTTP/1.0 authority omission must retain its distinct semantics");
}

#[tokio::test]
async fn invalid_h2_authority_rejects_only_its_stream() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        let peer = fixture.connect("app.test", b"h2").await.unwrap();
        let (mut sender, connection) =
            hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
                TokioExecutor::new(),
                TokioIo::new(peer),
            )
            .await
            .unwrap();
        let mut drivers = JoinSet::new();
        drivers.spawn(connection);
        for headers in
            [vec!["wrong.test"], vec!["app.test", "app.test"], vec![]]
        {
            let expected = if headers.is_empty() {
                StatusCode::OK
            } else {
                StatusCode::BAD_REQUEST
            };
            let mut request = Request::builder()
                .version(Version::HTTP_2)
                .uri("https://app.test/");
            for host in headers {
                request = request.header("host", host);
            }
            let response = sender
                .send_request(request.body(Empty::<Bytes>::new()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            if expected == StatusCode::OK {
                assert_eq!(body, "healthy");
            } else {
                assert!(body.is_empty());
            }
            assert_eq!(fixture.gate.count(), 1);
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        drop(sender);
        fixture.finish().await;
        while let Some(result) = drivers.join_next().await {
            result.unwrap().unwrap();
        }
    })
    .await
    .expect("invalid H2 authority cannot close other streams");
}
