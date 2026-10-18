use std::sync::atomic::Ordering;

use tokio::io::AsyncWriteExt;

use super::framing_cases::*;
use super::support::{TEST_TIMEOUT, TlsFixture};

#[tokio::test]
async fn malformed_headers_never_enter_tls_application() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        for headers in INVALID_HEADERS {
            let mut peer =
                fixture.connect("app.test", b"http/1.1").await.unwrap();
            peer.write_all(malformed("app.test", headers).as_bytes())
                .await
                .unwrap();
            assert_bad_request(&read_closed(&mut peer).await, headers);
            drop(peer);
            fixture.reap_connection().await;
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        fixture.finish().await;
    })
    .await
    .expect("malformed headers must reject without waiting for a body");
}

#[tokio::test]
async fn valid_framing_preserves_tls_keepalive() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        let mut peer = fixture.connect("app.test", b"http/1.1").await.unwrap();
        peer.write_all(valid_keepalive("app.test").as_bytes()).await.unwrap();
        let response = read_closed(&mut peer).await;
        assert_eq!(response.matches("HTTP/1.1 200 ").count(), 2, "{response}");
        assert_eq!(response.matches("healthy").count(), 2);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
        drop(peer);
        fixture.finish().await;
    })
    .await
    .expect("valid framing must remain usable");
}

#[tokio::test]
async fn mixed_content_length_and_chunked_cannot_reuse_tls_connection() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut fixture = TlsFixture::new().await;
        let mut peer = fixture.connect("app.test", b"http/1.1").await.unwrap();
        peer.write_all(ambiguous_framing("app.test").as_bytes()).await.unwrap();
        let response = read_closed(&mut peer).await;
        assert_eq!(response.matches("HTTP/1.1 200 ").count(), 1, "{response}");
        assert_eq!(response.matches("healthy").count(), 1);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        drop(peer);
        fixture.finish().await;
    })
    .await
    .expect("ambiguous framing must close before another request");
}
