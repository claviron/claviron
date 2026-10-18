use tokio::io::AsyncWriteExt;

use super::framing_cases::*;
use super::*;

#[tokio::test]
async fn malformed_headers_never_enter_cleartext_application() {
    tokio::time::timeout(TIMEOUT, async {
        let fixture = Fixture::new().await;
        for host in ["app.test", "bypass.test"] {
            for headers in INVALID_HEADERS {
                let mut wire = fixture.raw().await;
                wire.stream
                    .write_all(malformed(host, headers).as_bytes())
                    .await
                    .unwrap();
                assert_bad_request(
                    &read_closed(&mut wire.stream).await,
                    headers,
                );
                wire.finish().await;
            }
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    })
    .await
    .expect("malformed headers must reject without waiting for a body");
}

#[tokio::test]
async fn valid_framing_preserves_cleartext_keepalive() {
    tokio::time::timeout(TIMEOUT, async {
        let fixture = Fixture::new().await;
        let mut wire = fixture.raw().await;
        wire.stream
            .write_all(valid_keepalive("bypass.test").as_bytes())
            .await
            .unwrap();
        let response = read_closed(&mut wire.stream).await;
        assert_eq!(response.matches("HTTP/1.1 200 ").count(), 2, "{response}");
        assert!(response.contains("|one"));
        assert!(response.ends_with("|two"));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
        wire.finish().await;
    })
    .await
    .expect("valid framing must remain usable");
}

#[tokio::test]
async fn mixed_content_length_and_chunked_cannot_reuse_cleartext_connection() {
    tokio::time::timeout(TIMEOUT, async {
        let fixture = Fixture::new().await;
        let mut wire = fixture.raw().await;
        wire.stream
            .write_all(ambiguous_framing("bypass.test").as_bytes())
            .await
            .unwrap();
        let response = read_closed(&mut wire.stream).await;
        assert_eq!(response.matches("HTTP/1.1 200 ").count(), 1, "{response}");
        assert!(response.ends_with("|one"));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        wire.finish().await;
    })
    .await
    .expect("ambiguous framing must close before another request");
}
