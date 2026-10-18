use tokio::io::AsyncWriteExt;

use super::framing_cases::read_closed;
use super::*;

#[tokio::test]
async fn invalid_host_rejects_before_cleartext_dispatch() {
    tokio::time::timeout(TIMEOUT, async {
        let fixture = Fixture::new().await;
        for headers in [
            "",
            "Host: app.test\r\nHost: app.test\r\n",
            "Host: bypass.test\r\nHost: wrong.test\r\n",
            "Host: app.test:invalid\r\n",
            "Host: user@app.test\r\n",
        ] {
            let mut wire = fixture.raw().await;
            wire.stream
                .write_all(
                    format!(
                        "POST / HTTP/1.1\r\n{headers}Content-Length: 5\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let response = read_closed(&mut wire.stream).await;
            assert!(
                response.starts_with("HTTP/1.1 400 "),
                "{headers:?}: {response}"
            );
            assert!(response.split_once("\r\n\r\n").unwrap().1.is_empty());
            wire.finish().await;
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    })
    .await
    .expect("invalid authority must reject without reading a body");
}

#[tokio::test]
async fn absolute_target_selects_cleartext_host_without_rewriting_uri() {
    tokio::time::timeout(TIMEOUT, async {
        let fixture = Fixture::new().await;
        let mut wire = fixture.raw().await;
        wire.stream
            .write_all(b"GET http://bypass.test/original?q=1 HTTP/1.1\r\nHost: app.test\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let response = read_closed(&mut wire.stream).await;
        assert!(response.starts_with("HTTP/1.1 200 "));
        assert!(response.contains("bypass.test|bypass.test|http://bypass.test/original?q=1|"));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        wire.finish().await;
    })
    .await
    .expect("valid absolute-form routing must finish");
}

#[cfg(feature = "acme")]
#[tokio::test]
async fn challenge_requires_valid_message_but_not_a_hosted_name() {
    tokio::time::timeout(TIMEOUT, async {
        let mut fixture = Fixture::new().await;
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("test-token"), "challenge-proof").unwrap();
        let snapshot = Arc::get_mut(&mut fixture.snapshot).unwrap();
        snapshot.http01_directory = Some(Arc::new(directory.path().to_owned()));
        let router: claviron_http::config::RouterConfig =
            serde_yaml::from_str("deny: ['^/']").unwrap();
        snapshot.deny = Arc::new(DenyService::from_set(router.deny_set()));
        for headers in ["", "Host: app.test\r\nHost: wrong.test\r\n", "Host: bad:port\r\n"] {
            let mut wire = fixture.raw().await;
            wire.stream.write_all(format!("GET /.well-known/acme-challenge/test-token HTTP/1.1\r\n{headers}Connection: close\r\n\r\n").as_bytes()).await.unwrap();
            let response = read_closed(&mut wire.stream).await;
            assert!(response.starts_with("HTTP/1.1 400 "), "{response}");
            assert!(response.split_once("\r\n\r\n").unwrap().1.is_empty());
            wire.finish().await;
        }
        let mut wire = fixture.raw().await;
        wire.stream.write_all(b"GET /.well-known/acme-challenge/test-token HTTP/1.1\r\nHost: unhosted.test\r\nConnection: close\r\n\r\n").await.unwrap();
        let response = read_closed(&mut wire.stream).await;
        assert!(response.starts_with("HTTP/1.1 200 "));
        assert!(response.ends_with("challenge-proof"));
        wire.finish().await;
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }).await.expect("HTTP-01 cannot bypass message syntax validation");
}
