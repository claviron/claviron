use claviron_http::config::H1Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::framing_cases::read_closed;
use super::*;

fn inherited_h1(value: &str) -> H1Config {
    let yaml = format!(
        "defaults: {{listener: {{h1: {{header_read_timeout: {value}}}}}}}\nlisteners: {{local: {{transport: tcp, tls: false, protocols: h1, listen: '127.0.0.1:0', requests: {{max_body_size: off, body_read_timeout: off}}}}}}"
    );
    let resolved =
        serde_yaml::from_str::<HttpYaml>(&yaml).unwrap().resolve().unwrap();
    let ListenerSettings::Tcp(config) =
        &resolved.listeners.values().next().unwrap().settings
    else {
        unreachable!()
    };
    config.h1.clone()
}

fn publish_h1(
    fixture: &mut Fixture,
    h1: H1Config,
) {
    let previous = &fixture.snapshot;
    fixture.snapshot = Arc::new(ServerSnapshot {
        listener: previous.listener.clone(),
        tcp: previous.tcp.clone(),
        h1,
        redirect_port: previous.redirect_port,
        http01_directory: previous.http01_directory.clone(),
        deny: previous.deny.clone(),
    });
}

#[tokio::test]
async fn inherited_header_deadline_closes_partial_request_and_releases_capacity()
 {
    tokio::time::timeout(TIMEOUT, async {
        let mut fixture = Fixture::new().await;
        publish_h1(&mut fixture, inherited_h1("25ms"));
        let mut wire = fixture.raw().await;
        wire.stream
            .write_all(b"GET / HTTP/1.1\r\nHost: app.test\r\n")
            .await
            .unwrap();
        let response = read_closed(&mut wire.stream).await;
        // Hyper closes a timed-out incomplete head; it need not synthesize
        // an HTTP response when there is no complete request to dispatch.
        assert!(response.is_empty(), "{response}");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        wire.finish().await;
    })
    .await
    .expect("a finite inherited deadline must bound incomplete headers");
}

#[tokio::test]
async fn timeout_reload_affects_new_connections_not_captured_ones() {
    tokio::time::timeout(TIMEOUT, async {
        let mut fixture = Fixture::new().await;
        publish_h1(&mut fixture, inherited_h1("null"));
        let mut old = fixture.raw().await;
        old.stream
            .write_all(
                b"GET / HTTP/1.1\r\nHost: app.test\r\nConnection: close\r\n",
            )
            .await
            .unwrap();
        publish_h1(&mut fixture, inherited_h1("25ms"));
        let mut new = fixture.raw().await;
        new.stream
            .write_all(b"GET / HTTP/1.1\r\nHost: app.test\r\n")
            .await
            .unwrap();
        assert!(read_closed(&mut new.stream).await.is_empty());
        new.finish().await;
        let mut byte = [0];
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                old.stream.read(&mut byte)
            )
            .await
            .is_err(),
            "old null policy must not acquire the new deadline"
        );
        assert_eq!(old.gate.count(), 1);
        old.stream.write_all(b"\r\n").await.unwrap();
        assert!(
            read_closed(&mut old.stream).await.starts_with("HTTP/1.1 200 ")
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        old.finish().await;
    })
    .await
    .expect("captured and newly published timeout policies remain distinct");
}
