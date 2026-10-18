//! Explicit, bounded native-driver churn probe; not a throughput benchmark.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{Request, StatusCode, Version};
use http_body_util::{BodyExt, Empty, Limited};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use super::framing_cases::{assert_bad_request, malformed, read_closed};
use super::support::TlsFixture;

const WARMUP_REQUESTS: usize = 8;
const BATCHES: usize = 4;
const ATTEMPTS_PER_BATCH: usize = 64;
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

async fn healthy_h1(fixture: &mut TlsFixture) {
    let mut peer = fixture.connect("app.test", b"http/1.1").await.unwrap();
    peer.write_all(
        b"GET / HTTP/1.1\r\nHost: app.test\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    let response = read_closed(&mut peer).await;
    assert!(response.starts_with("HTTP/1.1 200 "), "{response}");
    assert!(response.ends_with("healthy"));
    drop(peer);
    fixture.reap_connection().await;
}

async fn healthy_h2(fixture: &mut TlsFixture) {
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
    let response = sender
        .send_request(
            Request::builder()
                .version(Version::HTTP_2)
                .uri("https://app.test/control")
                .body(Empty::<Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(
        Limited::new(response.into_body(), 128)
            .collect()
            .await
            .unwrap()
            .to_bytes(),
        "healthy"
    );
    drop(sender);
    // This probe deliberately disconnects its client after the control reply.
    // Graceful GOAWAY is tested separately; no detached client driver survives.
    drivers.abort_all();
    while let Some(result) = drivers.join_next().await {
        match result {
            Ok(result) => result.unwrap(),
            Err(error) => {
                assert!(error.is_cancelled(), "client driver panicked: {error}")
            }
        }
    }
    fixture.reap_connection().await;
}

#[tokio::test]
#[ignore = "opt-in bounded churn and diagnostic RSS samples; run just test-http-resilience"]
async fn resilience_tls_churn_reclaims_each_slot_and_keeps_h1_h2_healthy() {
    tokio::time::timeout(PROBE_TIMEOUT, async {
        // Unknown SNI reaches the configured fallback, then the real client
        // verifier rejects its app.test certificate. No verifier bypass is used.
        let mut fixture = TlsFixture::new_default_host().await;
        for _ in 0..WARMUP_REQUESTS {
            healthy_h1(&mut fixture).await;
        }
        let mut rss = [None; BATCHES + 1];
        let mut controls = [Duration::ZERO; BATCHES];
        rss[0] = claviron_sys::rss::read_rss(None);
        for batch in 0..BATCHES {
            let calls_before = fixture.calls.load(Ordering::SeqCst);
            for attempt in 0..ATTEMPTS_PER_BATCH {
                match attempt % 4 {
                    0 => {
                        let name = format!("unhosted-{batch}-{attempt}.test");
                        assert!(fixture.connect(&name, b"http/1.1").await.is_err());
                    }
                    1 => {
                        let mut peer = fixture.tcp().await;
                        peer.write_all(b"not a TLS ClientHello\r\n").await.unwrap();
                        peer.shutdown().await.unwrap();
                    }
                    2 => {
                        let mut peer = fixture.connect("app.test", b"http/1.1").await.unwrap();
                        let bad_header = "Content-Length : 0\r\n";
                        peer.write_all(malformed("app.test", bad_header).as_bytes()).await.unwrap();
                        assert_bad_request(&read_closed(&mut peer).await, bad_header);
                    }
                    _ => {
                        let mut peer = fixture.connect("app.test", b"http/1.1").await.unwrap();
                        peer.write_all(b"GET / HTTP/1.1\r\nHost: other.test\r\nConnection: close\r\n\r\n")
                            .await.unwrap();
                        let response = read_closed(&mut peer).await;
                        assert!(response.starts_with("HTTP/1.1 421 "), "{response}");
                        assert!(response.split_once("\r\n\r\n").unwrap().1.is_empty());
                    }
                }
                fixture.reap_connection().await;
                assert_eq!(fixture.calls.load(Ordering::SeqCst), calls_before);
            }
            let began = Instant::now();
            healthy_h1(&mut fixture).await;
            healthy_h2(&mut fixture).await;
            controls[batch] = began.elapsed();
            assert_eq!(fixture.calls.load(Ordering::SeqCst), calls_before + 2);
            rss[batch + 1] = claviron_sys::rss::read_rss(None);
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), WARMUP_REQUESTS + BATCHES * 2);
        fixture.finish().await;

        eprintln!(
            "bounded TLS probe: os={}, arch={}, debug_assertions={}, warmup={}, batches={}x{}, live_connections<=1, deadline={:?}",
            std::env::consts::OS, std::env::consts::ARCH, cfg!(debug_assertions),
            WARMUP_REQUESTS, BATCHES, ATTEMPTS_PER_BATCH, PROBE_TIMEOUT
        );
        eprintln!("post-warmup RSS bytes (None = unavailable): {rss:?}");
        eprintln!("healthy H1+H2 control-pair elapsed after each batch: {controls:?}");
        eprintln!(
            "completed: rejected={}, healthy={}; every server connection and client driver joined; RSS has no pass/fail threshold",
            BATCHES * ATTEMPTS_PER_BATCH, WARMUP_REQUESTS + BATCHES * 2
        );
    })
    .await
    .expect("bounded TLS churn must recover and join all owned work before its deadline");
}
