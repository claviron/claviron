use std::sync::atomic::{AtomicU32, Ordering};

use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;

use super::*;
use crate::connection::FcgiEndpoint;

static SEQ: AtomicU32 = AtomicU32::new(0);

/// An FPM that accepts the connection and never answers must not stop
/// the metrics loop.
///
/// The dial is not the half that hangs — the kernel accepts into the
/// listen backlog whether or not a child is free — so `connect_timeout`
/// bounds nothing here. Everything after it was unbounded, and the
/// caller awaits this inline, so one unanswered scrape ended the whole
/// family for the life of the process with nothing logged.
#[tokio::test]
async fn a_silent_fpm_is_reported_down_rather_than_waited_for() {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let sock = std::env::temp_dir()
        .join(format!("clv-fpm-scrape-{}-{n}.sock", std::process::id()));
    let listener = UnixListener::bind(&sock).expect("bind fake fpm");

    let fpm = tokio::spawn(async move {
        // Accept, read the request, and say nothing at all.
        let (mut conn, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 1024];
        loop {
            match conn.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
    });

    let target = FpmStatusTarget {
        name: "www".to_string(),
        endpoint: FcgiEndpoint::Unix(sock.clone()),
        connect_timeout: Duration::from_secs(2),
        status_path: "/status".to_string(),
    };

    let started = std::time::Instant::now();
    let pool = scrape_target(target, Duration::from_millis(200)).await;

    assert!(!pool.up, "a pool that never answered is down, not absent");
    assert_eq!(pool.pool, "www");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "bounded by the scrape budget, not by the dial timeout"
    );

    drop(fpm);
    let _ = std::fs::remove_file(&sock);
}
