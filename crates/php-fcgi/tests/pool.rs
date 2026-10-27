use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;

use tokio::net::UnixListener;

use super::*;

static SEQ: AtomicUsize = AtomicUsize::new(0);

fn unique_path() -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("clv-pool-{}-{n}.sock", std::process::id()))
}

fn opts() -> PoolOpts {
    PoolOpts {
        max_live: 4,
        min_live: 0,
        max_idle: 4,
        connect_timeout: Duration::from_secs(2),
        acquire_timeout: Duration::from_secs(2),
        exec_timeout: Duration::from_secs(5),
        head_timeout: Duration::from_secs(5),
        max_lifetime: None,
        max_requests: None,
        idle_timeout: None,
        ping_path: None,
        replenish: Some(crate::config::ReplenishConfig::default()),
    }
}

/// One FPM child reaching its own `pm.max_requests` breaks exactly
/// like an FPM that went away, and is far commoner. So one break
/// retires its own connection and nothing else; the queue is dropped
/// only when a second reused connection agrees, and a reused
/// connection that works in between takes the agreement away.
#[tokio::test]
async fn a_queue_is_condemned_by_two_breaks_and_acquitted_by_one_success() {
    let sock = unique_path();
    let listener = UnixListener::bind(&sock).expect("bind");
    // Accepts and holds: this test dials, it never speaks FCGI.
    let fpm = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((conn, _)) = listener.accept().await {
            held.push(conn);
        }
    });

    let pool = FcgiPool::new(
        FcgiEndpoint::Unix(sock.clone()),
        opts(),
        CancellationToken::new(),
    );

    let a = pool.acquire().await.expect("dial one");
    let b = pool.acquire().await.expect("dial two");
    drop(a);
    drop(b);
    assert_eq!(pool.stats().idle, 2);

    assert_eq!(pool.report_stale_keepalive(), StaleKeepalive::Suspected);
    assert_eq!(
        pool.stats().idle,
        2,
        "one break is the connection's own evidence"
    );

    let good = pool.acquire().await.expect("reuse");
    assert!(
        !good.is_fresh(),
        "the point of the check is that it came from the queue"
    );
    drop(good);
    assert_eq!(
        pool.report_stale_keepalive(),
        StaleKeepalive::Suspected,
        "the connection that worked cleared what the earlier break was holding"
    );

    assert_eq!(
        pool.report_stale_keepalive(),
        StaleKeepalive::Confirmed { purged: 2 }
    );
    assert_eq!(pool.stats().idle, 0);
    assert_eq!(pool.stats().live, 0, "and the slots the queue held are back");

    // The verdict is the one thing a gauge could never show: both
    // readings look like an empty pool a moment later.
    #[cfg(feature = "metrics")]
    {
        let events = pool.events();
        assert_eq!(events.dialled, 2, "counted where the connection is opened");
        assert_eq!(
            events.queue_suspected, 2,
            "two breaks that left the queue standing"
        );
        assert_eq!(events.queue_condemned, 1);
        assert_eq!(
            events.purged, 2,
            "a purge is counted by what it cost, not by itself"
        );
    }

    fpm.abort();
    let _ = std::fs::remove_file(&sock);
}

/// The retry after a break must not draw from the queue that broke —
/// the next connection out of it has the same provenance as the one
/// that failed. At the ceiling the only slots left are the ones that
/// queue is holding, so it makes its own room out of them.
#[tokio::test]
async fn a_fresh_acquire_refuses_the_queue_and_makes_its_own_room() {
    let sock = unique_path();
    let listener = UnixListener::bind(&sock).expect("bind");
    let fpm = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((conn, _)) = listener.accept().await {
            held.push(conn);
        }
    });

    let pool = FcgiPool::new(
        FcgiEndpoint::Unix(sock.clone()),
        PoolOpts { max_live: 2, max_idle: 2, ..opts() },
        CancellationToken::new(),
    );

    let mut a = pool.acquire().await.expect("dial one");
    let mut b = pool.acquire().await.expect("dial two");
    let pooled = [a.connection().id(), b.connection().id()];
    drop(a);
    drop(b);
    assert_eq!(pool.stats().live, 2, "the pool is at its ceiling");
    assert_eq!(pool.stats().idle, 2);

    let mut fresh = pool.acquire_fresh().await.expect("fresh");
    assert!(fresh.is_fresh());
    assert!(
        !pooled.contains(&fresh.connection().id()),
        "not one the queue was holding"
    );
    assert_eq!(
        pool.stats().live,
        2,
        "the room came out of the queue, not out of the ceiling"
    );
    assert_eq!(
        pool.stats().idle,
        1,
        "and exactly one suspect connection paid for it"
    );

    #[cfg(feature = "metrics")]
    {
        let events = pool.events();
        assert_eq!(
            events.dialled, 3,
            "two warm plus the one that refused them"
        );
        assert_eq!(
            events.purged, 1,
            "the room it made is a dropped connection like any other"
        );
    }

    fpm.abort();
    let _ = std::fs::remove_file(&sock);
}

#[test]
fn abandoned_stream_dial_reservation_returns_capacity_without_a_socket() {
    let mut config = opts();
    config.max_live = 1;
    let pool = FcgiPool::new(
        FcgiEndpoint::Unix(PathBuf::from("unused-fpm.sock")),
        config,
        CancellationToken::new(),
    );
    assert!(matches!(pool.inner.try_admit(), Admission::Granted));
    let reservation = DialSlot { inner: &pool.inner, retained: false };
    assert!(matches!(pool.inner.try_admit(), Admission::WorkerFull));
    drop(reservation);
    assert_eq!(pool.stats().live, 0);
    assert!(matches!(pool.inner.try_admit(), Admission::Granted));
    drop(DialSlot { inner: &pool.inner, retained: false });
    assert_eq!(pool.stats().live, 0);
}

#[tokio::test]
async fn audit_replenish_retry_wait_is_cancelled_and_drain_is_pool_local() {
    use futures_util::poll;
    let parent = CancellationToken::new();
    let cancel = parent.child_token();
    let wait = wait_retry(&cancel, Duration::from_secs(3600));
    tokio::pin!(wait);
    assert!(poll!(&mut wait).is_pending());
    cancel.cancel();
    assert!(!wait.await);
    assert!(!wait_retry(&cancel, Duration::from_secs(3600)).await);
    assert!(!parent.is_cancelled());

    for disabled in [false, true] {
        let mut config = opts();
        config.min_live = usize::from(disabled);
        if disabled {
            config.replenish = None;
        }
        let pool = FcgiPool::new(
            FcgiEndpoint::Unix(PathBuf::from("unused-fpm.sock")),
            config,
            parent.clone(),
        );
        pool.maybe_spawn_replenisher();
        assert!(pool.inner.replenish_tasks.is_empty());
        pool.shutdown().await;
        assert!(pool.inner.replenish_tasks.is_closed());
        assert!(pool.inner.replenish_tasks.is_empty());
        assert!(!parent.is_cancelled());
    }
}
