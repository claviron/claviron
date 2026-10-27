use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::time::timeout;

use super::*;

static SEQ: AtomicU32 = AtomicU32::new(0);

fn unique_path(suffix: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("clv-fcgi-{}-{n}{suffix}", std::process::id()))
}

/// One FCGI END_REQUEST record for request id 1: eight bytes of
/// body (app status, protocol status, reserved) in an eight-byte
/// header, which is what ends a response.
fn end_request_record() -> Vec<u8> {
    let mut rec = vec![1, 3, 0, 1, 0, 8, 0, 0];
    rec.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    rec
}

/// One FCGI STDOUT record for request id 1.
fn stdout_record(payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u16;
    let mut rec = vec![1, 6, 0, 1, (len >> 8) as u8, (len & 0xff) as u8, 0, 0];
    rec.extend_from_slice(payload);
    rec
}

/// A streamed response the client walks away from must retire its
/// connection, not re-publish it.
///
/// The FCGI socket still holds this response's unread records at that
/// point, and `ResponseStream` does not drain them on drop — so a
/// connection returned to the idle queue here would serve this
/// response's tail to whoever leases it next.
///
/// The fake FPM signals by observing its peer close, which is exactly
/// what "the connection was retired" means on the wire. If the
/// connection went back to the pool instead, no close arrives and the
/// bounded wait fails the test rather than hanging it.
#[tokio::test]
async fn abandoned_stream_retires_the_connection_instead_of_pooling_it() {
    let sock = unique_path(".sock");
    let listener = UnixListener::bind(&sock).expect("bind fake fpm");

    let (body_dropped_tx, body_dropped_rx) = oneshot::channel::<()>();
    let (peer_closed_tx, peer_closed_rx) = oneshot::channel::<()>();

    let fpm = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.expect("accept");
        // Headers only: the pump parses them, hands the head to the
        // caller, and blocks for more records without having touched
        // the body channel yet.
        conn.write_all(&stdout_record(
            b"Status: 200\r\nContent-Type: text/plain\r\n\r\n",
        ))
        .await
        .expect("write head");
        conn.flush().await.expect("flush head");

        let _ = body_dropped_rx.await;

        // Receiver departure must wake the pump while this backend stays
        // silent; sending another body record would hide a missing wake.

        let mut buf = [0u8; 1024];
        loop {
            match conn.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        let _ = peer_closed_tx.send(());
    });

    let pool = FcgiPool::new(
        FcgiEndpoint::Unix(sock.clone()),
        PoolOpts {
            max_live: 1,
            min_live: 0,
            max_idle: 1,
            connect_timeout: Duration::from_secs(2),
            acquire_timeout: Duration::from_secs(2),
            exec_timeout: Duration::from_secs(5),
            head_timeout: Duration::from_secs(5),
            max_lifetime: None,
            max_requests: None,
            idle_timeout: None,
            ping_path: None,
            replenish: Some(crate::config::ReplenishConfig::default()),
        },
        CancellationToken::new(),
    );

    let params =
        Params::default().request_method("GET").script_name("/index.php");
    let (status, _headers, body) = exec_pooled_stream(
        "test".into(),
        pool.clone(),
        params,
        Stdin::Whole(Bytes::new()),
    )
    .await
    .expect("head resolves");
    assert_eq!(status, StatusCode::OK);

    assert_eq!(pool.stats().live, 1, "the lease is still held by the pump");

    drop(body);
    let _ = body_dropped_tx.send(());

    timeout(Duration::from_secs(5), peer_closed_rx)
        .await
        .expect("connection must be closed, not returned to the pool")
        .expect("fpm side signalled");

    let stats = pool.stats();
    assert_eq!(
        stats.idle, 0,
        "abandoned connection must not join the idle queue"
    );
    assert_eq!(stats.live, 0, "its live slot must be released");

    fpm.await.expect("fake fpm");
    let _ = std::fs::remove_file(&sock);
}

/// An FPM that takes the request and says nothing must not hold the
/// caller for ever.
///
/// The streaming path puts no wall-clock cap on the body, which is
/// the point of it, and so had no bound at all on the wait before
/// the body began. `head_timeout` covers that stretch and nothing
/// after it. The pump has to be the one to enforce it: a timeout
/// only around the caller's wait would leave the pump holding the
/// lease, so the slot would never come back.
#[tokio::test]
async fn a_silent_fpm_gives_the_head_back_and_frees_the_slot() {
    let sock = unique_path(".sock");
    let listener = UnixListener::bind(&sock).expect("bind fake fpm");

    let (peer_closed_tx, peer_closed_rx) = oneshot::channel::<()>();
    let fpm = tokio::spawn(async move {
        // Accept, then say nothing at all.
        let (mut conn, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 1024];
        loop {
            match conn.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        let _ = peer_closed_tx.send(());
    });

    let pool = FcgiPool::new(
        FcgiEndpoint::Unix(sock.clone()),
        PoolOpts {
            max_live: 1,
            min_live: 0,
            max_idle: 1,
            connect_timeout: Duration::from_secs(2),
            acquire_timeout: Duration::from_secs(2),
            exec_timeout: Duration::from_secs(30),
            head_timeout: Duration::from_millis(200),
            max_lifetime: None,
            max_requests: None,
            idle_timeout: None,
            ping_path: None,
            replenish: Some(crate::config::ReplenishConfig::default()),
        },
        CancellationToken::new(),
    );

    let params =
        Params::default().request_method("GET").script_name("/index.php");
    let started = std::time::Instant::now();
    let outcome = exec_pooled_stream(
        "test".into(),
        pool.clone(),
        params,
        Stdin::Whole(Bytes::new()),
    )
    .await;

    match outcome {
        Err(ExecuteError::Timeout(stage)) => {
            assert_eq!(stage, "response headers")
        }
        Err(other) => panic!("expected a head timeout, got {other}"),
        Ok(_) => panic!("a silent FPM must not produce a head"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "bounded by head_timeout, not by exec_timeout"
    );

    // And the pump let go with it: the connection is retired rather
    // than left held by a task still waiting on a silent socket.
    timeout(Duration::from_secs(5), peer_closed_rx)
        .await
        .expect("the pump must release the connection")
        .expect("fpm side signalled");
    assert_eq!(pool.stats().live, 0, "the slot comes back");

    fpm.await.expect("fake fpm");
    let _ = std::fs::remove_file(&sock);
}

/// A route naming more than one FPM entry has to resolve on its first
/// request. `entries` is lazy, so the balancer is what builds the
/// candidates it ranks — ranking a map nobody has populated yet finds
/// nothing and reports the whole list missing.
#[tokio::test]
async fn a_multi_entry_fpm_list_resolves_before_anything_has_touched_it() {
    let dir = unique_path(".config");
    std::fs::create_dir_all(&dir).expect("config dir");
    std::fs::write(
        dir.join("fpm.yaml"),
        "fpm:\n  \
         primary:\n    unix: /nonexistent/primary.sock\n    \
         pool: { min_live: 0, max_idle: 2, max_live: 4 }\n  \
         fallback:\n    unix: /nonexistent/fallback.sock\n    \
         pool: { min_live: 0, max_idle: 2, max_live: 4 }\n  \
         direct:\n    unix: /nonexistent/direct.sock\n",
    )
    .expect("write fpm.yaml");

    let state = AppState::standalone("fcgi-balancer-test", &dir);
    let svc = PhpFcgiService::from_state(state.clone());
    svc.rebuild(&state).await.expect("parse fpm.yaml");

    let balanced = ["primary".to_string(), "fallback".to_string()];
    let (name, entry) =
        svc.resolve_entry(&balanced).await.expect("balanced list resolves");
    assert!(matches!(entry, FpmEntry::Pooled(_)));
    assert!(balanced.contains(&name), "picked `{name}`");

    // A one-shot entry is the overflow path, not the preference: a
    // pool with room wins over it.
    let mixed = ["primary".to_string(), "direct".to_string()];
    let (name, _) =
        svc.resolve_entry(&mixed).await.expect("mixed list resolves");
    assert_eq!(name, "primary");

    let _ = std::fs::remove_dir_all(&dir);
}

/// A reload that changes a number has to reach the pool.
///
/// A pool's options are baked at construction, so the only way a new
/// `max_live` takes effect is a new pool. Reconciliation used to
/// remove only names that had left the catalog, which meant a reload
/// logged success and changed nothing — the case an operator hits
/// when they raise a ceiling under pressure.
#[tokio::test]
async fn a_reconfigured_entry_is_dropped_so_the_next_request_rebuilds_it() {
    let dir = unique_path(".reload");
    std::fs::create_dir_all(&dir).expect("config dir");
    let path = dir.join("fpm.yaml");
    let write = |max_live: u32| {
        std::fs::write(
            &path,
            format!(
                "fpm:\n  www:\n    unix: /nonexistent/www.sock\n    \
                 pool: {{ min_live: 0, max_idle: 2, max_live: {max_live} }}\n"
            ),
        )
        .expect("write");
    };

    let state = AppState::standalone("fcgi-reload-test", &dir);
    let svc = PhpFcgiService::from_state(state.clone());

    write(4);
    svc.rebuild(&state).await.expect("parse");
    let names = ["www".to_string()];
    let (_, entry) = svc.resolve_entry(&names).await.expect("resolve");
    let FpmEntry::Pooled(pool) = entry else { panic!("pooled") };
    assert_eq!(pool.opts().max_live, 4);

    // Same file, same numbers: the entry must survive, or every
    // reload would throw away warm connections for nothing.
    svc.rebuild(&state).await.expect("parse");
    svc.reconcile_pools().await.expect("reconcile");
    assert_eq!(svc.entries.read().await.len(), 1, "an unchanged entry is kept");

    // Now raise the ceiling.
    write(9);
    svc.rebuild(&state).await.expect("parse");
    svc.reconcile_pools().await.expect("reconcile");
    assert!(svc.entries.read().await.is_empty(), "a changed entry is dropped");

    let (_, entry) = svc.resolve_entry(&names).await.expect("resolve");
    let FpmEntry::Pooled(pool) = entry else { panic!("pooled") };
    assert_eq!(pool.opts().max_live, 9, "the new ceiling reached the pool");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pool capacity is process-local and the three tiers are reconciled
/// against that single ceiling.
#[test]
fn pool_capacity_is_process_local() {
    let cfg: FpmDocument = serde_yaml::from_str(
        "fpm:\n  www:\n    unix: /run/php.sock\n    \
         pool: { min_idle: 2, max_idle: 8, max_live: 32 }\n",
    )
    .expect("parse");
    let pool = FcgiPool::new(
        FcgiEndpoint::Unix("/run/php.sock".into()),
        build_opts(cfg.fpm.get("www").expect("www")),
        CancellationToken::new(),
    );
    assert_eq!(pool.opts().max_live, 32);
    assert_eq!(pool.opts().max_idle, 8);
    assert_eq!(pool.opts().min_live, 2);
}

/// A connection replays only while FPM has said nothing.
///
/// The evidence has to be per request, not per connection: a pooled
/// connection is configured here to serve 900 of them, so a flag kept
/// for the connection's life would be set by the first response and
/// veto every replay after it.
#[tokio::test]
async fn evidence_belongs_to_the_request_and_not_to_the_connection() {
    let sock = unique_path(".sock");
    let listener = UnixListener::bind(&sock).expect("bind fake fpm");

    let (answered_tx, answered_rx) = oneshot::channel::<()>();
    let fpm = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.expect("accept");
        // Request one: a complete reply, so the connection has now
        // delivered bytes and can go back to the pool.
        conn.write_all(&stdout_record(b"Content-Type: text/plain\r\n\r\nok"))
            .await
            .expect("write");
        conn.write_all(&end_request_record()).await.expect("end");
        conn.flush().await.expect("flush");
        let _ = answered_rx.await;
        conn
    });

    let pool = FcgiPool::new(
        FcgiEndpoint::Unix(sock.clone()),
        PoolOpts {
            max_live: 1,
            min_live: 0,
            max_idle: 1,
            connect_timeout: Duration::from_secs(2),
            acquire_timeout: Duration::from_secs(2),
            exec_timeout: Duration::from_secs(5),
            head_timeout: Duration::from_secs(5),
            max_lifetime: None,
            max_requests: None,
            idle_timeout: None,
            ping_path: None,
            replenish: Some(crate::config::ReplenishConfig::default()),
        },
        CancellationToken::new(),
    );

    let mut lease = pool.acquire().await.expect("lease");
    assert!(lease.is_fresh(), "dialled for this lease");
    assert!(!lease.connection().saw_response_byte(), "nothing said yet");

    lease.connection().begin_request();
    let mut empty: &[u8] = &[];
    let params =
        Params::default().request_method("GET").script_name("/index.php");
    let response = lease
        .connection()
        .client_mut()
        .execute(Request::new(params, &mut empty))
        .await
        .expect("first request");
    assert!(response.stdout.is_some());
    assert!(lease.connection().saw_response_byte(), "FPM answered this one");

    // The next request on the same connection starts from nothing,
    // which is what keeps a replay legal after a successful one.
    lease.connection().begin_request();
    assert!(
        !lease.connection().saw_response_byte(),
        "a served connection must not veto the next request's replay"
    );

    let _ = answered_tx.send(());
    let _ = fpm.await;
    let _ = std::fs::remove_file(&sock);
}

/// The sweep cadence is read from `fpm.yaml`, not compiled in and not
/// taken from `tasks.yaml`, so that it sits beside the `idle_timeout`
/// it has to stay under.
#[cfg(feature = "tasks")]
#[tokio::test]
async fn the_sweep_takes_its_cadence_from_the_pool_config() {
    use crate::config::HousekeepConfig;

    /// The cadence as Congif prepares it for the service.
    async fn read_housekeep(
        state: &AppState,
        svc: &PhpFcgiService,
    ) -> HousekeepConfig {
        let owner: &dyn claviron_runtime::congif::ConfigRegistration = svc;
        state.config_reader().prepare_owners(&[owner]).await.expect("prepare");
        state
            .config_reader()
            .snapshot::<PhpFcgiService>()
            .expect("snapshot")
            .housekeep
            .clone()
    }

    let dir = unique_path(".housekeep");
    std::fs::create_dir_all(&dir).expect("config dir");
    let state = AppState::standalone("fcgi-housekeep-test", &dir);
    let svc = PhpFcgiService::from_state(state.clone());

    // No file at all: the task still registers, on the default.
    assert_eq!(
        read_housekeep(&state, &svc).await.interval,
        Duration::from_secs(15)
    );

    std::fs::write(
        dir.join("fpm.yaml"),
        "housekeep: { interval: \"5s\" }\nfpm:\n  www:\n    unix: /run/php.sock\n"
    )
    .expect("write");
    let housekeep = read_housekeep(&state, &svc).await;
    assert_eq!(housekeep.interval, Duration::from_secs(5));
    assert!(housekeep.enabled);

    std::fs::write(
        dir.join("fpm.yaml"),
        "housekeep: { enabled: false }\nfpm:\n  www:\n    unix: /run/php.sock\n"
    )
    .expect("write");
    let housekeep = read_housekeep(&state, &svc).await;
    assert!(!housekeep.enabled);
    assert_eq!(
        housekeep.interval,
        Duration::from_secs(15),
        "disabled keeps the default"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// `connect_timeout` and `exec_timeout` belong to the FPM entry, not
/// to `pool:`. They used to be accepted there and silently dropped,
/// which left an operator reading their own timeouts out of a file
/// the server never applied.
#[test]
fn pool_block_rejects_budgets_that_belong_to_the_entry() {
    let parsed = serde_yaml::from_str::<FpmDocument>(
        "fpm:\n  www:\n    unix: /run/php.sock\n    \
         pool: { min_live: 1, max_idle: 2, max_live: 4, connect_timeout: \"100ms\" }\n",
    );
    let err = match parsed {
        Ok(_) => panic!("`pool.connect_timeout` must not parse"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("connect_timeout"), "{err}");

    serde_yaml::from_str::<FpmDocument>(
        "fpm:\n  www:\n    unix: /run/php.sock\n    connect_timeout: \"100ms\"\n    \
         exec_timeout: \"60s\"\n    \
         pool: { min_live: 1, max_idle: 2, max_live: 4 }\n"
    )
    .expect("entry-level budgets parse");
}
