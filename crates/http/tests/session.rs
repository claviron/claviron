use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use claviron_http::session::{SessionOwner, SessionSpawnError};
use claviron_runtime::gate::{Gate, GateDrainOutcome};
use tokio::sync::oneshot;

const TIMEOUT: Duration = Duration::from_secs(5);

struct Released(Arc<AtomicUsize>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn session_retains_capacity_after_response_and_indefinite_grace_does_not_kill()
 {
    let owner = SessionOwner::new("test".parse().unwrap());
    let gate = Gate::new(Some(1), TIMEOUT);
    let connection = owner.bind(gate.try_enter().unwrap());
    let sessions = connection.sessions();
    let signal = sessions.clone();
    let (notified, notification) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = Released(dropped.clone());
    sessions
        .spawn(async move {
            let _guard = guard;
            signal.wait_graceful_shutdown().await;
            notified.send(()).unwrap();
            let _ = released.await;
        })
        .unwrap();
    drop(connection);
    assert_eq!(gate.count(), 1, "101 must not release connection capacity");
    assert!(gate.try_enter().is_err());
    let drain = owner.close();
    gate.graceful_shutdown(None);
    tokio::time::timeout(TIMEOUT, notification).await.unwrap().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), gate.wait_all_done())
            .await
            .is_err()
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(
        sessions.spawn(async {}),
        Err(SessionSpawnError::ListenerClosed)
    );
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, gate.wait_all_done()).await.unwrap(),
        GateDrainOutcome::Drained
    );
    tokio::time::timeout(TIMEOUT, drain.join()).await.unwrap();
    assert_eq!(gate.count(), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn explicit_force_joins_cleanup_and_a_new_generation_cannot_use_old_capabilities()
 {
    let owner = SessionOwner::new("test".parse().unwrap());
    let gate = Gate::new(Some(1), TIMEOUT);
    let connection = owner.bind(gate.try_enter().unwrap());
    let sessions = connection.sessions();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = Released(dropped.clone());
    sessions
        .spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        })
        .unwrap();
    drop(connection);
    let mut drain = owner.close();
    gate.graceful_shutdown(Some(Duration::ZERO));
    assert!(matches!(
        gate.wait_all_done().await,
        GateDrainOutcome::Forced { remaining: 1 }
    ));
    drain.abort_all();
    tokio::time::timeout(TIMEOUT, drain.join()).await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(gate.count(), 0);

    let next = SessionOwner::new("test".parse().unwrap());
    let next_gate = Gate::new(Some(1), TIMEOUT);
    let connection = next.bind(next_gate.try_enter().unwrap());
    assert_eq!(
        sessions.spawn(async {}),
        Err(SessionSpawnError::ListenerClosed)
    );
    connection.sessions().spawn(async {}).unwrap();
    drop(connection);
    next.close().join().await;
    assert_eq!(next_gate.count(), 0);
}

#[tokio::test]
async fn unused_capability_retains_no_capacity_and_drain_rejects_new_work() {
    let owner = SessionOwner::new("test".parse().unwrap());
    let gate = Gate::new(None, TIMEOUT);
    let connection = owner.bind(gate.try_enter().unwrap());
    let sessions = connection.sessions();
    drop(connection);
    assert_eq!(gate.count(), 0);
    assert_eq!(
        sessions.spawn(async {}),
        Err(SessionSpawnError::ConnectionClosed)
    );
    let connection = owner.bind(gate.try_enter().unwrap());
    gate.graceful_shutdown(None);
    assert_eq!(
        connection.sessions().spawn(async {}),
        Err(SessionSpawnError::Draining)
    );
    drop(connection);
    owner.close().join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_close_joins_every_accepted_submission() {
    let owner = SessionOwner::new("test".parse().unwrap());
    let gate = Gate::new(None, TIMEOUT);
    let connection = owner.bind(gate.try_enter().unwrap());
    let sessions = connection.sessions();
    let dropped = Arc::new(AtomicUsize::new(0));
    let observed = dropped.clone();
    let submissions = tokio::spawn(async move {
        for _ in 0..128 {
            let guard = Released(observed.clone());
            let _ = sessions.spawn(async move {
                drop(guard);
            });
            tokio::task::yield_now().await;
        }
    });
    tokio::task::yield_now().await;
    let drain = owner.close();
    submissions.await.unwrap();
    drop(connection);
    tokio::time::timeout(TIMEOUT, drain.join()).await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 128);
    assert_eq!(gate.count(), 0);
}
