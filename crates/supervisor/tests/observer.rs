use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use crate::heartbeat::publish_heartbeat;

struct TestFleet(Arc<Fleet>);

impl TestFleet {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let pid = std::process::id() & 0xffff;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xff;
        let name = Box::leak(
            format!("o{pid:04x}{nonce:08x}{counter:02x}").into_boxed_str(),
        );
        let fleet =
            Arc::new(Fleet::join_shm_as(name, 3, NodeId::ZERO).unwrap());
        fleet.reset_ring::<HeartbeatRecord>().unwrap();
        Self(fleet)
    }
}

impl Drop for TestFleet {
    fn drop(&mut self) {
        self.0.shm_ring::<HeartbeatRecord>().unwrap().unlink().unwrap();
    }
}

#[test]
fn observer_has_master_scope_but_no_independent_runnable() {
    let observer = Arc::new(HeartbeatObserver::new());
    assert_eq!(observer.as_scoped().unwrap().scope(), Scope::Master);
    assert!(observer.fleet.get().is_none());
    assert!(observer.as_runnable().is_none());
}

#[tokio::test]
async fn boot_confirmation_starts_grace_without_fabricating_a_heartbeat() {
    let observer = HeartbeatObserver::new();
    observer.track_worker(WorkerId::new(1), 42).await;
    let timeout = Duration::from_secs(10);
    assert!(observer.stale_workers(timeout).await.is_empty());
    let snapshot = observer.snapshot().await;
    assert_eq!(snapshot[&WorkerId::new(1)].beats, 0);
    assert!(snapshot[&WorkerId::new(1)].first_heartbeat.is_none());

    observer
        .workers
        .write()
        .await
        .get_mut(&WorkerId::new(1))
        .unwrap()
        .last_heartbeat = Instant::now() - Duration::from_secs(11);
    assert_eq!(observer.stale_workers(timeout).await, [(WorkerId::new(1), 42)]);
}

#[tokio::test]
async fn observation_rejects_old_duplicate_stale_and_untracked_records() {
    let fleet = TestFleet::new();
    let worker = Fleet::join_shm_as(fleet.0.name(), 3, NodeId::new(1)).unwrap();
    let untracked =
        Fleet::join_shm_as(fleet.0.name(), 3, NodeId::new(2)).unwrap();
    let observer = HeartbeatObserver::new();
    observer.attach_fleet(fleet.0.clone()).unwrap();
    let timeout = Duration::from_secs(10);

    publish_heartbeat(&worker, OrbitEpoch::now());
    observer.track_worker(WorkerId::new(1), 42).await;
    assert!(observer.stale_workers(timeout).await.is_empty());
    assert_eq!(observer.snapshot().await[&WorkerId::new(1)].beats, 0);

    publish_heartbeat(&worker, OrbitEpoch::now());
    publish_heartbeat(&fleet.0, OrbitEpoch::now());
    publish_heartbeat(&untracked, OrbitEpoch::now());
    assert!(observer.stale_workers(timeout).await.is_empty());
    let snapshot = observer.snapshot().await;
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[&WorkerId::new(1)].beats, 1);
    assert!(snapshot[&WorkerId::new(1)].first_heartbeat.is_some());

    assert!(observer.stale_workers(timeout).await.is_empty());
    assert_eq!(observer.snapshot().await[&WorkerId::new(1)].beats, 1);
    assert_eq!(
        observer.snapshot().await[&WorkerId::new(1)].last_heartbeat,
        snapshot[&WorkerId::new(1)].last_heartbeat
    );

    publish_heartbeat(
        &worker,
        OrbitEpoch::from_unix_ms(OrbitEpoch::now().as_unix_ms() - 11_000),
    );
    observer
        .workers
        .write()
        .await
        .get_mut(&WorkerId::new(1))
        .unwrap()
        .last_heartbeat = Instant::now() - Duration::from_secs(11);
    assert_eq!(observer.stale_workers(timeout).await, [(WorkerId::new(1), 42)]);
    assert_eq!(observer.snapshot().await[&WorkerId::new(1)].beats, 1);

    // A current lane record is not evidence for the replacement until it emits
    // another heartbeat after the replacement's own validated boot boundary.
    publish_heartbeat(&worker, OrbitEpoch::now());
    observer.track_worker(WorkerId::new(1), 43).await;
    assert!(observer.stale_workers(timeout).await.is_empty());
    assert_eq!(observer.snapshot().await[&WorkerId::new(1)].beats, 0);
    assert!(!observer.complete_worker_shutdown(WorkerId::new(1), 42).await);
    assert_eq!(observer.snapshot().await[&WorkerId::new(1)].pid, 43);

    publish_heartbeat(&worker, OrbitEpoch::now());
    assert!(observer.stale_workers(timeout).await.is_empty());
    assert_eq!(observer.snapshot().await[&WorkerId::new(1)].beats, 1);
    assert!(observer.complete_worker_shutdown(WorkerId::new(1), 43).await);
    assert!(!observer.complete_worker_shutdown(WorkerId::new(1), 43).await);
    publish_heartbeat(&worker, OrbitEpoch::now());
    assert!(observer.stale_workers(timeout).await.is_empty());
    assert!(observer.snapshot().await.is_empty());
}
