use super::*;

#[test]
fn audit_upstream_warm_h2_stream_admission_tracks_peer_changes() {
    let capacity = StreamCapacity::new(None, Arc::new(Notify::new()));
    assert!(!capacity.try_reserve());
    capacity.update_peer(2);
    assert_eq!(capacity.limit(), Some(2));
    assert!(capacity.try_reserve());
    assert!(capacity.try_reserve());
    assert!(!capacity.try_reserve());
    capacity.update_peer(1);
    assert!(!capacity.try_reserve());
    assert!(!capacity.release());
    assert!(!capacity.try_reserve());
    assert!(capacity.release());
    assert!(capacity.try_reserve());
    assert!(!capacity.seal_if_idle());
    capacity.seal();
    assert!(!capacity.try_reserve());
    assert!(capacity.release());
    assert!(capacity.seal_if_idle());
    let local = StreamCapacity::new(Some(1), Arc::new(Notify::new()));
    local.update_peer(8);
    assert_eq!(local.limit(), Some(1));
    assert!(local.try_reserve());
    assert!(!local.try_reserve());
    local.release();
}

#[test]
fn audit_upstream_warm_h2_idle_eviction_and_admission_are_atomic() {
    for _ in 0..64 {
        let capacity = StreamCapacity::new(Some(1), Arc::new(Notify::new()));
        capacity.update_peer(1);
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let reservation = scope.spawn(|| {
                barrier.wait();
                capacity.try_reserve()
            });
            barrier.wait();
            let evicted = capacity.seal_if_idle();
            let reserved = reservation.join().unwrap();
            assert_ne!(evicted, reserved);
            if reserved {
                assert_eq!(capacity.used(), 1);
                capacity.release();
            }
        });
    }
}
