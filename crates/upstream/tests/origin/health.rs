use super::*;
use std::sync::{Arc, Barrier};

fn state(slot: &OriginSlot) -> (OriginHealth, u32) {
    let word = slot.verdict.load();
    (OriginHealth::decode(word.health()), word.failures())
}
fn setup(
    slot: &OriginSlot,
    health: OriginHealth,
    failures: u32,
) {
    let before = slot.verdict.load();
    slot.verdict
        .compare_exchange(before, before.with_health(health as u8, failures))
        .unwrap();
}

#[test]
fn audit_health_serial_thresholds_half_open_and_saturation() {
    let slot = OriginSlot::empty();
    slot.record_origin_reachability_failure(1);
    slot.record_success();
    slot.record_origin_reachability_failure(1);
    assert_eq!(state(&slot), (OriginHealth::Down, 1));
    slot.record_origin_reachability_failure(1);
    slot.record_success();
    assert_eq!(state(&slot), (OriginHealth::Healthy, 0));
    for failures in 1..=3 {
        slot.record_origin_reachability_failure(3);
        assert_eq!(
            state(&slot),
            (
                if failures == 3 {
                    OriginHealth::Down
                } else {
                    OriginHealth::Healthy
                },
                failures
            )
        );
    }
    assert!(slot.reserve_validator_probe());
    slot.record_origin_reachability_failure(u32::MAX);
    assert_eq!(state(&slot), (OriginHealth::Down, 4));
    assert!(slot.reserve_validator_probe());
    slot.cancel_validator_probe();
    assert_eq!(state(&slot), (OriginHealth::Down, 4));
    assert!(slot.reserve_validator_probe());
    slot.record_success();
    slot.cancel_validator_probe();
    assert_eq!(state(&slot), (OriginHealth::Healthy, 0));
    setup(&slot, OriginHealth::Healthy, u32::MAX - 1);
    slot.record_origin_reachability_failure(u32::MAX);
    slot.record_origin_reachability_failure(u32::MAX);
    assert_eq!(state(&slot), (OriginHealth::Down, u32::MAX));
    slot.record_success();
    assert_eq!(state(&slot), (OriginHealth::Healthy, 0));
    let since = slot.healthy_since.load(Ordering::Acquire);
    slot.record_success();
    assert_eq!(slot.healthy_since.load(Ordering::Acquire), since);
}

#[test]
fn audit_health_concurrent_success_failure_matches_a_serial_order() {
    for (health, failures, threshold) in [
        (OriginHealth::Down, 1, 1),
        (OriginHealth::HalfOpen, 1, 1),
        (OriginHealth::Healthy, 0, 3),
        (OriginHealth::Down, 3, 3),
        (OriginHealth::HalfOpen, 1, 3),
        (OriginHealth::Down, u32::MAX, u32::MAX),
    ] {
        let control = OriginSlot::empty();
        setup(&control, health, failures);
        control.record_success();
        control.record_origin_reachability_failure(threshold);
        let success_first = state(&control);
        setup(&control, health, failures);
        control.record_origin_reachability_failure(threshold);
        control.record_success();
        let failure_first = state(&control);
        let slot = Arc::new(OriginSlot::empty());
        let barrier = Arc::new(Barrier::new(3));
        std::thread::scope(|scope| {
            for success in [true, false] {
                let slot = slot.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    for _ in 0..512 {
                        barrier.wait();
                        if success {
                            slot.record_success();
                        } else {
                            slot.record_origin_reachability_failure(threshold);
                        }
                        barrier.wait();
                        // Keep the quiescent observation separate from the next round.
                        barrier.wait();
                    }
                });
            }
            let mut invalid = None;
            for _ in 0..512 {
                setup(&slot, health, failures);
                barrier.wait();
                barrier.wait();
                let observed = state(&slot);
                if observed != success_first && observed != failure_first {
                    invalid = Some(observed);
                }
                barrier.wait();
            }
            assert!(
                invalid.is_none(),
                "nonserial state {invalid:?} from {health:?}/{failures}, threshold {threshold}"
            );
        });
    }
}

#[test]
fn audit_health_concurrent_counts_probe_reservation_and_serving_bits() {
    let slot = OriginSlot::empty();
    slot.set_outlier_ejected(true);
    slot.set_l7_serving(Serving::No);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for _ in 0..128 {
                    slot.record_origin_reachability_failure(1);
                }
            });
        }
    });
    assert_eq!(state(&slot), (OriginHealth::Down, 1024));
    let barrier = Barrier::new(8);
    let wins = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                barrier.wait();
                if slot.reserve_validator_probe() {
                    wins.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(wins.load(Ordering::Relaxed), 1);
    slot.record_success();
    assert_eq!(state(&slot), (OriginHealth::Healthy, 0));
    assert!(slot.outlier_ejected());
    assert_eq!(slot.serving(), Serving::No);
    // Concurrent flag and reachability writers must not overwrite each other.
    std::thread::scope(|scope| {
        scope.spawn(|| {
            for _ in 0..512 {
                slot.record_success();
                slot.record_origin_reachability_failure(1);
            }
        });
        scope.spawn(|| {
            for _ in 0..512 {
                slot.set_l7_serving(Serving::Yes);
                slot.set_outlier_ejected(false);
                slot.set_l7_serving(Serving::No);
                slot.set_outlier_ejected(true);
            }
        });
    });
    assert!(slot.outlier_ejected());
    assert_eq!(slot.serving(), Serving::No);
    assert_eq!(state(&slot), (OriginHealth::Down, 1));
}
