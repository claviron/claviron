use super::*;
use crate::origin::OriginHealth;

#[test]
fn audit_health_stale_assignment_cannot_change_successor_verdict() {
    let table = LocalMembershipState {
        inner: Arc::new(Storage {
            parent: Weak::new(),
            first: ReservedOriginId(0),
            cells: vec![Cell {
                state: OriginSlot::empty(),
                stamp: AtomicU64::new(1),
                scopes: AtomicU64::new(0),
                owners: AtomicU32::new(0),
                assignment: Mutex::new(None),
            }]
            .into_boxed_slice(),
        }),
    };
    let old = SlotRef::Assignment(AssignmentRef {
        table: table.clone(),
        index: 0,
        stamp: 1,
    });
    old.record_failure_change(1).unwrap();
    assert!(old.reserve_validator_probe());
    let cell = &table.inner.cells[0];
    assert_eq!(cell.scopes.swap(CLOSED, Ordering::AcqRel), 0);
    assert!(old.record_success_change().is_none());
    cell.state.reset();
    cell.stamp.store(2, Ordering::Release);
    cell.scopes.store(0, Ordering::Release);
    let next = SlotRef::Assignment(AssignmentRef {
        table: table.clone(),
        index: 0,
        stamp: 2,
    });
    next.record_failure_change(1).unwrap();
    assert!(next.reserve_validator_probe());
    assert!(old.record_success_change().is_none());
    assert!(old.record_failure_change(1).is_none());
    assert!(!old.reserve_validator_probe());
    old.cancel_validator_probe();
    assert_eq!(next.health(), OriginHealth::HalfOpen);
    assert_eq!(cell.state.verdict.load().failures(), 1);
    next.record_success_change().unwrap();
    assert_eq!(next.health(), OriginHealth::Healthy);
    assert_eq!(cell.state.verdict.load().failures(), 0);
}
