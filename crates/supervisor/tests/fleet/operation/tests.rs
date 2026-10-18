use super::*;

#[test]
fn a_slot_has_one_owner_and_a_fleet_operation_owns_every_slot() {
    let table = OperationTable::default();
    let one = WorkerId::new(1);
    let two = WorkerId::new(2);

    let stop = table
        .try_acquire(
            OperationScope::Slot(one),
            OperationKind::Stop,
            OperationOrigin::Supervisor,
        )
        .unwrap();
    assert_eq!(
        table
            .try_acquire(
                OperationScope::Slot(one),
                OperationKind::Stop,
                OperationOrigin::Supervisor
            )
            .unwrap_err(),
        Refusal::AlreadyInProgress(stop.operation())
    );
    assert_eq!(
        table
            .try_acquire(
                OperationScope::Slot(one),
                OperationKind::Restart,
                OperationOrigin::Supervisor
            )
            .unwrap_err(),
        Refusal::Conflict(stop.operation())
    );
    assert_eq!(
        table
            .try_acquire(
                OperationScope::Fleet,
                OperationKind::RestartAll,
                OperationOrigin::Signal
            )
            .unwrap_err(),
        Refusal::Conflict(stop.operation())
    );
    let other = table
        .try_acquire(
            OperationScope::Slot(two),
            OperationKind::Reload,
            OperationOrigin::Supervisor,
        )
        .unwrap();
    assert_eq!(table.active().len(), 2);

    drop(stop);
    drop(other);
    assert!(table.active().is_empty(), "dropping the guard releases the scope");
    let fleet = table
        .try_acquire(
            OperationScope::Fleet,
            OperationKind::RestartAll,
            OperationOrigin::Signal,
        )
        .unwrap();
    assert_eq!(
        table
            .try_acquire(
                OperationScope::Slot(two),
                OperationKind::Start,
                OperationOrigin::Supervisor
            )
            .unwrap_err(),
        Refusal::Conflict(fleet.operation())
    );
    // Only acceptances take a number: stop, reload, then this.
    assert_eq!(fleet.operation().id, OperationId(3));
    assert_eq!(fleet.operation().origin, OperationOrigin::Signal);
}

#[test]
fn command_keys_name_their_submitter_and_never_repeat_in_one_process() {
    let first = CommandKey::next(Some(WorkerId::new(2)));
    let second = CommandKey::next(Some(WorkerId::new(2)));
    let master = CommandKey::next(None);
    assert_ne!(first, second);
    assert_eq!(first.raw() >> 56, 2);
    assert_eq!(master.raw() >> 56, 0);
    assert_ne!(master.raw(), 0, "a key is never the wire's 'no key'");
    assert_eq!(CommandKey::from_raw(first.raw()), Some(first));
    assert_eq!(CommandKey::from_raw(0), None);
}
