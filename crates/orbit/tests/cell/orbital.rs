use super::*;

fn cells() -> Cells {
    Cells::new(Arc::new(orbitive::Fleet::join("managed-cell-test", 1).unwrap()))
        .unwrap()
}

// Exercise bindings directly, without booting a Claviron runtime or changing
// its process-default owner. Every test gets an independent in-memory Fleet.
fn declared<T: CellType>(initial: T) -> Orbital<T> {
    Orbital(Arc::new(Value::new(initial)))
}

#[test]
fn initial_value_needs_no_shared_infrastructure() {
    let value = declared(0_u64);
    assert_eq!(value.load().unwrap(), 0);
    assert!(matches!(value.store(1), Err(Error::NotPrepared)));
    assert!(matches!(value.id(), Err(Error::NotPrepared)));
}

#[test]
fn binding_preserves_the_initial_value_and_clones_share_a_slot() {
    let cells = cells();
    let value = declared(42_u64);
    let clone = value.clone();
    value.0.bind(&cells).unwrap();
    assert_eq!(clone.load().unwrap(), 42);
    assert_eq!(clone.fetch_add(1).unwrap(), 43);
    assert_eq!(value.load().unwrap(), 43);
    let id = value.id().unwrap();
    // Rebinding does not reset the value or allocate a second slot.
    value.0.bind(&cells).unwrap();
    assert_eq!(value.id().unwrap(), id);
    assert_eq!(value.load().unwrap(), 43);
}

#[test]
fn last_clone_releases_the_slot_but_an_earlier_drop_does_not() {
    let cells = cells();
    let value = declared(false);
    value.0.bind(&cells).unwrap();
    let id = value.id().unwrap();
    let clone = value.clone();
    drop(value);
    assert!(cells.is_live(id));
    clone.set().unwrap();
    assert!(clone.load().unwrap());
    drop(clone);
    assert!(!cells.is_live(id));
}

#[test]
fn retired_handles_and_overflow_do_not_fall_back_to_zero() {
    let cells = cells();
    let value = declared(u64::MAX);
    value.0.bind(&cells).unwrap();
    assert!(matches!(
        value.fetch_add(1),
        Err(Error::Value(cell::Error::Overflow))
    ));
    assert_eq!(value.load().unwrap(), u64::MAX);
    let clone = value.clone();
    value.release().unwrap();
    assert!(matches!(clone.load(), Err(Error::Value(cell::Error::Stale(_)))));
}
