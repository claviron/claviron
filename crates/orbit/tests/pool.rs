use claviron_runtime::{FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId};

use super::*;

fn worker() -> AppState {
    AppState::initialize(
        RunMode::Worker(WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("pool-incarnation".into()),
        "."
    )
}

#[test]
fn a_worker_is_stamped_with_the_incarnation_the_master_handed_it() {
    let state = worker();
    state.registry_ref().insert(Arc::new(FleetIncarnation::new(7))).unwrap();

    assert_eq!(incarnation(&state).unwrap(), Incarnation::new(7));
}

#[test]
fn a_worker_without_one_is_refused_rather_than_stamped_with_a_default() {
    // Stamping it `FIRST` would make every replacement of this slot look
    // like the original, and a death report naming the real incarnation
    // would match nothing and leak the units.
    let error = incarnation(&worker()).expect_err("a worker must carry its incarnation");

    assert!(error.to_string().contains("FleetIncarnation"), "{error}");
}

#[test]
fn reporting_a_death_where_no_table_was_opened_does_nothing() {
    // Lifecycle code reports unconditionally; a process that opened no
    // pool must not need to know that before it can say a worker is gone.
    report_node_dead(&AppState::standalone("pool-deaths-absent", "."), 3, 9);
}

#[test]
fn a_primary_needs_none_because_it_registers_no_pooled_resource() {
    let state = AppState::standalone("pool-incarnation-primary", ".");

    assert_eq!(incarnation(&state).unwrap(), Incarnation::new(1));
}
