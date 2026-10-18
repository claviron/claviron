use super::*;

#[test]
fn fleet_capacity_accepts_all_worker_ids() {
    let capacity = FleetCapacity::new(256).unwrap();

    assert_eq!(capacity.worker_capacity(), 255);
    assert!(capacity.admits(WorkerCount::new(255)));
    assert!(FleetCapacity::new(0).is_none());
    assert!(FleetCapacity::new(257).is_none());
}

#[test]
fn only_master_can_change_desired_worker_count_within_capacity() {
    let master = State::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(4).unwrap(),
        Some("runtime-test".into()),
        ".",
    );
    master.set_worker_count(WorkerCount::new(3)).unwrap();
    assert_eq!(master.worker_count(), 3);
    assert!(master.set_worker_count(WorkerCount::new(4)).is_err());
    assert_eq!(master.worker_count(), 3);

    let worker = State::initialize(
        RunMode::Worker(WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(2),
        FleetCapacity::new(4).unwrap(),
        Some("runtime-test".into()),
        ".",
    );
    assert!(worker.set_worker_count(WorkerCount::new(1)).is_err());
    assert_eq!(worker.worker_count(), 2);
}

#[test]
fn explicit_boot_hostname_is_preserved() {
    let state = State::initialize_with_hostname(
        RunMode::Standalone,
        FleetNodeId::ZERO,
        WorkerCount::ZERO,
        FleetCapacity::ONE,
        Some("runtime-test".into()),
        "xd01",
        ".",
    );

    assert_eq!(state.hostname(), "xd01");
}

#[test]
fn fleet_name_is_independent_from_instance_name() {
    let edge = State::standalone_in_fleet("edge", "clv", ".");
    let medusa = State::standalone_in_fleet("medusa", "clv", ".");
    let isolated = State::standalone_in_fleet("medusa", "python-dev", ".");

    assert_eq!(edge.fleet_name(), "clv");
    assert_eq!(medusa.fleet_name(), "clv");
    assert_eq!(isolated.fleet_name(), "python-dev");
    assert_ne!(edge.name(), medusa.name());
}
