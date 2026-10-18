use claviron_runtime::registry::Component;
use claviron_runtime::{
    FleetCapacity, FleetNodeId, RunMode, ServiceManager, WorkerCount,
};

use super::*;

#[test]
fn standalone_registers_local_service_commands_without_snapshot_transport() {
    let state = AppState::standalone("service-management-test", ".");
    register(&state).unwrap();
    register(&state).unwrap();

    assert!(
        state.registry_ref().resolve::<ServiceSnapshotPublisher>().is_none()
    );
    assert!(
        state.registry_ref().resolve::<ServiceControlRegistration>().is_some()
    );
}

#[test]
fn supervised_registration_requires_the_event_bridge() {
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("service-management-test".into()),
        ".",
    );
    assert!(register(&state).is_err());
}

#[test]
fn typed_registration_precedes_manager_and_preserves_the_first_policy() {
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("service-management-test".into()),
        ".",
    );
    crate::register(&state).unwrap();
    let policy = ServiceSnapshotPolicy::new(
        std::time::Duration::from_secs(2),
        std::time::Duration::from_secs(9),
    );
    register_with_policy(&state, policy).unwrap();
    let publisher =
        state.registry_ref().resolve::<ServiceSnapshotPublisher>().unwrap();

    <(ServiceSnapshotPublisher,) as Register<AppState>>::register(&state)
        .unwrap();
    register(&state).unwrap();

    assert!(state.registry_ref().resolve::<ServiceManager>().is_none());
    assert!(Arc::ptr_eq(
        &publisher,
        &state.registry_ref().resolve::<ServiceSnapshotPublisher>().unwrap(),
    ));
    assert_eq!(publisher.policy(), policy);
    assert!(publisher.as_configurable().is_some());
}
