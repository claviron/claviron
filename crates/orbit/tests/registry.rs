use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount,
};

use super::{register, register_fleet};
use crate::{FleetEventRegistry, FleetProvider};

#[test]
fn standalone_registers_fleet_without_cross_process_event_bridge() {
    let state = AppState::standalone("orbit-test", ".");

    register(&state).expect("Orbit registration should succeed");

    assert!(state.registry_ref().resolve::<FleetProvider>().is_some());
    assert!(state.registry_ref().resolve::<FleetEventRegistry>().is_none());
}

#[test]
fn supervised_mode_registers_cross_process_event_bridge() {
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("orbit-test".into()),
        ".",
    );

    register(&state).expect("Orbit registration should succeed");

    assert!(state.registry_ref().resolve::<FleetEventRegistry>().is_some());
}

#[test]
fn fleet_only_registration_does_not_install_event_bridge() {
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("orbit-test".into()),
        ".",
    );

    register_fleet(&state).expect("Orbit fleet registration should succeed");

    assert!(state.registry_ref().resolve::<FleetProvider>().is_some());
    assert!(state.registry_ref().resolve::<FleetEventRegistry>().is_none());
}
