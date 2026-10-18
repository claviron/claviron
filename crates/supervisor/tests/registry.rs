use claviron_orbit::FleetProvider;
use claviron_orbit::service::FleetMembership;
use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
};

use super::register;
use crate::WorkerControl;
use crate::heartbeat::HeartbeatProvider;
use crate::observer::HeartbeatObserver;

#[test]
fn standalone_registers_no_supervisor_capabilities() {
    let state = AppState::standalone("supervisor-test", ".");

    register(&state).expect("register standalone");

    assert!(state.registry_ref().resolve::<FleetProvider>().is_none());
    assert!(state.registry_ref().resolve::<HeartbeatObserver>().is_none());
    assert!(state.registry_ref().resolve::<HeartbeatProvider>().is_none());
    assert!(state.registry_ref().resolve::<WorkerControl>().is_none());
    assert!(
        state.registry_ref().resolve_dyn::<dyn FleetMembership>().is_none()
    );
    assert_eq!(state.events().registered_type_count(), 0);
}

#[test]
fn supervised_event_registration_requires_orbit_first() {
    for (mode, node_id) in [
        (RunMode::Master, FleetNodeId::ZERO),
        (RunMode::Worker(WorkerId::new(1)), FleetNodeId::new(1)),
    ] {
        let state = AppState::initialize(
            mode,
            node_id,
            WorkerCount::new(1),
            FleetCapacity::new(2).unwrap(),
            Some("registration-test".into()),
            ".",
        );
        let error = register(&state).unwrap_err();
        assert!(
            error.to_string().contains("FleetEventRegistry is unavailable")
        );
        assert_eq!(state.events().registered_type_count(), 0);
    }
}
