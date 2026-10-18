use claviron_runtime::registry::Component;
use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, ServiceStartPolicy,
    WorkerCount, WorkerId,
};

use super::*;

#[test]
fn standalone_registration_is_idempotent_without_orbit_transport() {
    let state = AppState::standalone("application-fleet-test", ".");
    crate::register(&state).unwrap();

    register(&state).unwrap();
    register(&state).unwrap();

    assert!(
        state
            .registry_ref()
            .resolve::<ApplicationFleetControlRegistration>()
            .is_some()
    );
    assert!(
        state
            .registry_ref()
            .resolve::<publisher::ApplicationSnapshotPublisher>()
            .is_none()
    );
}

#[test]
fn snapshot_provider_is_registered_on_both_roles_but_only_workers_are_runnable()
{
    for mode in [RunMode::Master, RunMode::Worker(WorkerId::new(1))] {
        let state = AppState::initialize(
            mode,
            FleetNodeId::new(if matches!(mode, RunMode::Worker(_)) {
                1
            } else {
                0
            }),
            WorkerCount::new(1),
            FleetCapacity::new(2).unwrap(),
            Some("application-registration-test".into()),
            ".",
        );
        crate::register(&state).unwrap();
        // Registration only: no live fleet or SHM is opened by this test.
        let events = state
            .registry_ref()
            .resolve::<claviron_orbit::FleetEventRegistry>()
            .unwrap();
        let policy = ApplicationSnapshotPolicy::new(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(10),
        );
        register_with_policy(&state, policy).unwrap();
        assert!(Arc::ptr_eq(
            &events,
            &state
                .registry_ref()
                .resolve::<claviron_orbit::FleetEventRegistry>()
                .unwrap()
        ));
        let provider = state
            .registry_ref()
            .resolve::<publisher::ApplicationSnapshotPublisher>()
            .unwrap();
        assert_eq!(
            provider.policy(),
            policy,
            "policy remains available on both roles"
        );
        let runnable = provider.as_runnable();
        assert_eq!(runnable.is_some(), state.is_worker());
        if let Some(runnable) = runnable {
            assert_eq!(
                runnable.start_policy(&state),
                ServiceStartPolicy::Automatic
            );
        }
    }
}

#[test]
fn supervised_registration_uses_the_automatically_registered_orbit_events() {
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("application-fleet-test".into()),
        ".",
    );
    assert!(
        state
            .registry_ref()
            .resolve::<claviron_orbit::FleetEventRegistry>()
            .is_none()
    );
    crate::register(&state).unwrap();
    let events = state
        .registry_ref()
        .resolve::<claviron_orbit::FleetEventRegistry>()
        .unwrap();

    register(&state).unwrap();
    register(&state).unwrap();

    assert!(Arc::ptr_eq(
        &events,
        &state
            .registry_ref()
            .resolve::<claviron_orbit::FleetEventRegistry>()
            .unwrap()
    ));
    assert!(
        state
            .registry_ref()
            .resolve::<ApplicationFleetControlRegistration>()
            .is_some()
    );
}
