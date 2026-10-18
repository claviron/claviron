use claviron_orbit::OrbitEpoch;
use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
};
use claviron_sys::process::ProcessIdentity;

use super::{
    MasterIdentity, OperationId, PresencePhase, WORKER_IDENTITY_BYTES,
    WorkerIdentity, WorkerIdentityProvider,
};

#[test]
fn worker_identity_round_trips_and_names_the_differing_field() {
    let master = MasterIdentity::new(ProcessIdentity::new(100, 5_000), 3);
    let me = WorkerIdentity::new(
        WorkerId::new(2),
        ProcessIdentity::new(200, 7_000),
        4,
        master,
    );
    let mut bytes = Vec::new();
    me.encode_into(&mut bytes);
    assert_eq!(bytes.len(), WORKER_IDENTITY_BYTES);
    assert_eq!(
        WorkerIdentity::decode(bytes[..].try_into().unwrap()).unwrap(),
        me
    );

    assert_eq!(me.mismatch(me), None);
    let with = |worker_id, pid, stamp, incarnation, master| {
        WorkerIdentity::new(
            WorkerId::new(worker_id),
            ProcessIdentity::new(pid, stamp),
            incarnation,
            master,
        )
    };
    assert_eq!(me.mismatch(with(3, 200, 7_000, 4, master)), Some("worker id"));
    assert_eq!(me.mismatch(with(2, 201, 7_000, 4, master)), Some("pid"));
    assert_eq!(
        me.mismatch(with(2, 200, 7_001, 4, master)),
        Some("process start stamp")
    );
    assert_eq!(
        me.mismatch(with(2, 200, 7_000, 5, master)),
        Some("incarnation")
    );
    let other_master = MasterIdentity::new(ProcessIdentity::new(100, 5_001), 3);
    assert_eq!(
        me.mismatch(with(2, 200, 7_000, 4, other_master)),
        Some("master process")
    );
    let later_master = MasterIdentity::new(ProcessIdentity::new(100, 5_000), 4);
    assert_eq!(
        me.mismatch(with(2, 200, 7_000, 4, later_master)),
        Some("master generation")
    );

    let mut zero_incarnation = bytes.clone();
    zero_incarnation[13..21].fill(0);
    assert!(
        WorkerIdentity::decode(zero_incarnation[..].try_into().unwrap())
            .is_err()
    );
}

#[test]
fn presence_phase_changes_without_resetting_runtime_start() {
    let state = AppState::initialize(
        RunMode::Worker(WorkerId::new(2)),
        FleetNodeId::new(2),
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("presence-phase-test".into()),
        ".",
    );
    let process = ProcessIdentity::new(200, 7_000);
    let identity = WorkerIdentity::new(
        WorkerId::new(2),
        process,
        4,
        MasterIdentity::new(ProcessIdentity::new(100, 5_000), 3),
    );
    let provider = WorkerIdentityProvider::new(identity, 1_757_780_000_123);

    let booting = provider.presence(&state, OrbitEpoch::from_unix_ms(10));
    assert_eq!(booting.phase(), PresencePhase::Booting);
    assert_eq!(booting.runtime_started_at_unix_ms(), 1_757_780_000_123);

    provider.mark_started();
    let running = provider.presence(&state, OrbitEpoch::from_unix_ms(20));
    assert_eq!(running.phase(), PresencePhase::Running);
    assert_eq!(
        running.runtime_started_at_unix_ms(),
        booting.runtime_started_at_unix_ms()
    );

    let operation = OperationId::from_raw(7).unwrap();
    assert_eq!(provider.accept_drain(operation), operation);
    state.initiate_shutdown();
    let draining = provider.presence(&state, OrbitEpoch::from_unix_ms(30));
    assert_eq!(draining.phase(), PresencePhase::Draining);
    assert_eq!(draining.active_operation_id(), Some(7));
    assert_eq!(
        draining.runtime_started_at_unix_ms(),
        booting.runtime_started_at_unix_ms()
    );
}
