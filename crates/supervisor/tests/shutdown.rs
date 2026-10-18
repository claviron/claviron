use std::sync::Arc;
use std::time::Duration;

use claviron_orbit::EVENT_PAYLOAD_MAX;
use claviron_runtime::registry::Runnable;
use claviron_runtime::{
    EventDispatcher, FleetCapacity, FleetNodeId, RunContext, WorkerCount,
    WorkerId,
};
use claviron_sys::process::ProcessIdentity;
use tokio::sync::broadcast;

use super::*;
use crate::lifecycle::MasterIdentity;

fn worker_state() -> AppState {
    AppState::initialize(
        RunMode::Worker(WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("shutdown-test".into()),
        ".",
    )
}

fn operation(raw: u64) -> OperationId {
    OperationId::from_raw(raw).unwrap()
}

fn some_identity() -> WorkerIdentity {
    WorkerIdentity::new(
        WorkerId::new(2),
        ProcessIdentity::new(0x1234_5678, 0x0102_0304_0506_0708),
        3,
        MasterIdentity::new(ProcessIdentity::new(99, 0x0a0b_0c0d_0e0f_1011), 5),
    )
}

#[test]
fn drain_v2_codecs_round_trip() {
    let request = WorkerDrainRequested {
        operation: operation(7),
        target: some_identity(),
        reason: DrainReason::RollingRestart,
    };
    let payload = request.encode_orbit();
    assert_eq!(WorkerDrainRequested::decode_orbit(&payload).unwrap(), request);
    assert!(
        12 + WorkerDrainRequested::TOPIC.len() + payload.len()
            <= EVENT_PAYLOAD_MAX
    );
    let mut no_operation = payload.clone();
    no_operation[..8].fill(0);
    assert!(
        WorkerDrainRequested::decode_orbit(&no_operation).is_err(),
        "a request needs an operation"
    );
    let mut unknown_reason = payload.clone();
    *unknown_reason.last_mut().unwrap() = 99;
    assert!(WorkerDrainRequested::decode_orbit(&unknown_reason).is_err());
    assert!(WorkerDrainRequested::decode_orbit(&payload[1..]).is_err());

    let acknowledged = WorkerDrainAcknowledged {
        operation: operation(7),
        worker: some_identity(),
    };
    let payload = acknowledged.encode_orbit();
    assert_eq!(
        WorkerDrainAcknowledged::decode_orbit(&payload).unwrap(),
        acknowledged
    );
    assert!(
        12 + WorkerDrainAcknowledged::TOPIC.len() + payload.len()
            <= EVENT_PAYLOAD_MAX
    );

    for (operation, outcome) in [
        (Some(operation(7)), TeardownOutcome::Completed),
        (None, TeardownOutcome::FinalizersFailed { count: 2 }),
    ] {
        let completed = WorkerTeardownCompleted {
            operation,
            worker: some_identity(),
            outcome,
        };
        let payload = completed.encode_orbit();
        assert_eq!(
            WorkerTeardownCompleted::decode_orbit(&payload).unwrap(),
            completed
        );
        assert!(
            12 + WorkerTeardownCompleted::TOPIC.len() + payload.len()
                <= EVENT_PAYLOAD_MAX
        );
    }
}

#[test]
fn teardown_publication_requires_identity_shutdown_and_a_booted_bus() {
    let state = worker_state();
    assert!(
        publish_worker_teardown_completed(&state, 0)
            .unwrap_err()
            .to_string()
            .contains("not been initiated")
    );
    state.initiate_shutdown();
    assert!(
        publish_worker_teardown_completed(&state, 0)
            .unwrap_err()
            .to_string()
            .contains("WorkerIdentityProvider")
    );
    state
        .registry_ref()
        .insert(Arc::new(WorkerIdentityProvider::for_tests(WorkerId::new(1))))
        .expect("register identity");
    assert!(
        publish_worker_teardown_completed(&state, 0)
            .unwrap_err()
            .to_string()
            .contains("FleetEventRegistry is unavailable")
    );
    state
        .registry_ref()
        .register(Arc::new(FleetEventRegistry::new()))
        .expect("register FleetEventRegistry");
    assert!(
        publish_worker_teardown_completed(&state, 0)
            .unwrap_err()
            .to_string()
            .contains("not booted")
    );

    for state in [
        AppState::standalone("shutdown-test", "."),
        AppState::initialize(
            RunMode::Master,
            FleetNodeId::ZERO,
            WorkerCount::new(2),
            FleetCapacity::new(3).unwrap(),
            Some("shutdown-test".into()),
            ".",
        ),
    ] {
        state.initiate_shutdown();
        assert!(
            publish_worker_teardown_completed(&state, 0)
                .unwrap_err()
                .to_string()
                .contains("worker mode")
        );
    }
}

fn worker_with_identity() -> (AppState, Arc<WorkerIdentityProvider>) {
    let state = worker_state();
    let me = Arc::new(WorkerIdentityProvider::for_tests(WorkerId::new(1)));
    state.registry_ref().insert(me.clone()).expect("register identity");
    state
        .registry_ref()
        .register(Arc::new(FleetEventRegistry::new()))
        .expect("register FleetEventRegistry");
    install_events(&state).unwrap();
    (state, me)
}

#[tokio::test]
async fn cancellation_alone_emits_no_completion() {
    let (state, _) = worker_with_identity();
    let mut completed = state.events().subscribe::<WorkerTeardownCompleted>();
    state.initiate_shutdown();
    tokio::task::yield_now().await;
    assert!(matches!(
        completed.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
}

/// A request that names anything but this exact process — another slot, a
/// reused PID, a stamp, an incarnation, another master — is not for this
/// worker. The one that names it is acknowledged, and the acknowledgement
/// carries the operation the worker acted on; a later operation cannot take
/// the teardown over.
#[tokio::test]
async fn targeted_drain_request_is_acknowledged_by_its_exact_worker_only() {
    let (state, me) = worker_with_identity();
    let identity = me.identity();
    let mut acknowledged =
        state.events().subscribe::<WorkerDrainAcknowledged>();
    let dispatcher = tokio::spawn(
        state.registry_ref().resolve::<EventDispatcher>().unwrap().run(
            (),
            state.clone(),
            RunContext::new(state.shutdown_token()),
        ),
    );
    let request = |target| WorkerDrainRequested {
        operation: operation(7),
        target,
        reason: DrainReason::ManualStop,
    };
    let wrong = [
        WorkerIdentity::new(
            WorkerId::new(2),
            identity.process(),
            1,
            identity.master(),
        ),
        WorkerIdentity::new(
            identity.worker_id(),
            ProcessIdentity::new(
                identity.process().pid(),
                identity.process().started_at() ^ 1,
            ),
            1,
            identity.master(),
        ),
        WorkerIdentity::new(
            identity.worker_id(),
            identity.process(),
            2,
            identity.master(),
        ),
        WorkerIdentity::new(
            identity.worker_id(),
            identity.process(),
            1,
            MasterIdentity::new(
                identity.master().process(),
                identity.master().generation() + 1,
            ),
        ),
    ];
    for target in wrong {
        state.events().emit(request(target));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !state.is_shutting_down(),
        "a request for another identity must not drain this worker"
    );
    assert!(matches!(
        acknowledged.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));

    state.events().emit(request(identity));
    tokio::time::timeout(Duration::from_secs(1), state.on_shutdown())
        .await
        .unwrap();
    let ack = tokio::time::timeout(Duration::from_secs(1), acknowledged.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ack,
        WorkerDrainAcknowledged { operation: operation(7), worker: identity }
    );
    assert_eq!(me.accepted_drain(), Some(operation(7)));

    // Another operation asking later does not take the teardown over.
    state.events().emit(WorkerDrainRequested {
        operation: operation(8),
        target: identity,
        reason: DrainReason::Rotation,
    });
    tokio::time::timeout(Duration::from_secs(1), dispatcher)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(me.accepted_drain(), Some(operation(7)));
}

/// Full shutdown is the same protocol as every other drain: one request per
/// live identity under the fleet shutdown operation, none for a slot that has
/// no process, and `WorkersExited` only once every one of them is gone.
#[tokio::test]
async fn full_shutdown_publishes_one_targeted_request_per_identity() {
    use std::process::Command;

    use crate::config::SupervisorConfig;
    use crate::control::WorkerProcess;
    use crate::lifecycle::{
        ExitOutcome, OperationKind, OwnedProcess, WorkerIntent, WorkerPhase,
    };

    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(3),
        FleetCapacity::new(4).unwrap(),
        Some("shutdown-test".into()),
        ".",
    );
    let observer = Arc::new(HeartbeatObserver::new());
    let supervisor = Arc::new(WorkerControl::new(observer.clone()));
    let config: SupervisorConfig =
        serde_yaml::from_str("shutdown: { timeout: null }").unwrap();
    supervisor.config.store(Some(Arc::new(config)));
    let master = supervisor.establish_master_for_tests();
    state
        .registry_ref()
        .register(observer)
        .expect("register HeartbeatObserver");
    state
        .registry_ref()
        .register(Arc::clone(&supervisor))
        .expect("register WorkerControl");
    state
        .registry_ref()
        .register(Arc::new(FleetEventRegistry::new()))
        .expect("register FleetEventRegistry");
    install_events(&state).unwrap();

    let mut identities = Vec::new();
    for id in [1, 2] {
        let child = Command::new("/bin/sh")
            .args(["-c", "trap '' TERM; echo ready; exec sleep 60"])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let process =
            OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
                .unwrap();
        supervisor.workers.write().await.push(WorkerProcess::new(
            WorkerId::new(id),
            Some(process),
            WorkerPhase::Running,
            WorkerIntent::EnsureRunning,
        ));
        identities.push(
            supervisor.workers.read().await[usize::from(id) - 1]
                .identity(master)
                .unwrap(),
        );
    }
    supervisor.workers.write().await.push(WorkerProcess::new(
        WorkerId::new(3),
        None,
        WorkerPhase::Exited { outcome: ExitOutcome::Planned },
        WorkerIntent::Stopped,
    ));

    let mut requests = state.events().subscribe::<WorkerDrainRequested>();
    let mut exited = state.events().subscribe::<WorkersExited>();
    let shutdown = {
        let state = state.clone();
        tokio::spawn(async move { initiate_shutdown(&state).await })
    };

    let first = tokio::time::timeout(Duration::from_secs(1), requests.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(1), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first.operation, second.operation,
        "one fleet shutdown operation"
    );
    assert_eq!(
        supervisor.operations.fleet_shutdown().map(|op| (op.id, op.kind)),
        Some((first.operation, OperationKind::FleetShutdown))
    );
    let mut targets = vec![first.target, second.target];
    targets.sort_by_key(|target| target.worker_id());
    assert_eq!(
        targets, identities,
        "exactly the live identities are asked, no more"
    );
    assert_eq!(first.reason, DrainReason::Shutdown);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        requests.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        exited.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
    assert!(!shutdown.is_finished(), "the master waits while its workers live");
    assert!(
        !state.is_shutting_down(),
        "master providers stay up until the workers are gone"
    );

    for index in [0, 1] {
        supervisor.workers.write().await[index]
            .process
            .as_mut()
            .unwrap()
            .child_mut()
            .kill()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(exited.try_recv(), Ok(WorkersExited)));
    assert!(state.is_shutting_down());
    for worker in supervisor.workers.read().await.iter() {
        assert!(
            worker.is_stopped(),
            "worker #{} rests after the fleet shutdown",
            worker.id
        );
    }
}

/// A worker accepted from a dead master is logically managed by the current
/// master, but it is not this master's child. The normal coordinator still
/// emits its drain request; even the bootstrap fallback tested here must not
/// signal it, pin the new master, or silently acquire kill authority.
#[tokio::test]
async fn unbounded_shutdown_leaves_a_live_adopted_worker_outside_the_exit_barrier()
 {
    use std::process::Command;

    use crate::config::SupervisorConfig;
    use crate::control::WorkerProcess;
    use crate::lifecycle::{OwnedProcess, WorkerIntent, WorkerPhase};

    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("shutdown-adopted-test".into()),
        ".",
    );
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    let config: SupervisorConfig =
        serde_yaml::from_str("shutdown: { timeout: null }").unwrap();
    supervisor.config.store(Some(Arc::new(config)));
    supervisor.establish_master_for_tests();

    let mut child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let identity = claviron_sys::process::identity(child.id())
        .expect("adopted test process identity");
    let process =
        OwnedProcess::from_adopted(identity, 0, Arc::clone(&supervisor.exited))
            .unwrap();
    supervisor.workers.write().await.push(
        WorkerProcess::new(
            WorkerId::new(1),
            Some(process),
            WorkerPhase::Running,
            WorkerIntent::EnsureRunning,
        )
        .with_incarnation(2),
    );

    tokio::time::timeout(
        Duration::from_millis(250),
        supervisor.shutdown_workers(&state),
    )
    .await
    .expect("adopted worker must not be signalled or pin unbounded shutdown");
    tokio::time::timeout(Duration::from_millis(250), supervisor.wait_workers())
        .await
        .expect("the final exit barrier also excludes the adopted worker");
    assert!(
        identity.is_alive(),
        "the origin policy must not terminate the adopted worker"
    );

    child.kill().unwrap();
    child.wait().unwrap();
}
