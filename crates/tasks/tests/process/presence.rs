use std::sync::Arc;

use claviron_runtime::{FleetCapacity, FleetNodeId, WorkerCount, WorkerId};

use super::*;
use crate::TaskScope;

fn fixture() -> (AppState, TaskRuntime) {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let state = AppState::standalone(
        Arc::<str>::from(format!("presence-test-{unique}")),
        ".",
    );
    let runtime = TaskRuntime {
        task_name: "presence_test".into(),
        task_worker_number: 1,
        attempt: 1,
        task_scope: TaskScope::Workers,
        task_dispatch: TaskDispatch::Each,
        run_mode: RunMode::Standalone,
        worker_count: 0,
        process_id: std::process::id(),
    };
    (state, runtime)
}

fn record_instance(
    state: &AppState,
    runtime: &TaskRuntime,
    process: ProcessIdentity,
) {
    let path = instance::instance_path(state, runtime).unwrap();
    let mut file = instance::open_instance_file(&path).unwrap();
    file.set_len(0).unwrap();
    write!(
        file,
        "v1 {} {} {}",
        process.pid(),
        process.started_at(),
        "00".repeat(32)
    )
    .unwrap();
}

#[test]
fn process_owner_and_identity_round_trip() {
    let process = ProcessIdentity::new(123, 987654321);
    for dispatch in [TaskDispatch::Each, TaskDispatch::One, TaskDispatch::Pool]
    {
        for owner in [
            ProcessOwner::Master(process),
            ProcessOwner::Standalone(process),
            ProcessOwner::Worker { worker_id: 4, process },
        ] {
            let record = Record {
                version: VERSION,
                task: "send-emails".into(),
                dispatch,
                slot: 2,
                process: ProcessIdentity::new(456, 123456789).into(),
                spawned_by: Some(owner.into()),
                controller: owner.into(),
                phase: TaskProcessPhase::Stopping,
                recorded_at_ms: 1234,
            };
            let bytes = serde_yaml::to_string(&record).unwrap();
            let decoded = Record::decode(bytes.as_bytes()).unwrap();
            assert_eq!(decoded.controller.owner().unwrap(), owner);
            assert_eq!(decoded.spawned_by.unwrap().owner().unwrap(), owner);
            assert_eq!(
                decoded.process.identity().unwrap(),
                ProcessIdentity::new(456, 123456789)
            );
            assert_eq!(decoded.dispatch, dispatch);
            assert_eq!(decoded.slot, 2);
            assert_eq!(decoded.phase, TaskProcessPhase::Stopping);
            assert!(
                Record::decode(
                    bytes.replace("version: 1", "version: 99").as_bytes()
                )
                .is_err()
            );
        }
    }
}

#[tokio::test]
async fn controller_tenure_is_exclusive_and_inventory_does_not_claim() {
    let (state, runtime) = fixture();
    let process = identity(std::process::id()).unwrap();
    let controller = Controller::acquire(&state, &runtime).await.unwrap();
    record_instance(&state, &runtime, process);
    controller.publish(process, false, TaskProcessPhase::Running).unwrap();
    assert!(
        matches!(Controller::acquire(&state, &runtime).await, Err(error) if error.kind() == io::ErrorKind::WouldBlock)
    );
    let before = inspect(&state).unwrap();
    assert!(before.issues.is_empty());
    assert_eq!(before.processes[0].control, TaskProcessControl::Managed);
    assert_eq!(before.processes[0].controller, before.processes[0].spawned_by);
    drop(controller);
    let after = inspect(&state).unwrap();
    assert_eq!(after.processes[0].control, TaskProcessControl::Detached);
    assert_eq!(after.processes[0].controller, None);
    assert_eq!(
        after.processes[0].last_controller,
        before.processes[0].controller
    );
    assert_eq!(after.processes[0].liveness, TaskProcessLiveness::Alive);
    let _controller = Controller::acquire(&state, &runtime).await.unwrap();
    drop(_controller);
    instance::remove_instance_dir(&state, &runtime);
}

#[tokio::test]
async fn attach_preserves_birth_owner_and_unknown_legacy_birth() {
    let (state, runtime) = fixture();
    let process = identity(std::process::id()).unwrap();
    let controller = Controller::acquire(&state, &runtime).await.unwrap();
    record_instance(&state, &runtime, process);
    // Old instance records are visible before a controller ever attaches.
    let legacy = inspect(&state).unwrap();
    assert_eq!(legacy.processes[0].control, TaskProcessControl::Unknown);
    assert_eq!(legacy.processes[0].task, None);
    controller.publish(process, true, TaskProcessPhase::Running).unwrap();
    assert_eq!(inspect(&state).unwrap().processes[0].spawned_by, None);

    let path = instance::instance_path(&state, &runtime)
        .unwrap()
        .with_extension("presence");
    let mut record = read_presence(&path).unwrap();
    let birth = ProcessOwner::Worker {
        worker_id: 4,
        process: ProcessIdentity::new(123, 99),
    };
    record.spawned_by = Some(birth.into());
    write_atomic(&path, serde_yaml::to_string(&record).unwrap().as_bytes())
        .unwrap();
    controller.publish(process, true, TaskProcessPhase::Stopping).unwrap();
    let status = &inspect(&state).unwrap().processes[0];
    assert_eq!(status.spawned_by, Some(birth));
    assert_eq!(status.controller, Some(ProcessOwner::Standalone(process)));
    assert_eq!(status.last_phase, Some(TaskProcessPhase::Stopping));
    drop(controller);
    instance::remove_instance_dir(&state, &runtime);
}

#[tokio::test]
async fn inventory_keeps_missing_worker_slots_and_rejects_reused_pid_metadata()
{
    let (state, mut runtime) = fixture();
    runtime.run_mode = RunMode::Worker(WorkerId::new(4));
    let process = identity(std::process::id()).unwrap();
    let controller = Controller::acquire(&state, &runtime).await.unwrap();
    record_instance(&state, &runtime, process);
    controller.publish(process, false, TaskProcessPhase::Running).unwrap();
    drop(controller);
    let smaller = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some(state.name().into()),
        state.config_dir(),
    );
    let snapshot = inspect(&smaller).unwrap();
    assert_eq!(
        snapshot.processes.len(),
        1,
        "not bounded by current worker count/capacity/config"
    );
    assert_eq!(snapshot.processes[0].task.as_deref(), Some("presence_test"));
    assert_eq!(snapshot.processes[0].control, TaskProcessControl::Detached);
    let reused = ProcessIdentity::new(process.pid(), process.started_at() ^ 1);
    record_instance(&state, &runtime, reused);
    let snapshot = inspect(&state).unwrap();
    assert_eq!(snapshot.processes[0].liveness, TaskProcessLiveness::Unverified);
    assert_eq!(snapshot.processes[0].controller, None);
    assert_eq!(snapshot.issues.len(), 1);
    instance::remove_instance_dir(&state, &runtime);
}

#[test]
fn invalid_records_are_reported_and_reads_do_not_create_a_namespace() {
    let (state, runtime) = fixture();
    let dir = instance::instance_directory(&state).unwrap();
    assert!(!dir.exists());
    assert!(inspect(&state).unwrap().processes.is_empty());
    assert!(!dir.exists());
    let path = instance::instance_path(&state, &runtime).unwrap();
    let mut file = instance::open_instance_file(&path).unwrap();
    write!(file, "v1 1 2 {}", "é".repeat(32)).unwrap();
    let snapshot = inspect(&state).unwrap();
    assert!(snapshot.processes.is_empty());
    assert_eq!(
        snapshot.issues.len(),
        1,
        "malformed fingerprint must not panic or disappear"
    );
    drop(file);
    instance::remove_instance_dir(&state, &runtime);
}
