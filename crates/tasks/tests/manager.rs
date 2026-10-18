use std::path::PathBuf;
use std::sync::Arc;

use claviron_orbit::FleetEventCodec;
use claviron_runtime::AppState;

use super::{
    MAX_TASK_NAME_BYTES, TaskCommand, TaskCommandAction, TaskManager,
    validate_task_name,
};
use crate::TaskLifecycle;
use crate::config::TaskServiceConfig;
use crate::service::TaskService;

#[test]
fn task_command_codec_round_trips() {
    for action in [
        TaskCommandAction::Start,
        TaskCommandAction::Stop,
        TaskCommandAction::Reload,
        TaskCommandAction::Restart,
    ] {
        let command = TaskCommand { action, name: "queue".to_string() };
        assert_eq!(
            TaskCommand::decode_orbit(&command.encode_orbit()).unwrap(),
            command
        );
    }
}

#[test]
fn task_command_rejects_names_that_do_not_fit_one_orbit_event() {
    let name = "x".repeat(MAX_TASK_NAME_BYTES + 1);

    assert!(validate_task_name(&name).is_err());
}

#[test]
fn standalone_list_returns_the_local_management_snapshot() {
    let state = AppState::standalone("task-manager-test", PathBuf::from("."));
    let parsed: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  cleanup:
    timing: { kind: interval, every: 1h }
"#,
    )
    .unwrap();
    let service = Arc::new(TaskService::new(PathBuf::from("tasks.yaml")));
    service.inner.tasks.store(Arc::new(parsed.tasks));
    service.register_fn("cleanup", |_: AppState, _| async { Ok(()) });
    state.registry_ref().register(service).expect("register TaskService");

    let snapshot = TaskManager::new(&state).unwrap().list().unwrap();

    assert_eq!(snapshot.members_expected, 1);
    assert_eq!(snapshot.members_reporting, 1);
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].name, "cleanup");
    assert_eq!(
        snapshot.tasks[0].placements[0].lifecycle,
        TaskLifecycle::Inactive
    );
}

#[test]
fn status_returns_one_task_from_the_current_snapshot() {
    let state =
        AppState::standalone("task-manager-status-test", PathBuf::from("."));
    let parsed: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  cleanup:
    timing: { kind: interval, every: 1h }
  queue:
    timing: { kind: daemon }
"#,
    )
    .unwrap();
    let service = Arc::new(TaskService::new(PathBuf::from("tasks.yaml")));
    service.inner.tasks.store(Arc::new(parsed.tasks));
    service.register_fn("cleanup", |_: AppState, _| async { Ok(()) });
    service.register_fn("queue", |_: AppState, _| async { Ok(()) });
    state.registry_ref().register(service).expect("register TaskService");

    let manager = TaskManager::new(&state).unwrap();
    let snapshot = manager.status("queue").unwrap();

    assert_eq!(snapshot.members_expected, 1);
    assert_eq!(snapshot.members_reporting, 1);
    assert_eq!(snapshot.task.name, "queue");
    assert_eq!(snapshot.task.placements.len(), 1);
    assert!(matches!(
        manager.status("missing"),
        Err(super::TaskManagerError::TaskNotFound(name)) if name == "missing"
    ));
}
