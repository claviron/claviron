use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use claviron_helpers::sync::LockUnpoisoned;
use claviron_runtime::registry::Reloadable;
use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount,
};
use tokio::time::timeout;

use super::{ReloadTaskChange, SupervisorPhase, TaskService};

/// Prepare `tasks.yaml` through the reader, as the runtime does before boot
/// and in a reload batch.
async fn prepare(
    service: &TaskService,
    state: &AppState,
) {
    let owner: &dyn claviron_runtime::congif::ConfigRegistration = service;
    state
        .config_reader()
        .prepare_owners(&[owner])
        .await
        .expect("prepare tasks.yaml");
}
use crate::config::TaskServiceConfig;
use crate::manager::{TaskCommand, TaskCommandAction, TaskManagerError};
use crate::process::ProcessSignal;
use crate::{Error, TaskExecutionResult, TaskLifecycle};

const IN_PLACE_SIGNALS: [(&str, ProcessSignal); 6] = [
    ("hup", ProcessSignal::Hup),
    ("int", ProcessSignal::Int),
    ("quit", ProcessSignal::Quit),
    ("term", ProcessSignal::Term),
    ("usr1", ProcessSignal::Usr1),
    ("usr2", ProcessSignal::Usr2),
];

#[test]
fn scheduler_cadence_change_replaces_only_its_task_generation() {
    fn plan(yaml: &str) -> crate::TaskConfig {
        let mut cfg: TaskServiceConfig = serde_yaml::from_str(yaml).unwrap();
        cfg.tasks.insert(
            "scheduler".to_owned(),
            Arc::new(
                crate::TaskConfig::from_yaml("timing: { kind: daemon }")
                    .unwrap(),
            ),
        );
        TaskService::materialize_scheduler(&mut cfg).unwrap();
        (*cfg.tasks["scheduler"]).clone()
    }
    let active = plan("scheduler: { poll_interval: 1m }");
    let changed = plan("scheduler: { poll_interval: 1s }");
    assert_eq!(
        super::classify_task_change(Some(&active), Some(&active), None),
        ReloadTaskChange::Unchanged
    );
    assert_eq!(
        super::classify_task_change(Some(&active), Some(&changed), None),
        ReloadTaskChange::Rebuild
    );
}

#[test]
fn scheduler_rejects_zero_poll_interval_before_publishing() {
    let mut cfg: TaskServiceConfig =
        serde_yaml::from_str("scheduler: { poll_interval: 0s }").unwrap();
    assert!(TaskService::materialize_scheduler(&mut cfg).is_err());
}

#[test]
fn scheduled_overlap_change_rebuilds_its_generation() {
    let skip = crate::TaskConfig::from_yaml(
        "timing: { kind: cron, expression: '0 * * * * *' }",
    )
    .unwrap();
    let allow = crate::TaskConfig::from_yaml(
        "timing: { kind: cron, expression: '0 * * * * *', overlap: allow }",
    )
    .unwrap();
    assert_eq!(
        super::classify_task_change(Some(&skip), Some(&allow), None),
        ReloadTaskChange::Rebuild
    );
}

fn service_with(yaml: &str) -> (TaskService, AppState) {
    let parsed: TaskServiceConfig =
        serde_yaml::from_str(yaml).expect("task config");
    let service = TaskService::new(PathBuf::from("tasks.yaml"));
    service.inner.tasks.store(Arc::new(parsed.tasks));
    let state = AppState::standalone("task-service-test", PathBuf::from("."));
    (service, state)
}

/// A builtin task's own entry fills the table when the file has none and
/// steps aside, whole, when the file names the task. Without this, a
/// builtin registered in code silently never runs.
#[test]
fn builtin_defaults_fill_the_table_and_the_file_replaces_them() {
    let service = TaskService::new(PathBuf::from("tasks.yaml"));
    let defaults = crate::config::TaskConfig::from_yaml(
        "timing: { kind: daemon }\nscope: workers\ndispatch: one\nattempts: unlimited\n"
    )
    .expect("builtin config");
    service.register_fn(
        "builtin",
        crate::TaskRunner::from(
            |_: AppState, _: tokio_util::sync::CancellationToken| async {
                Ok(())
            },
        )
        .with_defaults(defaults),
    );
    service.register_fn(
        "plain",
        |_: AppState, _: tokio_util::sync::CancellationToken| async { Ok(()) },
    );

    let mut tasks = crate::config::TaskTable::default();
    service.merge_builtin_defaults(&mut tasks);
    assert!(tasks["builtin"].timing.is_daemon());
    assert_eq!(tasks["builtin"].dispatch, Some(crate::TaskDispatch::One));
    assert!(
        !tasks.contains_key("plain"),
        "a task without defaults stays the file's business"
    );

    let parsed: TaskServiceConfig = serde_yaml::from_str(
        "tasks:\n  builtin:\n    timing: { kind: daemon, enabled: false }\n",
    )
    .expect("file config");
    let mut tasks = parsed.tasks;
    service.merge_builtin_defaults(&mut tasks);
    assert_eq!(tasks["builtin"].timing.trigger_enabled(), Some(false));
    assert_eq!(
        tasks["builtin"].dispatch, None,
        "the file entry replaces, it does not merge"
    );
}

#[test]
fn snapshot_prioritizes_scope_ineligibility_over_missing_runner() {
    let (service, _) = service_with(
        r#"
tasks:
  worker-only:
    timing: { kind: daemon }
    scope: workers
"#,
    );
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(3).expect("fleet capacity"),
        Some("task-service-test".into()),
        ".",
    );

    let snapshot = service.local_snapshot(&state);

    assert_eq!(
        snapshot.tasks[0].placements[0].lifecycle,
        TaskLifecycle::Ineligible
    );
}

/// The executor is asked once, in one place, and a task that names neither
/// block is `Rust` because the type says so — not because an `else` ran out
/// of alternatives. That `else` is SEMANTICS.md S14, and it reached eight
/// sites before this.
#[test]
fn every_task_names_exactly_one_executor() {
    let (service, state) = service_with(
        r#"
tasks:
  registered:
    timing: { kind: interval, every: 30s }
  spawned:
    timing: { kind: daemon }
    command: { program: /bin/true }
  embedded:
    timing: { kind: cron, expression: "0 * * * * *" }
    php: { script: artisan, args: ["schedule:run"] }
"#,
    );
    let tasks = service.task_snapshot();
    for (name, expected) in [
        ("registered", crate::TaskExecutorKind::Builtin),
        ("spawned", crate::TaskExecutorKind::Command),
        ("embedded", crate::TaskExecutorKind::Php),
    ] {
        assert_eq!(tasks[name].executor_kind(), expected, "{name}");
    }

    // A PHP task may not be placed on the master: the engine is not there.
    // Commands default to workers too; a Rust task is asked of its runner.
    assert_eq!(
        crate::TaskExecutorKind::Php.default_scope(),
        Some(crate::registry::TaskScope::Workers)
    );
    assert_eq!(
        crate::TaskExecutorKind::Command.default_scope(),
        Some(crate::registry::TaskScope::Worker)
    );
    assert_eq!(crate::TaskExecutorKind::Builtin.default_scope(), None);

    // Only a Rust body is a future this runner may bound on cancellation.
    assert!(!crate::TaskExecutorKind::Builtin.owns_shutdown());
    assert!(crate::TaskExecutorKind::Command.owns_shutdown());
    assert!(crate::TaskExecutorKind::Php.owns_shutdown());

    let _ = state;
}

/// Two executor blocks are refused where the configuration is accepted,
/// so no running task can ever hold both.
#[test]
fn a_task_may_not_declare_two_executors() {
    let (service, state) = service_with(
        r#"
tasks:
  confused:
    timing: { kind: daemon }
    command: { program: /bin/true }
    php: { script: artisan }
"#,
    );
    let tasks = service.task_snapshot();
    assert!(tasks["confused"].has_conflicting_executors());

    let error = service
        .validate_task_policies_for(
            &tasks,
            &crate::config::TaskPoolConfig::default(),
            &state,
        )
        .expect_err("a task has one executor");
    let message = error.to_string();
    assert!(message.contains("confused"), "{message}");
    assert!(
        message.contains("command") && message.contains("php"),
        "{message}"
    );
}

#[test]
fn service_graceful_timeout_does_not_supply_a_runtime_default() {
    let service = TaskService::new(PathBuf::from("tasks.yaml"));

    assert_eq!(service.graceful_timeout(), None);

    service
        .inner
        .graceful_timeout
        .store(Arc::new(Some(Duration::from_secs(12))));
    assert_eq!(service.graceful_timeout(), Some(Duration::from_secs(12)));

    service.inner.graceful_timeout.store(Arc::new(None));
    assert_eq!(service.graceful_timeout(), None);
}

#[test]
fn reload_plan_classifies_each_task_transition() {
    let (service, _) = service_with(
        r#"
tasks:
  unchanged:
    timing: { kind: daemon }
  stopped:
    timing: { kind: daemon }
  rebuilt:
    timing: { kind: daemon }
    concurrency: 1
  rescheduled:
    timing: { kind: interval, every: 30s }
  reconfigured:
    timing: { kind: daemon }
    attempts: 1
  disabled:
    timing: { kind: daemon, enabled: false }
"#,
    );
    let candidate: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  unchanged:
    timing: { kind: daemon }
  started:
    timing: { kind: daemon }
  rebuilt:
    timing: { kind: daemon }
    concurrency: 2
  rescheduled:
    timing: { kind: interval, every: 60s }
  reconfigured:
    timing: { kind: daemon }
    attempts: 2
  disabled:
    timing: { kind: interval, enabled: false, every: 10s }
"#,
    )
    .expect("candidate config");

    let changes = service
        .classify_reload(&service.task_snapshot(), &candidate.tasks)
        .into_iter()
        .collect::<HashMap<_, _>>();

    assert_eq!(changes["unchanged"], ReloadTaskChange::Unchanged);
    assert_eq!(changes["started"], ReloadTaskChange::Start);
    assert_eq!(changes["stopped"], ReloadTaskChange::Stop);
    assert_eq!(changes["rebuilt"], ReloadTaskChange::Rebuild);
    assert_eq!(changes["rescheduled"], ReloadTaskChange::Reschedule);
    assert_eq!(changes["reconfigured"], ReloadTaskChange::Reconfigure);
    assert_eq!(changes["disabled"], ReloadTaskChange::Unchanged);
}

#[test]
fn explicit_pool_reload_action_targets_only_its_task() {
    let (service, _) = service_with(
        r#"
tasks:
  queue:
    timing: { kind: daemon }
    dispatch: pool
    command:
      program: php
      reload_action: restart
  stable:
    timing: { kind: daemon }
    dispatch: pool
    command:
      program: php
  killed:
    timing: { kind: daemon }
    dispatch: pool
    command:
      program: php
      reload_action: { signal: kill }
"#,
    );
    let active = service.task_snapshot();
    let changes = service.classify_reload(&active, &active);

    let targets = service.reload_targets(&active, &active, &changes, false);

    assert!(targets.supervisors.is_empty());
    assert_eq!(targets.pools.len(), 2);
    assert_eq!(
        targets.pools.get("queue"),
        Some(&crate::process::ReloadAction::Restart)
    );
    assert_eq!(
        targets.pools.get("killed"),
        Some(&crate::process::ReloadAction::Signal(
            crate::process::ProcessSignal::Kill
        ))
    );
    assert!(!targets.pools.contains_key("stable"));
}

#[tokio::test]
async fn service_owns_one_active_supervisor_generation_per_task() {
    let (service, state) = service_with(
        r#"
tasks:
  daemon:
    timing: { kind: daemon }
"#,
    );
    service.register_fn(
        "daemon",
        |_: AppState, token: tokio_util::sync::CancellationToken| async move {
            token.cancelled().await;
            Ok(())
        },
    );

    assert_eq!(service.start_tasks(&state), Some(1));
    tokio::task::yield_now().await;
    assert_eq!(service.start_tasks(&state), Some(0));
    assert_eq!(service.inner.supervisors.lock_unpoisoned().len(), 1);

    service.cancel_all();
    timeout(Duration::from_secs(1), service.wait_all_stopped(None))
        .await
        .expect("owned supervisor stopped");
}

#[tokio::test]
async fn manager_commands_disable_enable_reload_and_restart_one_named_task() {
    let (service, state) = service_with(
        r#"
tasks:
  managed:
    timing: { kind: daemon }
"#,
    );
    let starts = Arc::new(AtomicUsize::new(0));
    let started = Arc::clone(&starts);
    service.register_fn(
        "managed",
        move |_: AppState, token: tokio_util::sync::CancellationToken| {
            let started = Arc::clone(&started);
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                token.cancelled().await;
                Ok(())
            }
        },
    );

    assert_eq!(service.start_tasks(&state), Some(1));
    timeout(Duration::from_secs(1), async {
        while starts.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial generation started");

    service
        .apply_manager_command(
            &state,
            TaskCommand {
                action: TaskCommandAction::Stop,
                name: "managed".to_string(),
            },
        )
        .await
        .expect("manager disable");
    assert_eq!(
        service.task_snapshot()["managed"].timing.trigger_enabled(),
        Some(false)
    );
    assert_eq!(service.start_tasks(&state), Some(0));

    service
        .apply_manager_command(
            &state,
            TaskCommand {
                action: TaskCommandAction::Start,
                name: "managed".to_string(),
            },
        )
        .await
        .expect("manager enable");
    assert_eq!(
        service.task_snapshot()["managed"].timing.trigger_enabled(),
        Some(true)
    );
    timeout(Duration::from_secs(1), async {
        while starts.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second generation started");

    service
        .apply_manager_command(
            &state,
            TaskCommand {
                action: TaskCommandAction::Reload,
                name: "managed".to_string(),
            },
        )
        .await
        .expect("manager reload");
    timeout(Duration::from_secs(1), async {
        while starts.load(Ordering::SeqCst) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replacement generation started");

    service
        .apply_manager_command(
            &state,
            TaskCommand {
                action: TaskCommandAction::Restart,
                name: "managed".to_string(),
            },
        )
        .await
        .expect("manager restart");
    timeout(Duration::from_secs(1), async {
        while starts.load(Ordering::SeqCst) != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("explicit replacement generation started");
    assert_eq!(
        service.local_snapshot(&state).tasks[0].placements[0].generation,
        4
    );

    service.cancel_all();
    timeout(Duration::from_secs(1), service.wait_all_stopped(None))
        .await
        .expect("managed task stopped");
}

#[tokio::test]
async fn manager_restart_replaces_a_pool_daemon_generation() {
    let (service, state) = service_with(
        r#"
tasks:
  pooled:
    timing: { kind: daemon }
    dispatch: pool
"#,
    );
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let task_starts = Arc::clone(&starts);
    let task_stops = Arc::clone(&stops);
    service.register_fn(
        "pooled",
        move |_: AppState, token: tokio_util::sync::CancellationToken| {
            let starts = Arc::clone(&task_starts);
            let stops = Arc::clone(&task_stops);
            async move {
                starts.fetch_add(1, Ordering::SeqCst);
                token.cancelled().await;
                stops.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );

    assert_eq!(service.start_tasks(&state), Some(1));
    timeout(Duration::from_secs(1), async {
        while starts.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial pool generation started");

    service
        .apply_manager_command(
            &state,
            TaskCommand {
                action: TaskCommandAction::Restart,
                name: "pooled".to_string(),
            },
        )
        .await
        .expect("pool manager restart");
    timeout(Duration::from_secs(1), async {
        while starts.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replacement pool generation started");
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    assert_eq!(
        service.local_snapshot(&state).tasks[0].placements[0].generation,
        2
    );

    service.cancel_all();
    timeout(Duration::from_secs(1), service.wait_all_stopped(None))
        .await
        .expect("pool generation stopped");
}

#[tokio::test]
async fn explicit_run_executes_one_occurrence_despite_concurrency() {
    let (service, state) = service_with(
        r#"
tasks:
  manual:
    concurrency: 2
"#,
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    service.register_fn("manual", move |_: AppState, _| {
        let task_calls = Arc::clone(&task_calls);
        async move {
            task_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });

    assert_eq!(service.start_tasks(&state), Some(0));
    service
        .run_explicit(&state, "manual", 41, None)
        .await
        .expect("explicit run accepted")
        .expect("manual task completed");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let snapshot = service.local_snapshot(&state);
    assert!(matches!(
        snapshot.tasks[0].timing,
        crate::TaskTimingSummary::Manual { .. }
    ));
    assert_eq!(
        snapshot.tasks[0].placements[0].lifecycle,
        TaskLifecycle::Inactive
    );
    assert_eq!(
        snapshot.tasks[0].placements[0]
            .last_execution
            .as_ref()
            .map(|outcome| outcome.result),
        Some(TaskExecutionResult::Succeeded)
    );
}

#[tokio::test]
async fn explicit_run_ignores_a_disabled_scheduler() {
    let (service, state) = service_with(
        r#"
tasks:
  scheduled:
    timing: { kind: interval, enabled: false, every: 30s }
"#,
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    service.register_fn("scheduled", move |_: AppState, _| {
        let task_calls = Arc::clone(&task_calls);
        async move {
            task_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });

    assert_eq!(service.start_tasks(&state), Some(0));
    service
        .run_explicit(&state, "scheduled", 42, None)
        .await
        .expect("disabled scheduler does not block explicit run")
        .expect("explicit task completed");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn manual_timing_rejects_enable_and_disable_commands() {
    let (service, _) = service_with(
        r#"
tasks:
  manual: {}
"#,
    );

    for action in [TaskCommandAction::Start, TaskCommandAction::Stop] {
        let error = service
            .validate_manager_command("manual", action)
            .expect_err("manual timing has no managed trigger");
        assert!(
            matches!(error, TaskManagerError::ManualTaskHasNoSchedule(name) if name == "manual")
        );
    }
}

#[test]
fn restart_requires_an_enabled_daemon() {
    let (service, _) = service_with(
        r#"
tasks:
  manual: {}
  interval:
    timing: { kind: interval, every: 30s }
  disabled:
    timing: { kind: daemon, enabled: false }
  daemon:
    timing: { kind: daemon }
"#,
    );

    for name in ["manual", "interval"] {
        let error = service
            .validate_manager_command(name, TaskCommandAction::Restart)
            .expect_err("only daemon timing can restart");
        assert!(
            matches!(error, TaskManagerError::RestartRequiresDaemon(task) if task == name)
        );
    }
    assert!(matches!(
        service.validate_manager_command("disabled", TaskCommandAction::Restart),
        Err(TaskManagerError::TaskStopped(name)) if name == "disabled"
    ));
    service
        .validate_manager_command("daemon", TaskCommandAction::Restart)
        .expect("enabled daemon can restart");
}

#[tokio::test]
async fn explicit_run_rejects_daemon_timing() {
    let (service, state) = service_with(
        r#"
tasks:
  short:
    timing: { kind: daemon }
"#,
    );
    service.register_fn("short", |_: AppState, _| async { Ok(()) });

    let error = service
        .run_explicit(&state, "short", 42, None)
        .await
        .expect_err("daemon requires enable");
    assert!(
        matches!(error, TaskManagerError::DaemonRequiresStart(name) if name == "short")
    );
}

#[tokio::test]
async fn explicit_runs_may_overlap_and_return_their_results() {
    let (service, state) = service_with(
        r#"
tasks:
  manual: {}
"#,
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let task_barrier = Arc::clone(&barrier);
    service.register_fn("manual", move |_: AppState, _| {
        let barrier = Arc::clone(&task_barrier);
        async move {
            barrier.wait().await;
            Ok(())
        }
    });

    let first_service = service.clone();
    let first_state = state.clone();
    let first = tokio::spawn(async move {
        first_service.run_explicit(&first_state, "manual", 43, None).await
    });
    let second_service = service.clone();
    let second_state = state.clone();
    let second = tokio::spawn(async move {
        second_service.run_explicit(&second_state, "manual", 44, None).await
    });

    timeout(Duration::from_secs(1), barrier.wait())
        .await
        .expect("both explicit runs entered together");
    first.await.unwrap().unwrap().expect("first task result");
    second.await.unwrap().unwrap().expect("second task result");
}

#[tokio::test]
async fn explicit_run_returns_the_task_error() {
    let (service, state) = service_with(
        r#"
tasks:
  failing: {}
"#,
    );
    service.register_fn("failing", |_: AppState, _| async {
        Err(Error::other("task result reached caller"))
    });

    let result =
        service.run_explicit(&state, "failing", 45, None).await.unwrap();

    assert!(matches!(
        result,
        Err(Error::Other(message)) if message == "task result reached caller"
    ));
}

#[tokio::test]
async fn dropping_the_waiter_does_not_cancel_the_explicit_run() {
    let (service, state) = service_with(
        r#"
tasks:
  manual: {}
"#,
    );
    let entered = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let task_entered = Arc::clone(&entered);
    let task_release = Arc::clone(&release);
    let task_completed = Arc::clone(&completed);
    service.register_fn("manual", move |_: AppState, _| {
        let entered = Arc::clone(&task_entered);
        let release = Arc::clone(&task_release);
        let completed = Arc::clone(&task_completed);
        async move {
            entered.wait().await;
            release.notified().await;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });

    let request_service = service.clone();
    let request_state = state.clone();
    let waiter = tokio::spawn(async move {
        request_service.run_explicit(&request_state, "manual", 46, None).await
    });
    timeout(Duration::from_secs(1), entered.wait())
        .await
        .expect("explicit task started");

    waiter.abort();
    assert!(
        waiter.await.expect_err("request waiter was cancelled").is_cancelled()
    );
    release.notify_one();

    timeout(Duration::from_secs(1), async {
        loop {
            let active = service
                .inner
                .explicit_runs
                .lock_unpoisoned()
                .values()
                .map(HashMap::len)
                .sum::<usize>();
            if completed.load(Ordering::SeqCst) == 1 && active == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("service-owned run completed after its waiter dropped");
}

#[tokio::test]
async fn explicit_run_panic_reaches_the_waiter_as_a_task_error() {
    let (service, state) = service_with(
        r#"
tasks:
  panicking: {}
"#,
    );
    service.register_fn("panicking", |_: AppState, _| async {
        panic!("explicit task panic reached waiter")
    });

    let result =
        service.run_explicit(&state, "panicking", 47, None).await.unwrap();

    assert!(matches!(
        result,
        Err(Error::Other(message)) if message.contains("explicit task panic reached waiter")
    ));
    assert!(service.inner.explicit_runs.lock_unpoisoned().is_empty());
}

#[tokio::test]
async fn explicit_run_uses_request_timeout_override() {
    let (service, state) = service_with(
        r#"
tasks:
  manual:
    timing: { kind: manual, execution_timeout: 1ms }
"#,
    );
    service.register_fn("manual", |_: AppState, _| async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(())
    });

    service
        .run_explicit(&state, "manual", 46, Some(Duration::from_millis(100)))
        .await
        .expect("explicit run accepted")
        .expect("request timeout overrides config");

    let result = service
        .run_explicit(&state, "manual", 47, None)
        .await
        .expect("explicit run accepted");
    assert!(matches!(
        result,
        Err(Error::Exit(crate::Reason::ExecutionTimeout))
    ));
}

#[tokio::test]
async fn snapshot_keeps_waiting_scheduler_active_without_in_flight_work() {
    let (service, state) = service_with(
        r#"
tasks:
  scheduled:
    timing: { kind: interval, every: 1h, skip_on_boot: true }
"#,
    );
    service.register_fn("scheduled", |_: AppState, _| async { Ok(()) });

    assert_eq!(service.start_tasks(&state), Some(1));
    let snapshot = service.local_snapshot(&state);
    let placement = &snapshot.tasks[0].placements[0];
    assert_eq!(placement.lifecycle, TaskLifecycle::Active);
    assert_eq!(placement.in_flight, 0);
    assert_eq!(placement.last_execution, None);

    service.cancel_all();
    timeout(Duration::from_secs(1), service.wait_all_stopped(None))
        .await
        .expect("scheduled supervisor stopped");
}

#[tokio::test]
async fn snapshot_retains_terminal_execution_outcome() {
    let (service, state) = service_with(
        r#"
tasks:
  failing:
    timing: { kind: daemon }
"#,
    );
    service.register_fn("failing", |_: AppState, _| async {
        Err(Error::other("boom"))
    });

    assert_eq!(service.start_tasks(&state), Some(1));
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = service.local_snapshot(&state);
            if snapshot.tasks[0].placements[0].lifecycle
                == TaskLifecycle::Finished
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failing supervisor finished");

    let snapshot = service.local_snapshot(&state);
    let placement = &snapshot.tasks[0].placements[0];
    assert_eq!(placement.in_flight, 0);
    assert_eq!(
        placement.last_execution.as_ref().map(|outcome| outcome.result),
        Some(TaskExecutionResult::GaveUp)
    );
}

#[tokio::test]
async fn targeted_reload_stops_removed_task_without_bouncing_unchanged_task() {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    let path = std::env::temp_dir().join(format!(
        "claviron-tasks-targeted-reload-{}-{unique}.yaml",
        std::process::id()
    ));
    tokio::fs::write(
        &path,
        r#"
tasks:
  stable:
    timing: { kind: daemon }
  removed:
    timing: { kind: daemon }
"#,
    )
    .await
    .expect("write initial config");

    let service = TaskService::new(path.clone());
    let state = AppState::standalone("task-service-test", PathBuf::from("."));
    let stable_starts = Arc::new(AtomicUsize::new(0));
    let stable_stops = Arc::new(AtomicUsize::new(0));
    let removed_starts = Arc::new(AtomicUsize::new(0));
    let removed_stops = Arc::new(AtomicUsize::new(0));

    let starts = Arc::clone(&stable_starts);
    let stops = Arc::clone(&stable_stops);
    service.register_fn(
        "stable",
        move |_: AppState, token: tokio_util::sync::CancellationToken| {
            let starts = Arc::clone(&starts);
            let stops = Arc::clone(&stops);
            async move {
                starts.fetch_add(1, Ordering::SeqCst);
                token.cancelled().await;
                stops.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    let starts = Arc::clone(&removed_starts);
    let stops = Arc::clone(&removed_stops);
    service.register_fn(
        "removed",
        move |_: AppState, token: tokio_util::sync::CancellationToken| {
            let starts = Arc::clone(&starts);
            let stops = Arc::clone(&stops);
            async move {
                starts.fetch_add(1, Ordering::SeqCst);
                token.cancelled().await;
                stops.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );

    prepare(&service, &state).await;
    service.load_tasks(&state).await.expect("load initial config");
    assert_eq!(service.start_tasks(&state), Some(2));
    timeout(Duration::from_secs(1), async {
        while stable_starts.load(Ordering::SeqCst) != 1
            || removed_starts.load(Ordering::SeqCst) != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial task generation started");

    tokio::fs::write(
        &path,
        r#"
tasks:
  stable:
    timing: { kind: daemon }
"#,
    )
    .await
    .expect("write candidate config");
    prepare(&service, &state).await;
    timeout(Duration::from_secs(1), Reloadable::reload(&service, &state))
        .await
        .expect("targeted reload completed")
        .expect("targeted reload succeeded");

    assert_eq!(stable_starts.load(Ordering::SeqCst), 1);
    assert_eq!(stable_stops.load(Ordering::SeqCst), 0);
    assert_eq!(removed_starts.load(Ordering::SeqCst), 1);
    assert_eq!(removed_stops.load(Ordering::SeqCst), 1);
    {
        let supervisors = service.inner.supervisors.lock_unpoisoned();
        assert!(supervisors.contains_key("stable"));
        assert!(!supervisors.contains_key("removed"));
    }

    service.cancel_all();
    timeout(Duration::from_secs(1), service.wait_all_stopped(None))
        .await
        .expect("remaining supervisor stopped");
    tokio::fs::remove_file(path).await.expect("remove task config");
}

#[tokio::test]
async fn targeted_pool_reload_does_not_bounce_unchanged_pool_task() {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    let path = std::env::temp_dir().join(format!(
        "claviron-tasks-targeted-pool-reload-{}-{unique}.yaml",
        std::process::id()
    ));
    tokio::fs::write(
        &path,
        r#"
tasks:
  stable:
    timing: { kind: daemon }
    dispatch: pool
    attempts: unlimited
  changed:
    timing: { kind: daemon }
    dispatch: pool
    attempts: 1
"#,
    )
    .await
    .expect("write initial config");

    let service = TaskService::new(path.clone());
    let state = AppState::standalone("task-service-test", PathBuf::from("."));
    let stable_starts = Arc::new(AtomicUsize::new(0));
    let stable_stops = Arc::new(AtomicUsize::new(0));
    let changed_starts = Arc::new(AtomicUsize::new(0));
    let changed_stops = Arc::new(AtomicUsize::new(0));

    let starts = Arc::clone(&stable_starts);
    let stops = Arc::clone(&stable_stops);
    service.register_fn(
        "stable",
        move |_: AppState, token: tokio_util::sync::CancellationToken| {
            let starts = Arc::clone(&starts);
            let stops = Arc::clone(&stops);
            async move {
                starts.fetch_add(1, Ordering::SeqCst);
                token.cancelled().await;
                stops.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    let starts = Arc::clone(&changed_starts);
    let stops = Arc::clone(&changed_stops);
    service.register_fn(
        "changed",
        move |_: AppState, token: tokio_util::sync::CancellationToken| {
            let starts = Arc::clone(&starts);
            let stops = Arc::clone(&stops);
            async move {
                starts.fetch_add(1, Ordering::SeqCst);
                token.cancelled().await;
                stops.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );

    prepare(&service, &state).await;
    service.load_tasks(&state).await.expect("load initial config");
    assert_eq!(service.start_tasks(&state), Some(2));
    timeout(Duration::from_secs(1), async {
        while stable_starts.load(Ordering::SeqCst) != 1
            || changed_starts.load(Ordering::SeqCst) != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial pool task generations started");

    tokio::fs::write(
        &path,
        r#"
tasks:
  stable:
    timing: { kind: daemon }
    dispatch: pool
    attempts: unlimited
  changed:
    timing: { kind: daemon }
    dispatch: pool
    attempts: 2
"#,
    )
    .await
    .expect("write candidate config");
    prepare(&service, &state).await;
    timeout(Duration::from_secs(1), Reloadable::reload(&service, &state))
        .await
        .expect("targeted pool reload completed")
        .expect("targeted pool reload succeeded");
    timeout(Duration::from_secs(1), async {
        while changed_starts.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("changed pool task replacement started");

    assert_eq!(stable_starts.load(Ordering::SeqCst), 1);
    assert_eq!(stable_stops.load(Ordering::SeqCst), 0);
    assert_eq!(changed_stops.load(Ordering::SeqCst), 1);

    service.cancel_all();
    timeout(Duration::from_secs(1), service.wait_all_stopped(None))
        .await
        .expect("pool controllers stopped");
    tokio::fs::remove_file(path).await.expect("remove task config");
}

#[tokio::test]
async fn reload_timeout_keeps_old_config_and_stopping_generation() {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    let path = std::env::temp_dir().join(format!(
        "claviron-tasks-reload-timeout-{}-{unique}.yaml",
        std::process::id()
    ));
    tokio::fs::write(
        &path,
        r#"
graceful_timeout: 20ms
tasks:
  stuck:
    timing: { kind: daemon }
"#,
    )
    .await
    .expect("write initial config");

    let service = TaskService::new(path.clone());
    let state = AppState::standalone("task-service-test", PathBuf::from("."));
    let starts = Arc::new(AtomicUsize::new(0));
    let task_starts = Arc::clone(&starts);
    service.register_fn(
        "stuck",
        move |_: AppState, _: tokio_util::sync::CancellationToken| {
            let starts = Arc::clone(&task_starts);
            async move {
                starts.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok(())
            }
        },
    );

    prepare(&service, &state).await;
    service.load_tasks(&state).await.expect("load initial config");
    assert_eq!(service.start_tasks(&state), Some(1));
    timeout(Duration::from_secs(1), async {
        while starts.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stuck generation started");

    tokio::fs::write(&path, "graceful_timeout: 20ms\ntasks: {}\n")
        .await
        .expect("write candidate config");
    prepare(&service, &state).await;
    let error =
        timeout(Duration::from_secs(1), Reloadable::reload(&service, &state))
            .await
            .expect("reload returned at service deadline")
            .expect_err("stuck generation must reject candidate");

    assert!(error.to_string().contains("candidate config was not published"));
    assert!(service.task_snapshot().contains_key("stuck"));
    let active = service
        .inner
        .supervisors
        .lock()
        .unwrap()
        .remove("stuck")
        .expect("old generation remains owned");
    assert_eq!(active.phase, SupervisorPhase::Stopping);
    active.handle.abort();

    tokio::fs::remove_file(path).await.expect("remove task config");
}

#[test]
fn dispatch_one_accepts_daemon_tasks() {
    let (service, state) = service_with(
        r#"
tasks:
  queue:
    dispatch: one
    timing: { kind: daemon }
"#,
    );

    service
        .validate_task_policies(&state)
        .expect("daemon dispatch one must be valid");
}

#[test]
fn dispatch_pool_rejects_scheduled_tasks() {
    let (service, state) = service_with(
        r#"
tasks:
  queue:
    timing: { kind: interval, every: 30s }
    dispatch: pool
    concurrency: 3
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("scheduled dispatch pool must fail");
    assert!(error.to_string().contains("long-running daemons"));
}

#[test]
fn scheduled_task_rejects_non_terminating_action() {
    let (service, state) = service_with(
        r#"
tasks:
  queue:
    timing: { kind: interval, every: 30s }
    command:
      program: php
      exit_action: keep-running
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("scheduled keep-running command must fail");
    assert!(
        error.to_string().contains("scheduled commands cannot keep running")
    );
}

#[test]
fn coordinated_dispatch_accepts_reload_signals_but_not_detached_exit_actions() {
    for dispatch in ["one", "pool"] {
        for (signal, _) in IN_PLACE_SIGNALS {
            let (service, state) = service_with(&format!(
                "tasks:\n  queue:\n    timing: {{ kind: daemon }}\n    dispatch: {dispatch}\n    command:\n      program: php\n      reload_action: {{ signal: {signal} }}\n"
            ));
            service
                .validate_task_policies(&state)
                .expect("coordinated reload signal keeps ownership");
        }

        for action in [
            "exit_action: keep-running",
            "exit_action: { signal: hup }",
            "exit_action: { signal: usr1 }",
            "exit_action: { signal: term }",
        ] {
            let (service, state) = service_with(&format!(
                "tasks:\n  queue:\n    timing: {{ kind: daemon }}\n    dispatch: {dispatch}\n    command:\n      program: php\n      {action}\n"
            ));
            let error = service
                .validate_task_policies(&state)
                .expect_err("detached coordinated command rejected");
            assert!(error.to_string().contains(
                "slot ownership cannot be relinquished while its command remains running"
            ));
        }
    }
}

#[test]
fn reload_signals_target_only_unchanged_generations() {
    for dispatch in ["each", "one", "pool"] {
        let yaml = format!(
            "tasks:\n  queue:\n    timing: {{ kind: daemon }}\n    dispatch: {dispatch}\n    command:\n      program: php\n      reload_action: {{ signal: hup }}\n"
        );
        let (service, _) = service_with(&yaml);
        let active = service.task_snapshot();
        let changes = service.classify_reload(&active, &active);
        let targets = service.reload_targets(&active, &active, &changes, false);
        assert!(!targets.has_transitions());
        assert_eq!(targets.signals["queue"], ProcessSignal::Hup);

        for (name, signal) in IN_PLACE_SIGNALS {
            let candidate: TaskServiceConfig = serde_yaml::from_str(
                &yaml.replace("signal: hup", &format!("signal: {name}")),
            )
            .unwrap();
            let changes = service.classify_reload(&active, &candidate.tasks);
            let targets = service.reload_targets(
                &active,
                &candidate.tasks,
                &changes,
                false,
            );
            assert!(!targets.has_transitions());
            assert_eq!(targets.signals["queue"], signal);
        }

        let targets = service.reload_targets(&active, &active, &changes, true);
        if dispatch != "each" {
            assert!(targets.signals.is_empty());
            assert_eq!(
                targets.pools["queue"],
                crate::process::ReloadAction::Restart
            );
        } else {
            assert!(!targets.has_transitions());
            assert_eq!(targets.signals["queue"], ProcessSignal::Hup);
        }

        for candidate in [
            yaml.replace("program: php", "program: php-next"),
            yaml.replace("    command:", "    concurrency: 2\n    command:"),
            yaml.replace("signal: hup", "signal: kill"),
        ] {
            let candidate: TaskServiceConfig =
                serde_yaml::from_str(&candidate).unwrap();
            let changes = service.classify_reload(&active, &candidate.tasks);
            let targets = service.reload_targets(
                &active,
                &candidate.tasks,
                &changes,
                false,
            );
            assert!(targets.has_transitions());
            assert!(targets.signals.is_empty());
        }

        let candidate: TaskServiceConfig = serde_yaml::from_str(
            &yaml.replace("      reload_action: { signal: hup }\n", ""),
        )
        .unwrap();
        let changes = service.classify_reload(&active, &candidate.tasks);
        let targets =
            service.reload_targets(&active, &candidate.tasks, &changes, false);
        assert!(!targets.has_transitions());
        assert!(
            targets.signals.is_empty(),
            "omitted action remains a true no-op"
        );
    }
}

#[test]
fn scheduled_task_rejects_restart_always() {
    let (service, state) = service_with(
        r#"
tasks:
  sweep:
    timing: { kind: interval, every: 30s }
    command:
      program: php
      restart: always
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("scheduled restart always must fail");
    assert!(error.to_string().contains("restart 'always' with a schedule"));
}

#[test]
fn dispatch_pool_rejects_master_scope() {
    let (service, state) = service_with(
        r#"
tasks:
  queue:
    scope: master
    dispatch: pool
    concurrency: 3
    timing: { kind: daemon }
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("master-only pool must fail");
    assert!(error.to_string().contains("scope 'workers' or 'fleet'"));
}

#[test]
fn dispatch_pool_accepts_workers_scope() {
    let (service, state) = service_with(
        r#"
tasks:
  queue:
    scope: workers
    dispatch: pool
    concurrency: 3
    timing: { kind: daemon }
"#,
    );

    service.validate_task_policies(&state).expect("workers pool must be valid");
}

#[test]
fn standalone_pool_does_not_require_orbit() {
    let (service, state) = service_with(
        r#"
tasks:
  queue:
    dispatch: pool
    concurrency: 3
    timing: { kind: daemon }
"#,
    );

    service
        .validate_task_policies(&state)
        .expect("standalone pool must be local");
}

#[test]
fn pool_requires_positive_concurrency() {
    let (service, state) = service_with(
        r#"
tasks:
  queue:
    dispatch: pool
    concurrency: 0
    timing: { kind: daemon }
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("zero pool concurrency must fail");
    assert!(error.to_string().contains("greater than zero"));
}

#[test]
fn dispatch_one_requires_single_local_worker() {
    let (service, state) = service_with(
        r#"
tasks:
  sweep:
    timing: { kind: interval, every: 30s }
    dispatch: one
    concurrency: 2
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("dispatch one concurrency must fail");
    assert!(error.to_string().contains("concurrency: 1"));
}

#[test]
fn supervised_dispatch_one_requires_orbit_fleet() {
    let (service, _) = service_with(
        r#"
tasks:
  sweep:
    timing: { kind: interval, every: 30s }
    dispatch: one
"#,
    );
    let state = AppState::initialize(
        RunMode::Worker(claviron_runtime::WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(1),
        FleetCapacity::new(2).expect("fleet capacity"),
        Some("task-service-test".into()),
        ".",
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("dispatch one without fleet must fail");
    assert!(error.to_string().contains("requires"));
}

#[test]
fn zero_interval_is_rejected_before_spawn() {
    let (service, state) = service_with(
        r#"
tasks:
  sweep:
    timing: { kind: interval, every: 0s }
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("zero interval must fail");
    assert!(error.to_string().contains("greater than zero"));
}

#[test]
fn zero_execution_timeout_is_rejected_before_spawn() {
    let (service, state) = service_with(
        r#"
tasks:
  sweep:
    timing: { kind: cron, expression: "0 * * * * *", execution_timeout: 0s }
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("zero execution timeout must fail");
    assert!(
        error
            .to_string()
            .contains("execution_timeout must be greater than zero")
    );
}

#[test]
fn invalid_cron_is_rejected_before_spawn() {
    let (service, state) = service_with(
        r#"
tasks:
  sweep:
    timing: { kind: cron, expression: definitely-not-cron }
"#,
    );

    let error = service
        .validate_task_policies(&state)
        .expect_err("invalid cron must fail");
    assert!(error.to_string().contains("invalid cron expression"));
}

#[tokio::test]
async fn invalid_candidate_is_rejected_before_snapshot_swap() {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    let path = std::env::temp_dir().join(format!(
        "claviron-tasks-invalid-candidate-{}-{unique}.yaml",
        std::process::id()
    ));
    tokio::fs::write(
        &path,
        r#"
tasks:
  queue:
    timing: { kind: interval, every: 30s }
    dispatch: pool
    concurrency: 3
"#,
    )
    .await
    .expect("write candidate config");

    let service = TaskService::new(path.clone());
    let initial: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  queue:
    timing: { kind: daemon }
    dispatch: pool
    concurrency: 3
"#,
    )
    .expect("initial config");
    service.inner.tasks.store(Arc::new(initial.tasks));
    let state = AppState::standalone("task-service-test", PathBuf::from("."));

    prepare(&service, &state).await;
    let error = service
        .load_tasks(&state)
        .await
        .expect_err("scheduled pool candidate must fail before publication");
    assert!(error.to_string().contains("long-running daemons"));
    assert!(
        service
            .task_snapshot()
            .get("queue")
            .expect("original queue")
            .timing
            .is_daemon()
    );

    tokio::fs::remove_file(path).await.expect("remove candidate config");
}

#[tokio::test]
async fn valid_candidate_remains_private_until_published() {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    let path = std::env::temp_dir().join(format!(
        "claviron-tasks-private-candidate-{}-{unique}.yaml",
        std::process::id()
    ));
    tokio::fs::write(
        &path,
        r#"
tasks:
  next:
    timing: { kind: daemon }
"#,
    )
    .await
    .expect("write candidate config");

    let service = TaskService::new(path.clone());
    let initial: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  current:
    timing: { kind: daemon }
"#,
    )
    .expect("initial config");
    service.inner.tasks.store(Arc::new(initial.tasks));
    let state = AppState::standalone("task-service-test", PathBuf::from("."));

    prepare(&service, &state).await;
    let candidate =
        service.read_task_config(&state).await.expect("valid candidate");

    assert!(candidate.tasks.contains_key("next"));
    assert!(service.task_snapshot().contains_key("current"));
    assert!(!service.task_snapshot().contains_key("next"));

    tokio::fs::remove_file(path).await.expect("remove candidate config");
}
