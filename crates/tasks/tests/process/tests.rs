use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::child::{Action, Running};
use super::spawn::classify_start_failure;
use super::{
    CommandSpec, ExitAction, ProcessSignal, RestartPolicy, classify_exit,
    instance, run_command,
};
use crate::runner::{StopCause, StopContext};
use crate::{Error, LogConfig, Reason, TaskDispatch, TaskRuntime, TaskScope};

fn command(exit_action: ExitAction) -> CommandSpec {
    CommandSpec {
        program: "/bin/sleep".to_string(),
        args: vec!["30".to_string()],
        env: HashMap::new(),
        env_file: None,
        cwd: None,
        restart: RestartPolicy::OnFailure,
        exit_action,
        reload_action: None,
        log: LogConfig::default(),
        default_path: crate::config::default_command_path(),
    }
}

fn runtime() -> TaskRuntime {
    TaskRuntime {
        task_name: "persistent_command_test".into(),
        task_worker_number: 1,
        attempt: 1,
        task_scope: TaskScope::Workers,
        task_dispatch: TaskDispatch::Each,
        run_mode: RunMode::Standalone,
        worker_count: 0,
        process_id: std::process::id(),
    }
}

fn coordinated_runtime(
    dispatch: TaskDispatch,
    worker_id: u8,
) -> TaskRuntime {
    TaskRuntime {
        task_name: (match dispatch {
            TaskDispatch::One => "one_process_recovery_test",
            TaskDispatch::Pool => "pool_process_recovery_test",
            TaskDispatch::Each => unreachable!("each is not fleet-coordinated"),
        })
        .into(),
        task_worker_number: 1,
        attempt: 1,
        task_scope: TaskScope::Workers,
        task_dispatch: dispatch,
        run_mode: RunMode::Worker(WorkerId::new(worker_id)),
        worker_count: 2,
        process_id: std::process::id(),
    }
}

fn worker_state(
    name: Arc<str>,
    worker_id: u8,
) -> AppState {
    AppState::initialize(
        RunMode::Worker(WorkerId::new(worker_id)),
        FleetNodeId::new(u16::from(worker_id)),
        WorkerCount::new(2),
        FleetCapacity::new(3).expect("fleet capacity"),
        Some(name),
        PathBuf::from("."),
    )
}

#[test]
fn restart_policy_classifies_natural_process_completion() {
    let runtime = runtime();
    let mut spec = command(ExitAction::Terminate);
    let success = || ExitStatus::from_raw(0);
    let failure = || ExitStatus::from_raw(7 << 8);
    let terminated = || ExitStatus::from_raw(nix::libc::SIGTERM);

    assert!(classify_exit(&spec, &runtime, success()).is_ok());
    assert!(matches!(
        classify_exit(&spec, &runtime, failure()),
        Err(Error::Other(_))
    ));
    assert!(matches!(
        classify_exit(&spec, &runtime, terminated()),
        Err(Error::Other(_))
    ));
    assert!(matches!(
        classify_start_failure(
            &spec,
            &runtime,
            std::io::Error::other("missing")
        ),
        Error::Other(_)
    ));

    spec.restart = RestartPolicy::Never;
    assert!(classify_exit(&spec, &runtime, success()).is_ok());
    assert!(matches!(
        classify_exit(&spec, &runtime, failure()),
        Err(Error::Exit(Reason::Custom("process exited")))
    ));
    assert!(matches!(
        classify_start_failure(
            &spec,
            &runtime,
            std::io::Error::other("missing")
        ),
        Error::Exit(Reason::Custom("process failed to start"))
    ));

    spec.restart = RestartPolicy::Always;
    assert!(matches!(
        classify_exit(&spec, &runtime, success()),
        Err(Error::Other(_))
    ));
}

#[tokio::test]
async fn unhandled_reload_hup_is_a_natural_failure_not_owner_cancellation() {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let state = AppState::standalone(
        Arc::<str>::from(format!("claviron-task-hup-exit-{unique}")),
        PathBuf::from("."),
    );
    let runtime =
        TaskRuntime { task_name: "hup_exit_test".into(), ..runtime() };
    let token = CancellationToken::new();
    let stop = StopContext::default();
    let child_state = state.clone();
    let child_token = token.clone();
    let child_stop = stop.clone();
    let task_runtime = runtime.clone();
    let task = tokio::spawn(async move {
        run_command(
            child_state,
            child_token,
            child_stop,
            &command(ExitAction::Terminate),
            task_runtime,
            Some(Duration::from_secs(1)),
            true,
        )
        .await
    });

    let identity = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(identity) = instance::recorded_identity(&state, &runtime)
            {
                break identity;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sleep process started");
    stop.send_reload_signal(ProcessSignal::Hup);
    let result = timeout(Duration::from_secs(2), task).await.unwrap().unwrap();
    assert!(
        matches!(result, Err(Error::Other(_))),
        "on-failure must enter normal retry"
    );
    assert!(!token.is_cancelled());
    assert!(!identity.is_alive(), "exited child must be reaped");
    instance::remove_instance_dir(&state, &runtime);
}

#[tokio::test]
async fn kill_action_terminates_and_reaps_immediately() {
    let child = tokio::process::Command::new("/bin/sleep")
        .arg("30")
        .kill_on_drop(false)
        .spawn()
        .expect("sleep process");
    let pid = child.id().expect("sleep pid");
    let mut running = Running::Spawned(child);

    timeout(
        Duration::from_secs(2),
        running.apply(Action::Signal(ProcessSignal::Kill), None),
    )
    .await
    .expect("SIGKILL completion")
    .expect("SIGKILL action");

    let alive = unsafe { nix::libc::kill(pid as i32, 0) } == 0;
    assert!(!alive, "reaped process must no longer exist");
}

#[tokio::test]
async fn execution_timeout_kills_and_reaps_even_a_persistent_command() {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let state = AppState::standalone(
        Arc::<str>::from(format!("claviron-task-execution-timeout-{unique}")),
        PathBuf::from("."),
    );
    let runtime =
        TaskRuntime { task_name: "execution_timeout_test".into(), ..runtime() };
    let token = CancellationToken::new();
    let stop = StopContext::default();
    let child_state = state.clone();
    let child_token = token.clone();
    let child_stop = stop.clone();
    let task_runtime = runtime.clone();
    let task = tokio::spawn(async move {
        run_command(
            child_state,
            child_token,
            child_stop,
            &command(ExitAction::KeepRunning),
            task_runtime,
            Some(Duration::from_secs(1)),
            true,
        )
        .await
    });

    let identity = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(identity) = instance::recorded_identity(&state, &runtime)
            {
                break identity;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("persistent command started");

    stop.set(StopCause::ExecutionTimeout);
    token.cancel();
    let result = timeout(Duration::from_secs(2), task).await.unwrap().unwrap();
    assert!(matches!(result, Err(Error::Exit(Reason::ExecutionTimeout))));
    assert!(
        !identity.is_alive(),
        "timed-out command must be killed and reaped"
    );
    instance::remove_instance_dir(&state, &runtime);
}

#[tokio::test]
async fn exit_can_keep_daemon_running_for_later_reattachment() {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let state = AppState::standalone(
        Arc::<str>::from(format!("claviron-task-reload-test-{unique}")),
        PathBuf::from("."),
    );
    let runtime = runtime();

    let first_token = CancellationToken::new();
    let first_stop = StopContext::default();
    first_stop.set(StopCause::Shutdown);
    let first_state = state.clone();
    let first_wait = first_token.clone();
    let first_context = first_stop.clone();
    let task_runtime = runtime.clone();
    let first = tokio::spawn(async move {
        let spec = command(ExitAction::KeepRunning);
        run_command(
            first_state,
            first_wait,
            first_context,
            &spec,
            task_runtime,
            Some(Duration::from_secs(1)),
            true,
        )
        .await
    });

    let identity = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(identity) = instance::recorded_identity(&state, &runtime)
            {
                break identity;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("instance record");
    assert!(identity.is_alive());

    first_token.cancel();
    assert!(matches!(
        first.await.expect("first runner join"),
        Err(Error::Exit(Reason::Shutdown))
    ));
    assert!(identity.is_alive(), "exit action must leave the command alive");

    let second_token = CancellationToken::new();
    let second_state = state.clone();
    let second_wait = second_token.clone();
    let task_runtime = runtime.clone();
    let second = tokio::spawn(async move {
        run_command(
            second_state,
            second_wait,
            StopContext::default(),
            &command(ExitAction::Terminate),
            task_runtime,
            Some(Duration::from_secs(1)),
            true,
        )
        .await
    });
    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        instance::recorded_identity(&state, &runtime).unwrap(),
        identity
    );

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(identity.pid() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("stop inherited command");
    let replacement = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(current) = instance::recorded_identity(&state, &runtime)
                && current != identity
                && current.is_alive()
            {
                break current;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement command");

    second_token.cancel();
    let second_result = second.await.expect("second runner join");
    assert!(
        matches!(second_result, Err(Error::Exit(Reason::Shutdown))),
        "unexpected attached-runner result: {second_result:?}"
    );
    timeout(Duration::from_secs(2), async {
        while replacement.is_alive() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("managed attach termination");

    instance::remove_instance_dir(&state, &runtime);
}

async fn coordinated_command_is_reattached_by_the_next_worker_owner(
    dispatch: TaskDispatch
) {
    let unique =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let name: Arc<str> =
        Arc::from(format!("claviron-{dispatch:?}-recovery-test-{unique}"));
    let first_state = worker_state(Arc::clone(&name), 1);
    let second_state = worker_state(name, 2);
    let first_runtime = coordinated_runtime(dispatch, 1);
    let second_runtime = coordinated_runtime(dispatch, 2);

    let first_token = CancellationToken::new();
    let spawned_state = first_state.clone();
    let spawned_token = first_token.clone();
    let task_runtime = first_runtime.clone();
    let first = tokio::spawn(async move {
        run_command(
            spawned_state,
            spawned_token,
            StopContext::default(),
            &command(ExitAction::Terminate),
            task_runtime,
            Some(Duration::from_secs(1)),
            true,
        )
        .await
    });

    let identity = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(identity) =
                instance::recorded_identity(&first_state, &first_runtime)
            {
                break identity;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first coordinated command started");

    // Dropping the owner future models an abrupt worker loss: no cancellation
    // ladder runs, but the foreground command retains its instance lock.
    first.abort();
    assert!(
        first.await.expect_err("owner task must be aborted").is_cancelled()
    );
    assert!(
        identity.is_alive(),
        "abrupt owner loss must leave a recoverable process"
    );
    assert_eq!(
        instance::recorded_identity(&second_state, &second_runtime).unwrap(),
        identity,
        "coordinated identity must not include the former worker lane"
    );

    let second_token = CancellationToken::new();
    let attached_state = second_state.clone();
    let attached_token = second_token.clone();
    let task_runtime = second_runtime.clone();
    let second = tokio::spawn(async move {
        run_command(
            attached_state,
            attached_token,
            StopContext::default(),
            &command(ExitAction::Terminate),
            task_runtime,
            Some(Duration::from_secs(1)),
            true,
        )
        .await
    });
    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        instance::recorded_identity(&second_state, &second_runtime).unwrap(),
        identity
    );

    second_token.cancel();
    let result = timeout(Duration::from_secs(2), second)
        .await
        .expect("attached coordinated command stopped")
        .expect("second owner joined");
    assert!(matches!(result, Err(Error::Exit(Reason::Shutdown))));
    assert!(
        !identity.is_alive(),
        "replacement owner must terminate and reap the command"
    );

    instance::remove_instance_dir(&second_state, &second_runtime);
}

#[tokio::test]
async fn one_command_is_reattached_by_the_next_worker_owner() {
    coordinated_command_is_reattached_by_the_next_worker_owner(
        TaskDispatch::One,
    )
    .await;
}

#[tokio::test]
async fn pool_command_is_reattached_by_the_next_worker_owner() {
    coordinated_command_is_reattached_by_the_next_worker_owner(
        TaskDispatch::Pool,
    )
    .await;
}
