use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
};

use super::{WorkerControl, WorkerProcess};
use crate::config::SupervisorConfig;
use crate::lifecycle::{
    DrainReason, ExitDisposition, ExitOutcome, OwnedProcess, WorkerIntent,
    WorkerPhase,
};
use crate::observer::HeartbeatObserver;
use crate::snapshot::WorkerLifecycle;

/// A child that has already installed `SIG_IGN` for `SIGTERM` when this
/// returns: it announces readiness on stdout first, so a signal sent right after
/// spawning cannot land before the trap does. `exec` keeps the PID, and an
/// ignored disposition survives `exec`.
fn child_that_ignores_sigterm() -> std::process::Child {
    use std::io::{BufRead, BufReader};

    let mut child = Command::new("/bin/sh")
        .args(["-c", "trap '' TERM; echo ready; exec sleep 60"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut ready).unwrap();
    assert_eq!(ready.trim(), "ready");
    child
}

fn configure(
    supervisor: &WorkerControl,
    yaml: &str,
) {
    let config: SupervisorConfig =
        serde_yaml::from_str(yaml).expect("parse supervisor config");
    supervisor.config.store(Some(Arc::new(config)));
    supervisor.establish_master_for_tests();
}

fn master_state(worker_count: u8) -> AppState {
    AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(worker_count),
        FleetCapacity::new(u16::from(worker_count) + 1).unwrap(),
        Some("management-test".into()),
        ".",
    )
}

async fn operation_runner(
    control: Arc<WorkerControl>,
    state: &AppState,
) -> (
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<claviron_runtime::registry::Result<()>>,
) {
    use claviron_runtime::registry::Runnable;
    let cancel = tokio_util::sync::CancellationToken::new();
    let runner = Arc::new(
        crate::management::runner::WorkerOperationRunner::new(control),
    );
    let run = tokio::spawn(runner.run(
        (),
        state.clone(),
        claviron_runtime::RunContext::new(cancel.clone()),
    ));
    tokio::task::yield_now().await;
    (cancel, run)
}

/// A worker that ignores SIGTERM is exactly the one whose drain waits — and the
/// one the 2026-09-13 master lost sight of. The handle stays in its row now, so
/// the snapshot keeps showing the PID for as long as the process exists.
#[tokio::test]
async fn draining_slot_keeps_its_pid_while_a_waiter_awaits_exit() {
    let supervisor =
        Arc::new(WorkerControl::new(Arc::new(HeartbeatObserver::new())));
    configure(&supervisor, "shutdown: { timeout: null }");
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("management-test".into()),
        ".",
    );
    let child = child_that_ignores_sigterm();
    let process =
        OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
            .unwrap();
    let identity = process.identity();
    supervisor.workers.write().await.push(
        WorkerProcess::new(
            WorkerId::new(1),
            Some(process),
            WorkerPhase::Running,
            WorkerIntent::Drain {
                reason: DrainReason::ManualStop,
                after_exit: ExitDisposition::StayStopped,
            },
        )
        .with_reload_revision(2),
    );
    supervisor.signal_alive_workers(nix::sys::signal::Signal::SIGTERM).await;

    let waiter = {
        let supervisor = Arc::clone(&supervisor);
        tokio::spawn(async move {
            supervisor.await_slot_exit(WorkerId::new(1), identity, "test").await
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !waiter.is_finished(),
        "a process that ignores SIGTERM must keep the drain waiting"
    );

    let snapshot = supervisor.management_snapshot(&state).await;
    assert_eq!(snapshot.workers.len(), 1);
    assert_eq!(snapshot.workers[0].worker_id, 1);
    assert_eq!(snapshot.workers[0].lifecycle(), WorkerLifecycle::Stopping);
    assert_eq!(snapshot.workers[0].ownership, crate::WorkerOwnership::Spawned);
    assert_eq!(
        snapshot.workers[0].pid(),
        Some(identity.pid()),
        "the draining PID must stay visible"
    );
    assert!(snapshot.workers[0].uptime.is_some());
    assert_eq!(snapshot.workers[0].reload_revision, 2);

    supervisor.workers.write().await[0]
        .process
        .as_mut()
        .unwrap()
        .child_mut()
        .kill()
        .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(outcome, ExitOutcome::Planned);
    let workers = supervisor.workers.read().await;
    assert!(workers[0].is_stopped());
    assert_eq!(
        workers[0].pid(),
        Some(identity.pid()),
        "the reaped process stays known"
    );
    assert_eq!(workers[0].lifecycle(), WorkerLifecycle::Stopped);
}

#[tokio::test]
async fn removed_slot_tombstones_are_hidden_from_management_snapshots() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(3).unwrap(),
        Some("management-test".into()),
        ".",
    );
    supervisor.workers.write().await.extend([
        WorkerProcess::new(
            WorkerId::new(1),
            None,
            WorkerPhase::Running,
            WorkerIntent::EnsureRunning,
        )
        .with_incarnation(2),
        WorkerProcess::new(
            WorkerId::new(2),
            None,
            WorkerPhase::Exited { outcome: ExitOutcome::Planned },
            WorkerIntent::Stopped,
        )
        .with_incarnation(4)
        .with_reload_revision(1),
    ]);

    let snapshot = supervisor.management_snapshot(&state).await;
    assert_eq!(snapshot.workers_expected, 1);
    assert_eq!(snapshot.workers.len(), 1);
    assert_eq!(snapshot.workers[0].worker_id, 1);
}

#[tokio::test]
async fn supervisor_refuses_to_stop_its_last_running_worker() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    supervisor.workers.write().await.push(WorkerProcess::new(
        WorkerId::new(1),
        None,
        WorkerPhase::Running,
        WorkerIntent::EnsureRunning,
    ));

    configure(&supervisor, "shutdown: { timeout: null }");
    let error = supervisor
        .stop_worker(
            WorkerId::new(1),
            &master_state(1),
            crate::lifecycle::OperationId::from_raw(1).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("last running worker"));
    assert!(supervisor.workers.read().await[0].is_running());
}

fn draining_row(
    supervisor: &WorkerControl,
    child: std::process::Child,
) -> claviron_sys::process::ProcessIdentity {
    let process =
        OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
            .unwrap();
    let identity = process.identity();
    supervisor.workers.try_write().unwrap().push(WorkerProcess::new(
        WorkerId::new(1),
        Some(process),
        WorkerPhase::Running,
        WorkerIntent::Drain {
            reason: DrainReason::ManualStop,
            after_exit: ExitDisposition::StayStopped,
        },
    ));
    identity
}

#[tokio::test]
async fn unbounded_drain_never_invents_force_termination() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    configure(&supervisor, "shutdown: { timeout: null }");
    let identity = draining_row(
        &supervisor,
        Command::new("/bin/sleep").arg("60").spawn().unwrap(),
    );

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        supervisor.await_slot_exit(WorkerId::new(1), identity, "test"),
    )
    .await;

    assert!(result.is_err(), "an unbounded drain unexpectedly completed");
    assert!(identity.is_alive(), "an implicit force kill was sent");
    let mut workers = supervisor.workers.write().await;
    let child = workers[0].process.as_mut().unwrap().child_mut();
    child.kill().unwrap();
    child.wait().unwrap();
}

#[tokio::test]
async fn fleet_control_shutdown_does_not_also_deliver_a_posix_signal() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    configure(&supervisor, "shutdown: { timeout: null }");
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("management-test".into()),
        ".",
    );
    let child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
    supervisor.workers.write().await.push(WorkerProcess::new(
        WorkerId::new(1),
        Some(
            OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
                .unwrap(),
        ),
        WorkerPhase::Running,
        WorkerIntent::EnsureRunning,
    ));

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        supervisor.shutdown_workers_after_drain_requests(&state),
    )
    .await;

    assert!(result.is_err(), "worker unexpectedly received a POSIX signal");
    let mut workers = supervisor.workers.write().await;
    let child = workers[0].process.as_mut().unwrap().child_mut();
    child.kill().unwrap();
    child.wait().unwrap();
}

#[tokio::test]
async fn operator_deadline_authorizes_force_termination() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    configure(&supervisor, "shutdown: { timeout: 10ms }");
    let identity = draining_row(
        &supervisor,
        Command::new("/bin/sleep").arg("60").spawn().unwrap(),
    );

    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        supervisor.await_slot_exit(WorkerId::new(1), identity, "test"),
    )
    .await
    .expect("bounded drain did not complete")
    .expect("bounded drain failed");
    assert_eq!(outcome, ExitOutcome::Planned);
    assert!(!identity.is_alive());
}

fn slot_command(
    action: crate::manager::WorkerCommandAction,
    worker_id: u8,
) -> crate::manager::WorkerCommand {
    crate::manager::WorkerCommand {
        action,
        worker_id,
        origin_worker_id: 0,
        expected_worker_count: 0,
        target_worker_count: 0,
        command: crate::lifecycle::CommandKey::from_raw(0x2a).unwrap(),
    }
}

/// Two clicks on the same button are one operation; a different button while
/// it runs is refused with the operation that owns the slot, not queued behind
/// a drain that `shutdown_timeout: null` lets run for as long as it takes.
#[tokio::test]
async fn conflicting_command_is_rejected_with_the_active_operation() {
    use crate::lifecycle::{OperationKind, OperationScope, SubmitResult};
    use crate::manager::WorkerCommandAction;

    let supervisor =
        Arc::new(WorkerControl::new(Arc::new(HeartbeatObserver::new())));
    configure(&supervisor, "shutdown: { timeout: null }");
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("management-test".into()),
        ".",
    );
    for (id, child) in [
        (1, child_that_ignores_sigterm()),
        (2, Command::new("/bin/sleep").arg("60").spawn().unwrap()),
    ] {
        supervisor.workers.write().await.push(WorkerProcess::new(
            WorkerId::new(id),
            Some(
                OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
                    .unwrap(),
            ),
            WorkerPhase::Running,
            WorkerIntent::EnsureRunning,
        ));
    }

    let (cancel, runner) = operation_runner(supervisor.clone(), &state).await;
    let SubmitResult::Accepted(stop) = supervisor.submit_manager_command(
        &state,
        slot_command(WorkerCommandAction::Stop, 1),
    ) else {
        panic!("first stop must be accepted");
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let active = supervisor
        .operations
        .slot_operation(WorkerId::new(1))
        .expect("stop still runs");
    assert_eq!(active.id, stop);
    assert_eq!(active.kind, OperationKind::Stop);
    assert_eq!(active.scope, OperationScope::Slot(WorkerId::new(1)));

    assert_eq!(
        supervisor.submit_manager_command(
            &state,
            slot_command(WorkerCommandAction::Stop, 1)
        ),
        SubmitResult::AlreadyInProgress(active),
        "the same command again joins the running operation"
    );
    assert_eq!(
        supervisor.submit_manager_command(
            &state,
            slot_command(WorkerCommandAction::Restart, 1)
        ),
        SubmitResult::Conflict(active)
    );
    let restart_all = crate::manager::WorkerCommand {
        action: WorkerCommandAction::RestartAll,
        worker_id: 0,
        origin_worker_id: 0,
        expected_worker_count: 0,
        target_worker_count: 0,
        command: crate::lifecycle::CommandKey::from_raw(0x2a).unwrap(),
    };
    assert_eq!(
        supervisor.submit_manager_command(&state, restart_all),
        SubmitResult::Conflict(active),
        "a fleet operation cannot start while a slot is owned"
    );
    // The draining slot is still visible with its PID all the while.
    let snapshot = supervisor.management_snapshot(&state).await;
    assert_eq!(snapshot.workers[0].lifecycle(), WorkerLifecycle::Stopping);
    assert!(snapshot.workers[0].process.is_some());

    // Ending the process completes the operation and frees the slot.
    supervisor.workers.write().await[0]
        .process
        .as_mut()
        .unwrap()
        .child_mut()
        .kill()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while supervisor.operations.slot_operation(WorkerId::new(1)).is_some() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the stop operation releases the slot once the process is gone");
    assert!(supervisor.workers.read().await[0].is_stopped());
    let mut workers = supervisor.workers.write().await;
    let child = workers[1].process.as_mut().unwrap().child_mut();
    child.kill().unwrap();
    child.wait().unwrap();
    drop(workers);
    cancel.cancel();
    runner.await.unwrap().unwrap();
}

/// The incident's second symptom: workers that exited because the master was
/// shutting down were counted as crashes and queued for restart, because the
/// latch was set only after a lock a drain held. The latch now comes first,
/// with the intents, so the same exits are planned and nothing new starts.
#[tokio::test]
async fn shutdown_latch_is_visible_before_the_control_event_is_published() {
    use crate::lifecycle::{FleetMode, SubmitResult};
    use crate::manager::WorkerCommandAction;

    let supervisor =
        Arc::new(WorkerControl::new(Arc::new(HeartbeatObserver::new())));
    configure(&supervisor, "shutdown: { timeout: null }");
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("management-test".into()),
        ".",
    );
    for id in [1, 2] {
        let child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
        supervisor.workers.write().await.push(WorkerProcess::new(
            WorkerId::new(id),
            Some(
                OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
                    .unwrap(),
            ),
            WorkerPhase::Running,
            WorkerIntent::EnsureRunning,
        ));
    }
    assert_eq!(supervisor.fleet_mode(), FleetMode::Running);

    // The first thing `initiate_shutdown` does, before it emits or publishes.
    assert!(supervisor.begin_fleet_shutdown().await);
    assert_eq!(supervisor.fleet_mode(), FleetMode::ShuttingDown);
    assert!(
        !supervisor.begin_fleet_shutdown().await,
        "the latch is committed once"
    );
    for worker in supervisor.workers.read().await.iter() {
        assert_eq!(
            worker.intent(),
            WorkerIntent::Drain {
                reason: DrainReason::Shutdown,
                after_exit: ExitDisposition::FinishFleetShutdown
            }
        );
        assert!(
            worker.pid().is_some(),
            "the slots keep their processes and PIDs"
        );
        assert_eq!(worker.lifecycle(), WorkerLifecycle::Stopping);
    }
    assert!(matches!(
        supervisor.submit_manager_command(
            &state,
            slot_command(WorkerCommandAction::Stop, 1)
        ),
        SubmitResult::Rejected(_)
    ));

    // A worker that leaves now — as its drain request will make it — is
    // a planned exit, not a crash to restart. The observer sees it go first;
    // the health pass then reaps and classifies it.
    let exit = {
        let mut workers = supervisor.workers.write().await;
        let process = workers[1].process.as_mut().unwrap();
        process.child_mut().kill().unwrap();
        process.exit()
    };
    tokio::time::timeout(Duration::from_secs(2), exit.exited()).await.unwrap();
    let (alive, dead) = supervisor.check_worker_health().await;
    assert!(
        dead.is_empty(),
        "an exit after the latch is never a crash: {dead:?}"
    );
    assert!(alive.is_empty(), "a draining slot is not capacity");
    let workers = supervisor.workers.read().await;
    assert_eq!(workers[1].lifecycle(), WorkerLifecycle::Stopped);
    assert_eq!(workers[0].lifecycle(), WorkerLifecycle::Stopping);
    drop(workers);
    let mut workers = supervisor.workers.write().await;
    let child = workers[0].process.as_mut().unwrap().child_mut();
    child.kill().unwrap();
    child.wait().unwrap();
}

/// The 2026-09-13 sequence: a rolling restart is draining a worker that does
/// not answer SIGTERM when full shutdown arrives. The latch must show at once,
/// the draining PID must stay in the snapshot, the drain must finish as the
/// shutdown's own with no replacement spawned, and the fleet must not be
/// reported exited while that worker lives.
#[tokio::test]
async fn shutdown_preempts_a_rolling_restart_whose_worker_ignores_sigterm() {
    use crate::lifecycle::{FleetMode, OperationKind, SubmitResult};
    use crate::manager::{WorkerCommand, WorkerCommandAction};

    let supervisor =
        Arc::new(WorkerControl::new(Arc::new(HeartbeatObserver::new())));
    configure(
        &supervisor,
        "shutdown: { timeout: null }\nstartup: { timeout: 5s }",
    );
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("management-test".into()),
        ".",
    );
    let mut identities = Vec::new();
    for (id, child) in [
        (1, child_that_ignores_sigterm()),
        (2, Command::new("/bin/sleep").arg("60").spawn().unwrap()),
    ] {
        let process =
            OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
                .unwrap();
        identities.push(process.identity());
        supervisor.workers.write().await.push(WorkerProcess::new(
            WorkerId::new(id),
            Some(process),
            WorkerPhase::Running,
            WorkerIntent::EnsureRunning,
        ));
    }

    let (cancel, runner) = operation_runner(supervisor.clone(), &state).await;
    let restart_all = WorkerCommand {
        action: WorkerCommandAction::RestartAll,
        worker_id: 0,
        origin_worker_id: 0,
        expected_worker_count: 0,
        target_worker_count: 0,
        command: crate::lifecycle::CommandKey::from_raw(0x2a).unwrap(),
    };
    assert!(matches!(
        supervisor.submit_manager_command(&state, restart_all),
        SubmitResult::Accepted(_)
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let rolling = supervisor.operations.active();
    assert_eq!(rolling.len(), 1);
    assert_eq!(rolling[0].kind, OperationKind::RestartAll);
    {
        let workers = supervisor.workers.read().await;
        assert_eq!(
            workers[0].intent(),
            WorkerIntent::Drain {
                reason: DrainReason::RollingRestart,
                after_exit: ExitDisposition::Replace
            },
            "worker #1 is being drained by the rolling restart"
        );
        assert_eq!(workers[0].pid(), Some(identities[0].pid()));
        assert!(workers[1].is_running());
    }

    // Full shutdown arrives while the drain waits on a worker that ignores SIGTERM.
    assert!(supervisor.begin_fleet_shutdown().await);
    assert_eq!(
        supervisor.fleet_mode(),
        FleetMode::ShuttingDown,
        "the latch is visible at once"
    );
    {
        let workers = supervisor.workers.read().await;
        assert_eq!(
            workers[0].intent(),
            WorkerIntent::Drain {
                reason: DrainReason::RollingRestart,
                after_exit: ExitDisposition::FinishFleetShutdown
            },
            "the in-flight drain keeps its reason and loses its replacement"
        );
        assert_eq!(
            workers[1].intent(),
            WorkerIntent::Drain {
                reason: DrainReason::Shutdown,
                after_exit: ExitDisposition::FinishFleetShutdown
            }
        );
    }
    let snapshot = supervisor.management_snapshot(&state).await;
    assert_eq!(
        snapshot.workers[0].pid(),
        Some(identities[0].pid()),
        "the draining PID stays visible"
    );
    assert_eq!(snapshot.workers[0].lifecycle(), WorkerLifecycle::Stopping);

    // The fleet is not exited while worker #1 lives, however long that takes.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            supervisor.wait_workers()
        )
        .await
        .is_err(),
        "wait_workers must not return while an owned identity is alive"
    );

    // The workers leave, as their drain requests make them.
    for index in [1, 0] {
        supervisor.workers.write().await[index]
            .process
            .as_mut()
            .unwrap()
            .child_mut()
            .kill()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), supervisor.wait_workers())
        .await
        .expect("every owned identity is gone");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !supervisor.operations.active().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the rolling restart ends without a replacement");

    let workers = supervisor.workers.read().await;
    assert_eq!(workers.len(), 2);
    for (worker, identity) in workers.iter().zip(&identities) {
        assert_eq!(
            worker.process.as_ref().map(OwnedProcess::identity),
            Some(*identity),
            "no replacement was spawned for worker #{}",
            worker.id
        );
        assert!(
            worker.is_stopped(),
            "worker #{} rests after the fleet shutdown",
            worker.id
        );
        assert_eq!(worker.lifecycle(), WorkerLifecycle::Stopped);
    }
    drop(workers);
    cancel.cancel();
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn blocked_shutdown_report_names_every_live_identity() {
    use crate::lifecycle::{OperationKind, OperationScope};

    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    configure(&supervisor, "shutdown: { timeout: null }");
    let live = child_that_ignores_sigterm();
    let live =
        OwnedProcess::from_child(live, Arc::clone(&supervisor.exited)).unwrap();
    let identity = live.identity();
    let mut gone = OwnedProcess::from_child(
        Command::new("/bin/sleep").arg("60").spawn().unwrap(),
        Arc::clone(&supervisor.exited),
    )
    .unwrap();
    gone.child_mut().kill().unwrap();
    gone.child_mut().wait().unwrap();
    assert!(gone.try_reap().unwrap().is_some());
    let drain = WorkerIntent::Drain {
        reason: DrainReason::RollingRestart,
        after_exit: ExitDisposition::FinishFleetShutdown,
    };
    supervisor.workers.write().await.extend([
        WorkerProcess::new(
            WorkerId::new(1),
            Some(live),
            WorkerPhase::Running,
            drain,
        )
        .with_incarnation(3),
        WorkerProcess::new(
            WorkerId::new(2),
            Some(gone),
            WorkerPhase::Exited { outcome: ExitOutcome::Planned },
            WorkerIntent::Stopped,
        ),
    ]);
    let guard = supervisor
        .operations
        .try_acquire(
            OperationScope::Slot(WorkerId::new(1)),
            OperationKind::RestartAll,
            crate::lifecycle::OperationOrigin::Signal,
        )
        .unwrap();

    let blocked = supervisor.blocked_workers().await;
    assert_eq!(blocked.len(), 1, "only the live identity blocks: {blocked:?}");
    assert_eq!(blocked[0].worker_id, WorkerId::new(1));
    assert_eq!(blocked[0].identity, identity);
    assert_eq!(blocked[0].phase, WorkerPhase::Running);
    assert_eq!(blocked[0].intent, drain);
    assert_eq!(blocked[0].operation, Some(guard.operation()));
    let line = blocked[0].to_string();
    assert!(line.contains(&format!("pid={}", identity.pid())), "{line}");
    assert!(
        line.contains(&format!("start_stamp={}", identity.started_at())),
        "{line}"
    );
    assert!(line.contains("RollingRestart"), "{line}");
    assert!(line.contains("restart-workers"), "{line}");

    drop(guard);
    let mut workers = supervisor.workers.write().await;
    let child = workers[0].process.as_mut().unwrap().child_mut();
    child.kill().unwrap();
    child.wait().unwrap();
}

/// The drain protocol's three facts are three phases. None of them is the
/// exit, and the exit is classified from the intent as before.
#[tokio::test]
async fn acknowledgement_completion_and_exit_are_three_distinct_states() {
    use crate::lifecycle::{
        OperationKind, OperationScope, TeardownOutcome, WorkerIdentity,
    };
    use crate::shutdown::{WorkerDrainAcknowledged, WorkerTeardownCompleted};

    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    configure(&supervisor, "shutdown: { timeout: null }");
    let master = supervisor.master().unwrap();
    let child = child_that_ignores_sigterm();
    let process =
        OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
            .unwrap();
    supervisor.workers.write().await.push(
        WorkerProcess::new(
            WorkerId::new(1),
            Some(process),
            WorkerPhase::Running,
            WorkerIntent::Drain {
                reason: DrainReason::ManualStop,
                after_exit: ExitDisposition::StayStopped,
            },
        )
        .with_incarnation(2),
    );
    let identity = supervisor.workers.read().await[0].identity(master).unwrap();
    let stop = supervisor
        .operations
        .try_acquire(
            OperationScope::Slot(WorkerId::new(1)),
            OperationKind::Stop,
            crate::lifecycle::OperationOrigin::Supervisor,
        )
        .unwrap();
    let operation = stop.operation().id;
    let other_operation =
        crate::lifecycle::OperationId::from_raw(operation.raw() + 1).unwrap();
    let other_identity = WorkerIdentity::new(
        identity.worker_id(),
        identity.process(),
        identity.incarnation() + 1,
        identity.master(),
    );

    // Wrong operation or wrong identity: nothing changes.
    assert!(
        !supervisor
            .drain_acknowledged(&WorkerDrainAcknowledged {
                operation: other_operation,
                worker: identity
            })
            .await
    );
    assert!(
        !supervisor
            .drain_acknowledged(&WorkerDrainAcknowledged {
                operation,
                worker: other_identity
            })
            .await
    );
    assert_eq!(
        supervisor.workers.read().await[0].phase(),
        WorkerPhase::Running
    );

    assert!(
        supervisor
            .drain_acknowledged(&WorkerDrainAcknowledged {
                operation,
                worker: identity
            })
            .await
    );
    {
        let workers = supervisor.workers.read().await;
        assert_eq!(workers[0].phase(), WorkerPhase::DrainAcknowledged);
        assert!(workers[0].phase().is_live());
        assert_eq!(workers[0].lifecycle(), WorkerLifecycle::Stopping);
        assert_eq!(workers[0].pid(), Some(identity.process().pid()));
    }

    let completed = WorkerTeardownCompleted {
        operation: Some(operation),
        worker: identity,
        outcome: TeardownOutcome::Completed,
    };
    assert!(
        !supervisor
            .teardown_completed(&WorkerTeardownCompleted {
                operation: Some(other_operation),
                ..completed
            })
            .await
    );
    assert!(supervisor.teardown_completed(&completed).await);
    assert!(
        !supervisor.teardown_completed(&completed).await,
        "a repeat changes nothing"
    );
    {
        let workers = supervisor.workers.read().await;
        assert_eq!(workers[0].phase(), WorkerPhase::TeardownCompleted);
        assert!(
            workers[0].phase().is_live(),
            "teardown completion is not process exit"
        );
        assert_eq!(workers[0].lifecycle(), WorkerLifecycle::Stopping);
        assert!(identity.process().is_alive());
    }

    // Only the kernel ends it, and the exit is classified from the intent.
    drop(stop);
    supervisor.workers.write().await[0]
        .process
        .as_mut()
        .unwrap()
        .child_mut()
        .kill()
        .unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        supervisor.await_slot_exit(
            WorkerId::new(1),
            identity.process(),
            "test",
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(outcome, ExitOutcome::Planned);
    let workers = supervisor.workers.read().await;
    assert_eq!(
        workers[0].phase(),
        WorkerPhase::Exited { outcome: ExitOutcome::Planned }
    );
    assert_eq!(workers[0].lifecycle(), WorkerLifecycle::Stopped);
}

/// The snapshot carries the two facts a slot's label is derived from, how long
/// each has stood, the operation acting on the slot, the exact process, and —
/// for a slot boot could not reconcile — why. Enough to explain a blocked
/// fleet from a client without the master's log.
#[tokio::test]
async fn snapshot_carries_lifecycle_detail() {
    use claviron_orbit::OrbitEpoch;
    use claviron_sys::process::ProcessIdentity;

    use crate::lifecycle::{
        CommandKey, FleetMode, MasterIdentity, OperationKind, OperationOrigin,
        OperationScope, WorkerBlockReason, WorkerIdentity, WorkerObservedPhase,
    };
    use crate::presence::{PresencePhase, WorkerPresence};
    use crate::snapshot::WorkerOwnership;

    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    configure(&supervisor, "shutdown: { timeout: null }");
    let master = supervisor.master().unwrap();
    let state = master_state(2);

    // Slot 1: a spawned process whose manual stop the worker has acknowledged.
    let process = OwnedProcess::from_child(
        Command::new("/bin/sleep").arg("60").spawn().unwrap(),
        Arc::clone(&supervisor.exited),
    )
    .unwrap();
    let identity = process.identity();
    let mut draining = WorkerProcess::new(
        WorkerId::new(1),
        Some(process),
        WorkerPhase::Running,
        WorkerIntent::EnsureRunning,
    )
    .with_incarnation(3);
    draining.begin_drain(DrainReason::ManualStop, ExitDisposition::StayStopped);
    draining.set_phase(WorkerPhase::DrainAcknowledged);
    let command = CommandKey::from_raw(0x0200_0000_0000_0001).unwrap();
    let stop = supervisor
        .operations
        .try_acquire(
            OperationScope::Slot(WorkerId::new(1)),
            OperationKind::Stop,
            OperationOrigin::Management {
                via_worker: Some(WorkerId::new(2)),
                command,
            },
        )
        .unwrap();

    // Slot 2: presence of a worker whose old master is still alive; nothing is
    // spawned over it and the snapshot must say why.
    let foreign_master =
        MasterIdentity::new(ProcessIdentity::new(4_242, 0x0102), 1);
    let presence = WorkerPresence::new(
        WorkerIdentity::new(
            WorkerId::new(2),
            ProcessIdentity::new(31_337, 0x0304),
            5,
            foreign_master,
        ),
        1_757_780_000_000,
        PresencePhase::Running,
        4_242,
        None,
        OrbitEpoch::now(),
    );
    let blocked = WorkerProcess::new(
        WorkerId::new(2),
        None,
        WorkerPhase::ReconciliationBlocked {
            presence,
            reason: WorkerBlockReason::MasterStillAlive,
        },
        WorkerIntent::EnsureRunning,
    )
    .with_incarnation(0);
    supervisor.workers.write().await.extend([draining, blocked]);

    let snapshot = supervisor.management_snapshot(&state).await;
    assert_eq!(snapshot.master, Some(master));
    assert_eq!(snapshot.fleet_mode, FleetMode::Running);
    assert_eq!(snapshot.workers.len(), 2);

    let first = &snapshot.workers[0];
    assert_eq!(first.lifecycle(), WorkerLifecycle::Stopping);
    assert_eq!(first.ownership, WorkerOwnership::Spawned);
    assert_eq!(
        first.process,
        Some(identity),
        "pid and start stamp name the exact process"
    );
    assert_eq!(first.phase, WorkerObservedPhase::DrainAcknowledged);
    assert_eq!(
        first.desired,
        WorkerIntent::Drain {
            reason: DrainReason::ManualStop,
            after_exit: ExitDisposition::StayStopped
        }
    );
    assert!(
        first.phase_age <= first.desired_age,
        "the drain was wanted before it was acknowledged"
    );
    let operation = first.operation.expect("the stop owns the slot");
    assert_eq!(operation.id, stop.operation().id);
    assert_eq!(operation.kind, OperationKind::Stop);
    assert_eq!(
        operation.origin,
        OperationOrigin::Management {
            via_worker: Some(WorkerId::new(2)),
            command
        },
        "the client finds its command by the key it submitted"
    );
    assert_eq!(first.heartbeat_age, None, "no beat was ever accepted");
    assert_eq!(first.block_reason, None);

    let second = &snapshot.workers[1];
    assert_eq!(second.lifecycle(), WorkerLifecycle::Unclaimed);
    assert_eq!(second.ownership, WorkerOwnership::Unowned, "no handle is held");
    assert_eq!(
        second.process,
        Some(ProcessIdentity::new(31_337, 0x0304)),
        "but the process that blocks boot is named, PID and start stamp"
    );
    assert_eq!(second.incarnation, 5, "and so is its incarnation");
    assert!(second.uptime.is_some());
    assert_eq!(second.phase, WorkerObservedPhase::ReconciliationBlocked);
    assert_eq!(second.block_reason, Some(WorkerBlockReason::MasterStillAlive));
    assert_eq!(second.operation, None);

    // The fleet latch shows up in the header and takes the drain over: the
    // stop still owns the slot, but what acts on it now is the shutdown.
    assert!(supervisor.begin_fleet_shutdown().await);
    let snapshot = supervisor.management_snapshot(&state).await;
    assert_eq!(snapshot.fleet_mode, FleetMode::ShuttingDown);
    assert!(
        supervisor.operations.slot_operation(WorkerId::new(1)).is_some(),
        "the stop still runs"
    );
    drop(stop);
    assert_eq!(
        snapshot.workers[0].desired,
        WorkerIntent::Drain {
            reason: DrainReason::ManualStop,
            after_exit: ExitDisposition::FinishFleetShutdown
        }
    );
    let shutdown =
        snapshot.workers[0].operation.expect("shutdown acts on the slot");
    assert_eq!(shutdown.kind, OperationKind::FleetShutdown);
    assert_eq!(shutdown.origin, OperationOrigin::Signal);

    let mut workers = supervisor.workers.write().await;
    let child = workers[0].process.as_mut().unwrap().child_mut();
    child.kill().unwrap();
    child.wait().unwrap();
}
