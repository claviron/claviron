//! Runtime/service-manager regressions; never joins the live `clv` fleet.
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use claviron_runtime::registry::{Component, Finalizable};
use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, Runtime, WorkerCount,
    WorkerId,
};

use crate::config::SupervisorConfig;
use crate::control::WorkerControl;
use crate::lifecycle::{
    OperationKind, OperationOrigin, OperationScope, Refusal, RejectionReason,
    WorkerIntent, WorkerPhase,
};
use crate::management::runner::WorkerOperationRunner;
use crate::management::submit;
use crate::monitor::service::WorkerMonitor;
use crate::observer::HeartbeatObserver;

fn fixture() -> (AppState, Arc<WorkerControl>) {
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("generation-test".into()),
        ".",
    );
    let control =
        Arc::new(WorkerControl::new(Arc::new(HeartbeatObserver::new())));
    control.config.store(Some(Arc::new(SupervisorConfig::default())));
    control.establish_master_for_tests();
    (state, control)
}

#[tokio::test]
async fn startup_ack_waits_for_the_spawn_installation_boundary() {
    let (_, control) = fixture();
    let mut rows = control.workers.write().await;
    let child =
        std::process::Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let process = crate::lifecycle::OwnedProcess::from_child(
        child,
        control.exited.clone(),
    )
    .unwrap();
    let event = crate::startup::WorkerStarted {
        worker_id: WorkerId::new(1),
        pid: process.pid(),
    };
    let acknowledgement =
        crate::lifecycle::startup::confirm_worker_started(&control, event);
    tokio::pin!(acknowledgement);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), &mut acknowledgement)
            .await
            .is_err()
    );
    rows.push(crate::fleet::WorkerProcess::new(
        WorkerId::new(1),
        Some(process),
        WorkerPhase::Starting,
        WorkerIntent::EnsureRunning,
    ));
    drop(rows);
    assert!(acknowledgement.await);
    let mut rows = control.workers.write().await;
    assert_eq!(rows[0].phase(), WorkerPhase::Running);
    let child = rows[0].process.as_mut().unwrap().child_mut();
    child.kill().unwrap();
    child.wait().unwrap();
}

#[tokio::test]
async fn rejected_policy_does_not_open_a_recovery_epoch_or_queue_worker_reload()
{
    use claviron_runtime::registry::Reloadable;
    let (state, control) = fixture();
    state
        .registry_ref()
        .register(Arc::new(
            crate::config::provider::SupervisorPolicy::new(
                control.config.clone(),
                &state,
            )
            .unwrap(),
        ))
        .unwrap();
    control.workers.write().await.push(
        crate::fleet::WorkerProcess::new(
            WorkerId::new(1),
            None,
            WorkerPhase::Exited {
                outcome: crate::lifecycle::ExitOutcome::Failed,
            },
            WorkerIntent::EnsureRunning,
        )
        .with_recovery(10, Some(std::time::Instant::now())),
    );
    let reload =
        crate::management::configuration::WorkerConfigurationReload::new(
            control.clone(),
        );
    let error = reload.reload(&state).await.unwrap_err();
    assert!(error.to_string().contains("policy was rejected"));
    assert_eq!(control.workers.read().await[0].recovery_attempts, 10);
    assert!(control.operations.active().is_empty());
}

#[tokio::test]
async fn manager_stop_restart_monitor_preserves_workers_and_open_operations() {
    let (state, control) = fixture();
    // A child handle belongs to the fleet, not to the monitor generation.
    let child =
        std::process::Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let process = crate::lifecycle::OwnedProcess::from_child(
        child,
        control.exited.clone(),
    )
    .unwrap();
    let identity = process.identity();
    control.workers.write().await.push(
        crate::fleet::WorkerProcess::new(
            WorkerId::new(1),
            Some(process),
            WorkerPhase::Running,
            WorkerIntent::EnsureRunning,
        )
        .with_incarnation(17),
    );
    control.startup_completed.store(true, Ordering::Release);
    state
        .registry_ref()
        .register(Arc::new(WorkerMonitor::new(control.clone())))
        .unwrap();
    state
        .registry_ref()
        .register(Arc::new(WorkerOperationRunner::new(control.clone())))
        .unwrap();
    let mut runtime = Runtime::default();
    let manager = runtime.manager();
    runtime.spawn_all(state.registry_ref(), state.clone()).unwrap();
    let runtime_state = state.clone();
    let run = tokio::spawn(async move {
        runtime.wait_until_shutdown(&runtime_state).await.unwrap();
        runtime.drain().await.unwrap();
    });
    tokio::task::yield_now().await;

    let operation = submit::acquire(
        &control,
        OperationScope::Slot(WorkerId::new(1)),
        OperationKind::Stop,
        OperationOrigin::Supervisor,
    )
    .unwrap();
    let key = operation.operation().id;
    let (release, released) = tokio::sync::oneshot::channel();
    submit::enqueue_operation(&control, operation, async move {
        released.await.unwrap();
        Ok(())
    })
    .unwrap();
    for generation in 1..=3 {
        let stopped = manager.stop("WorkerMonitor").await.unwrap();
        assert_eq!(stopped.generation(), generation);
        assert!(!control.is_shutting_down());
        assert!(!state.is_shutting_down());
        let rows = control.workers.read().await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].process.as_ref().unwrap().identity(), identity);
        assert_eq!(rows[0].incarnation, 17);
        assert_eq!(rows[0].intent(), WorkerIntent::EnsureRunning);
        assert_eq!(
            control.operations.slot_operation(WorkerId::new(1)).unwrap().id,
            key
        );
        drop(rows);
        manager.start("WorkerMonitor").await.unwrap();
    }
    release.send(()).unwrap();
    manager.stop("WorkerOperationRunner").await.unwrap();
    assert!(control.operations.active().is_empty());
    state.initiate_shutdown();
    run.await.unwrap();
    let mut rows = control.workers.write().await;
    let child = rows[0].process.as_mut().unwrap().child_mut();
    child.kill().unwrap();
    child.wait().unwrap();
}

#[tokio::test]
async fn runner_stop_drains_accepted_work_before_the_next_generation() {
    let (state, control) = fixture();
    state
        .registry_ref()
        .register(Arc::new(WorkerOperationRunner::new(control.clone())))
        .unwrap();
    let mut runtime = Runtime::default();
    let manager = runtime.manager();
    runtime.spawn_all(state.registry_ref(), state.clone()).unwrap();
    let runtime_state = state.clone();
    let run = tokio::spawn(async move {
        runtime.wait_until_shutdown(&runtime_state).await.unwrap();
        runtime.drain().await.unwrap();
    });
    tokio::task::yield_now().await;
    let guard = submit::acquire(
        &control,
        OperationScope::Slot(WorkerId::new(1)),
        OperationKind::Rotation,
        OperationOrigin::Supervisor,
    )
    .unwrap();
    assert!(matches!(
        submit::acquire(
            &control,
            OperationScope::Slot(WorkerId::new(1)),
            OperationKind::CrashRecovery,
            OperationOrigin::Supervisor
        ),
        Err(Refusal::Conflict(_))
    ));
    let (release, released) = tokio::sync::oneshot::channel();
    let (done, mut completed) = tokio::sync::oneshot::channel();
    submit::enqueue_operation(&control, guard, async move {
        released.await.unwrap();
        done.send(()).unwrap();
        Ok(())
    })
    .unwrap();
    let stopping = manager.stop("WorkerOperationRunner");
    tokio::pin!(stopping);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut stopping)
            .await
            .is_err()
    );
    assert!(completed.try_recv().is_err());
    assert_eq!(control.operations.active().len(), 1);
    assert_eq!(
        control.operation_queue.submit(Box::pin(async {})),
        Err(RejectionReason::OperationRunnerStopped)
    );
    release.send(()).unwrap();
    stopping.await.unwrap();
    completed.await.unwrap();
    assert!(control.operations.active().is_empty());
    let started = manager.start("WorkerOperationRunner").await.unwrap();
    assert_eq!(started.generation(), 2);
    state.initiate_shutdown();
    run.await.unwrap();
}

#[tokio::test]
async fn finalization_does_not_signal_or_drop_live_owned_workers() {
    let (state, control) = fixture();
    let child =
        std::process::Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let process = crate::lifecycle::OwnedProcess::from_child(
        child,
        control.exited.clone(),
    )
    .unwrap();
    let identity = process.identity();
    control.workers.write().await.push(crate::fleet::WorkerProcess::new(
        WorkerId::new(1),
        Some(process),
        WorkerPhase::Running,
        WorkerIntent::EnsureRunning,
    ));
    let owner =
        crate::lifecycle::master::MasterIdentityProvider::new(control.clone());
    assert!(owner.finalize(&state).await.is_err());
    assert!(identity.is_alive());
    assert_eq!(control.workers.read().await.len(), 1);
    {
        let mut rows = control.workers.write().await;
        let child = rows[0].process.as_mut().unwrap().child_mut();
        child.kill().unwrap();
        child.wait().unwrap();
    }
    owner.finalize(&state).await.unwrap();
    owner.finalize(&state).await.unwrap();
    assert!(control.workers.read().await.is_empty());
    assert!(control.clone().as_runnable().is_none());
}
