use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use claviron_orbit::{EVENT_PAYLOAD_MAX, FleetEventCodec, FleetEventRegistry};
use claviron_runtime::registry::Runnable;
use claviron_runtime::{
    EventDispatcher, FleetCapacity, FleetNodeId, ProcessStarted, RunContext,
    RunMode, WorkerCount, WorkerId,
};
use claviron_sys::process::ProcessIdentity;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use super::*;
use crate::WorkersBootConfirmed;
use crate::lifecycle::{LaneWatch, OwnedProcess, WorkerPhase};
use crate::presence::WorkerPresence;
use crate::startup::{self, WorkerStarted};

// These tests use local events and real, isolated child handles. They never
// boot the production fleet.
struct TestWorkers {
    supervisor: Arc<WorkerControl>,
    state: AppState,
}

impl TestWorkers {
    async fn new(count: u8) -> Self {
        let state = AppState::initialize(
            RunMode::Master,
            FleetNodeId::ZERO,
            WorkerCount::new(count),
            FleetCapacity::new(u16::from(count) + 1).unwrap(),
            Some("startup-test".into()),
            ".",
        );
        let observer = Arc::new(HeartbeatObserver::new());
        let supervisor = Arc::new(WorkerControl::new(observer));
        for id in 1..=count {
            let child = Command::new("/bin/sleep")
                .arg("60")
                .spawn()
                .expect("spawn test child");
            let process =
                OwnedProcess::from_child(child, Arc::clone(&supervisor.exited))
                    .expect("test child identity");
            supervisor.workers.write().await.push(WorkerProcess::new(
                WorkerId::new(id),
                Some(process),
                WorkerPhase::Starting,
                WorkerIntent::EnsureRunning,
            ));
        }
        Self { supervisor, state }
    }

    async fn event(
        &self,
        id: u8,
    ) -> WorkerStarted {
        let workers = self.supervisor.workers.read().await;
        let worker = workers
            .iter()
            .find(|worker| worker.id.raw() == id)
            .expect("tracked test child");
        WorkerStarted {
            worker_id: WorkerId::new(id),
            pid: worker.pid().expect("tracked test child"),
        }
    }
}

impl Drop for TestWorkers {
    fn drop(&mut self) {
        let mut workers = self
            .supervisor
            .workers
            .try_write()
            .expect("test child table unlocked");
        // A slot may legitimately hold no child — see `a_slot_under_watch_is_not_a_dead_worker`.
        for child in workers
            .iter_mut()
            .filter_map(|worker| worker.process.as_mut())
            .map(OwnedProcess::child_mut)
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A slot whose lane is still being watched holds no child, by design. Reading
/// that absence as a dead worker cost the master every restart made inside
/// `heartbeat_timeout`: the startup wait sampled health before the loop that
/// was supposed to retry the slot had run once, found a worker it had never
/// spawned "exited early", and aborted the boot. Worse, it rewrote the slot as
/// `Crashed` on the way out, so the retry could not have found it afterwards
/// either, and the recovery budget an unclaimed slot never touches paid for it.
#[tokio::test]
async fn a_slot_under_watch_is_not_a_dead_worker() {
    let test = TestWorkers::new(1).await;
    let watching = WorkerPhase::Unclaimed { watch: LaneWatch::starting_at(7) };
    test.supervisor.workers.write().await.push(
        WorkerProcess::new(
            WorkerId::new(2),
            None,
            watching,
            WorkerIntent::EnsureRunning,
        )
        .with_incarnation(0),
    );

    let (alive, dead) = test.supervisor.check_worker_health().await;

    assert!(dead.is_empty(), "a slot under watch was reported dead: {dead:?}");
    assert!(
        alive.iter().all(|worker| worker.id() != WorkerId::new(2)),
        "a slot under watch was counted as serving capacity"
    );
    assert_eq!(
        test.supervisor.workers.read().await[1].phase(),
        watching,
        "the health check overwrote the watch"
    );
}

#[tokio::test]
async fn shutdown_completion_is_observed_after_monitor_stop_without_reaping_the_child()
 {
    use crate::lifecycle::TeardownOutcome;
    use crate::shutdown::{self, WorkerTeardownCompleted};

    let test = TestWorkers::new(1).await;
    let master = test.supervisor.establish_master_for_tests();
    let started = test.event(1).await;
    assert!(test.supervisor.confirm_worker_started(started).await);
    let identity =
        test.supervisor.workers.read().await[0].identity(master).unwrap();
    test.state
        .registry_ref()
        .register(test.supervisor.observer.clone())
        .expect("register HeartbeatObserver");
    test.state
        .registry_ref()
        .register(Arc::new(FleetEventRegistry::new()))
        .expect("register FleetEventRegistry");
    test.state
        .registry_ref()
        .register(Arc::clone(&test.supervisor))
        .expect("register WorkerControl");
    shutdown::install_events(&test.state).unwrap();
    let dispatcher = tokio::spawn(
        test.state.registry_ref().resolve::<EventDispatcher>().unwrap().run(
            (),
            test.state.clone(),
            RunContext::new(test.state.shutdown_token()),
        ),
    );
    assert!(test.supervisor.begin_fleet_shutdown().await);
    test.state.events().emit(WorkerTeardownCompleted {
        operation: None,
        worker: identity,
        outcome: TeardownOutcome::Completed,
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !test.supervisor.observer.snapshot().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!test.state.is_shutting_down());
    let mut workers = test.supervisor.workers.write().await;
    assert!(workers[0].process.as_mut().unwrap().try_reap().unwrap().is_none());
    assert_eq!(
        workers[0].phase(),
        WorkerPhase::TeardownCompleted,
        "completion is not exit"
    );
    drop(workers);
    test.state.initiate_shutdown();
    dispatcher.await.unwrap().unwrap();
}

#[tokio::test]
async fn killed_child_is_reaped_without_a_shutdown_completion_event() {
    let test = TestWorkers::new(1).await;
    let mut completed =
        test.state
            .events()
            .subscribe::<crate::shutdown::WorkerTeardownCompleted>();
    test.supervisor.workers.write().await[0]
        .process
        .as_mut()
        .unwrap()
        .child_mut()
        .kill()
        .unwrap();
    test.supervisor.wait_workers().await;
    assert!(
        test.supervisor.workers.write().await[0]
            .process
            .as_mut()
            .unwrap()
            .try_reap()
            .unwrap()
            .is_some()
    );
    assert!(matches!(completed.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn worker_started_codec_is_exact_and_bounded() {
    let event = WorkerStarted { worker_id: WorkerId::new(2), pid: 0x12345678 };
    let payload = event.encode_orbit();
    assert_eq!(payload, [2, 0x12, 0x34, 0x56, 0x78]);
    assert_eq!(WorkerStarted::decode_orbit(&payload).unwrap(), event);
    assert!(
        12 + WorkerStarted::TOPIC.len() + payload.len() <= EVENT_PAYLOAD_MAX
    );
    for invalid in [
        &[][..],
        &[1, 2, 3, 4],
        &[0, 0, 0, 0, 1],
        &[1, 0, 0, 0, 0],
        &[1, 0, 0, 0, 1, 0],
    ] {
        assert!(WorkerStarted::decode_orbit(invalid).is_err());
    }
}

#[tokio::test]
async fn only_worker_adapts_local_identity_free_startup() {
    for (mode, node_id, worker_count, fleet_capacity) in [
        (
            RunMode::Master,
            FleetNodeId::ZERO,
            WorkerCount::new(2),
            FleetCapacity::new(3).unwrap(),
        ),
        (
            RunMode::Standalone,
            FleetNodeId::ZERO,
            WorkerCount::ZERO,
            FleetCapacity::ONE,
        ),
        (
            RunMode::Worker(WorkerId::new(2)),
            FleetNodeId::new(2),
            WorkerCount::new(2),
            FleetCapacity::new(3).unwrap(),
        ),
    ] {
        let state = AppState::initialize(
            mode,
            node_id,
            worker_count,
            fleet_capacity,
            Some("startup-test".into()),
            ".",
        );
        state
            .registry_ref()
            .register(Arc::new(FleetEventRegistry::new()))
            .expect("register FleetEventRegistry");
        if let RunMode::Worker(worker_id) = state.run_mode() {
            state
                .registry_ref()
                .insert(Arc::new(crate::WorkerIdentityProvider::for_tests(
                    worker_id,
                )))
                .expect("register worker identity");
        }
        if state.is_master() {
            state
                .registry_ref()
                .register(Arc::new(WorkerControl::new(Arc::new(
                    HeartbeatObserver::new(),
                ))))
                .unwrap();
        }
        let mut wire = state.events().subscribe::<WorkerStarted>();
        let mut local = state.events().subscribe::<ProcessStarted>();
        startup::install_events(&state).unwrap();

        state.events().emit(ProcessStarted);
        assert_eq!(local.recv().await.unwrap(), ProcessStarted);
        assert_eq!(
            wire.try_recv(),
            Err(TryRecvError::Empty),
            "registration must not run callbacks"
        );
        let dispatcher = tokio::spawn(
            state.registry_ref().resolve::<EventDispatcher>().unwrap().run(
                (),
                state.clone(),
                RunContext::new(state.shutdown_token()),
            ),
        );
        if state.is_worker() {
            let event = wire.recv().await.unwrap();
            assert_eq!(event.worker_id, WorkerId::new(2));
            assert_eq!(event.pid, std::process::id());
        } else {
            tokio::task::yield_now().await;
            assert_eq!(wire.try_recv(), Err(TryRecvError::Empty));
        }
        // Neither remote envelopes nor duplicate local milestones synthesize
        // another process's local startup or republish this worker's boot.
        state
            .events()
            .emit(WorkerStarted { worker_id: WorkerId::new(1), pid: 42 });
        assert_eq!(wire.recv().await.unwrap().pid, 42);
        assert_eq!(local.try_recv(), Err(TryRecvError::Empty));
        state.events().emit(ProcessStarted);
        tokio::task::yield_now().await;
        assert_eq!(wire.try_recv(), Err(TryRecvError::Empty));
        state.initiate_shutdown();
        dispatcher.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn events_confirm_exact_child_set_without_heartbeat_or_config() {
    let test = TestWorkers::new(2).await;
    let mut started = test.state.events().subscribe::<WorkerStarted>();
    let mut confirmed = test.state.events().subscribe::<WorkersBootConfirmed>();
    let first = test.event(1).await;
    let second = test.event(2).await;
    test.state
        .events()
        .emit(WorkerStarted { pid: first.pid.wrapping_add(1), ..first });
    test.state
        .events()
        .emit(WorkerStarted { worker_id: WorkerId::new(99), ..first });
    test.state.events().emit(second);
    test.state.events().emit(second);
    test.state.events().emit(first);

    test.supervisor
        .wait_workers_started(
            &test.state,
            &mut started,
            &test.state.shutdown_token(),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert_eq!(confirmed.recv().await.unwrap().count, 2);
    assert!(matches!(confirmed.try_recv(), Err(TryRecvError::Empty)));
    let status = test.supervisor.observer.snapshot().await;
    assert_eq!(status.len(), 2);
    for event in [first, second] {
        let worker = &status[&event.worker_id];
        assert_eq!(worker.pid, event.pid);
        assert_eq!(worker.beats, 0);
        assert!(worker.first_heartbeat.is_none());
    }
    assert!(test.supervisor.config.load_full().is_none());
    assert!(
        test.supervisor
            .workers
            .read()
            .await
            .iter()
            .all(WorkerProcess::is_running)
    );
}

#[tokio::test(start_paused = true)]
async fn duplicate_wrong_pid_and_other_worker_do_not_satisfy_pending_boot() {
    let test = TestWorkers::new(2).await;
    let mut started = test.state.events().subscribe::<WorkerStarted>();
    let mut confirmed = test.state.events().subscribe::<WorkersBootConfirmed>();
    let first = test.event(1).await;
    let second = test.event(2).await;
    assert!(
        !test
            .supervisor
            .confirm_worker_started(WorkerStarted { pid: 0, ..second })
            .await
    );
    assert!(test.supervisor.observer.snapshot().await.is_empty());
    test.state.events().emit(first);
    test.state.events().emit(first);
    test.state.events().emit(WorkerStarted { pid: 0, ..second });
    test.state
        .events()
        .emit(WorkerStarted { worker_id: WorkerId::new(99), ..second });

    let error = test
        .supervisor
        .wait_workers_started(
            &test.state,
            &mut started,
            &test.state.shutdown_token(),
            Duration::from_secs(30),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    assert!(error.to_string().contains(&second.pid.to_string()));
    assert!(matches!(confirmed.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn child_exit_rejects_even_queued_boot_confirmation() {
    let test = TestWorkers::new(1).await;
    let mut started = test.state.events().subscribe::<WorkerStarted>();
    let mut confirmed = test.state.events().subscribe::<WorkersBootConfirmed>();
    test.state.events().emit(test.event(1).await);
    {
        let mut workers = test.supervisor.workers.write().await;
        workers[0].process.as_mut().unwrap().child_mut().kill().unwrap();
        workers[0].process.as_mut().unwrap().child_mut().wait().unwrap();
    }
    let error = test
        .supervisor
        .wait_workers_started(
            &test.state,
            &mut started,
            &test.state.shutdown_token(),
            Duration::from_secs(30),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("exited early"));
    assert!(matches!(confirmed.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn shutdown_prevents_boot_confirmation() {
    let test = TestWorkers::new(1).await;
    let mut started = test.state.events().subscribe::<WorkerStarted>();
    let mut confirmed = test.state.events().subscribe::<WorkersBootConfirmed>();
    test.state.events().emit(test.event(1).await);
    test.state.initiate_shutdown();
    let error = test
        .supervisor
        .wait_workers_started(
            &test.state,
            &mut started,
            &test.state.shutdown_token(),
            Duration::from_secs(30),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("shutdown"));
    assert!(matches!(confirmed.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn lost_or_closed_startup_stream_is_not_success() {
    assert!(
        startup::received(Err(RecvError::Lagged(3)))
            .unwrap_err()
            .to_string()
            .contains("lagged by 3")
    );
    assert!(
        startup::received(Err(RecvError::Closed))
            .unwrap_err()
            .to_string()
            .contains("closed")
    );
}

#[test]
fn exact_live_presence_above_configuration_restores_the_previous_topology() {
    use claviron_orbit::{Fleet, NodeId, OrbitEpoch};
    use claviron_sys::process;

    use crate::lifecycle::{MasterIdentity, WorkerIdentity};
    use crate::presence::{PresencePhase, publish_presence};

    let process = process::identity(std::process::id())
        .expect("own exact process identity");
    let previous_master =
        MasterIdentity::new(ProcessIdentity::new(u32::MAX - 1, 7), 3);
    let presence = WorkerPresence::new(
        WorkerIdentity::new(WorkerId::new(3), process, 8, previous_master),
        OrbitEpoch::now().as_unix_ms(),
        PresencePhase::Running,
        1,
        None,
        OrbitEpoch::now(),
    );
    let dead = WorkerPresence::new(
        WorkerIdentity::new(
            WorkerId::new(4),
            ProcessIdentity::new(u32::MAX, 9),
            1,
            previous_master,
        ),
        0,
        PresencePhase::Running,
        1,
        None,
        OrbitEpoch::now(),
    );

    let (effective, survivors) =
        effective_worker_count_from_presence(2, 4, |id| match id.raw() {
            3 => Some(presence),
            4 => Some(dead),
            _ => None,
        });

    assert_eq!(
        effective, 3,
        "the highest exact live slot becomes the effective count"
    );
    assert_eq!(survivors, vec![presence]);

    let fleet =
        Fleet::join_as("startup-topology-recovery", 5, NodeId::new(3)).unwrap();
    publish_presence(&fleet, presence).unwrap();
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(5).unwrap(),
        Some("startup-topology-recovery".into()),
        ".",
    );
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));

    assert_eq!(supervisor.recover_worker_topology(&state, &fleet).unwrap(), 3);
    assert_eq!(
        state.worker_count(),
        3,
        "all runnables observe the recovered topology"
    );
}

#[tokio::test(start_paused = true)]
async fn startup_wait_uses_configured_shorter_and_longer_deadlines() {
    for value in ["250ms", "2m"] {
        let test = TestWorkers::new(1).await;
        let cfg: SupervisorConfig =
            serde_yaml::from_str(&format!("startup: {{ timeout: {value} }}"))
                .unwrap();
        let mut started = test.state.events().subscribe::<WorkerStarted>();
        let mut confirmed =
            test.state.events().subscribe::<WorkersBootConfirmed>();
        let began = tokio::time::Instant::now();

        let error = test
            .supervisor
            .wait_workers_started(
                &test.state,
                &mut started,
                &test.state.shutdown_token(),
                cfg.startup.timeout,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("timed out"));
        assert_eq!(began.elapsed(), cfg.startup.timeout);
        assert!(matches!(confirmed.try_recv(), Err(TryRecvError::Empty)));
    }
}

#[tokio::test]
async fn replacement_requires_its_own_boot_event() {
    let test = TestWorkers::new(1).await;
    let old = test.event(1).await;
    assert!(test.supervisor.confirm_worker_started(old).await);
    let last_heartbeat = test.supervisor.observer.snapshot().await
        [&WorkerId::new(1)]
        .last_heartbeat;
    assert!(!test.supervisor.confirm_worker_started(old).await);
    assert_eq!(
        test.supervisor.observer.snapshot().await[&WorkerId::new(1)]
            .last_heartbeat,
        last_heartbeat
    );
    {
        let mut workers = test.supervisor.workers.write().await;
        workers[0].process.as_mut().unwrap().child_mut().kill().unwrap();
        workers[0].process.as_mut().unwrap().child_mut().wait().unwrap();
        workers[0].process = Some(
            OwnedProcess::from_child(
                Command::new("/bin/sleep").arg("60").spawn().unwrap(),
                Arc::clone(&test.supervisor.exited),
            )
            .unwrap(),
        );
        workers[0].set_phase(WorkerPhase::Starting);
    }
    let current = test.event(1).await;
    assert_ne!(old.pid, current.pid);
    assert!(!test.supervisor.confirm_worker_started(old).await);
    assert!(test.supervisor.confirm_worker_started(current).await);
    assert_eq!(
        test.supervisor.observer.snapshot().await[&WorkerId::new(1)].pid,
        current.pid
    );
    test.supervisor.establish_master_for_tests();
    test.supervisor
        .drain_stale_worker(
            &test.state,
            old.worker_id,
            old.pid,
            Duration::from_secs(10),
            crate::lifecycle::OperationId::from_raw(1).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        test.supervisor.workers.write().await[0]
            .process
            .as_mut()
            .unwrap()
            .try_reap()
            .unwrap()
            .is_none()
    );
}
