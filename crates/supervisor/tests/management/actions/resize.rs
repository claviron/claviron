use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use claviron_runtime::{FleetCapacity, FleetNodeId, RunMode};

use super::*;
use crate::control::WorkerProcess;
use crate::lifecycle::{ExitOutcome, OwnedProcess, WorkerPhase};
use crate::observer::HeartbeatObserver;
use crate::process::owned::ProcessOrigin;

// Isolated sleeping children stand in for already tracked workers. No
// production binary, event transport or shared-memory fleet is started.
struct GrowthFixture {
    control: WorkerControl,
    adopted_children: Vec<Child>,
}

impl Drop for GrowthFixture {
    fn drop(&mut self) {
        for child in &mut self.adopted_children {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Ok(mut workers) = self.control.workers.try_write() {
            for process in
                workers.iter_mut().filter_map(|row| row.process.as_mut())
            {
                if process.origin() == ProcessOrigin::Spawned {
                    let child = process.child_mut();
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
    }
}

impl GrowthFixture {
    async fn add(
        &mut self,
        id: u8,
        adopted: bool,
        phase: WorkerPhase,
    ) {
        let child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
        let process = if adopted {
            let identity = claviron_sys::process::identity(child.id()).unwrap();
            self.adopted_children.push(child);
            OwnedProcess::from_adopted(
                identity,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                self.control.exited.clone(),
            )
            .unwrap()
        } else {
            OwnedProcess::from_child(child, self.control.exited.clone())
                .unwrap()
        };
        let mut row = WorkerProcess::new(
            WorkerId::new(id),
            Some(process),
            phase,
            WorkerIntent::EnsureRunning,
        )
        .with_incarnation(u64::from(id) + 10);
        row.reload_revision = u64::from(id) + 20;
        self.control.workers.write().await.push(row);
    }
}

#[tokio::test]
async fn growth_preserves_spawned_adopted_and_stopped_workers_after_boot_confirmation()
 {
    for origins in [[false, false], [true, true], [true, false]] {
        let mut fixture = GrowthFixture {
            control: WorkerControl::new(Arc::new(HeartbeatObserver::new())),
            adopted_children: Vec::new(),
        };
        fixture
            .control
            .config
            .store(Some(Arc::new(crate::config::SupervisorConfig::default())));
        fixture.add(1, origins[0], WorkerPhase::Running).await;
        fixture.add(2, origins[1], WorkerPhase::Running).await;
        fixture.control.workers.write().await.push(stopped(3));
        fixture.add(4, false, WorkerPhase::Starting).await;
        let control = &fixture.control;
        let state = AppState::initialize(
            RunMode::Master,
            FleetNodeId::ZERO,
            WorkerCount::new(3),
            FleetCapacity::new(5).unwrap(),
            Some("growth-test".into()),
            ".",
        );
        let facts = |row: &WorkerProcess| {
            (
                row.id,
                row.process
                    .as_ref()
                    .map(|process| (process.identity(), process.origin())),
                row.phase(),
                row.intent(),
                row.incarnation,
                row.reload_revision,
            )
        };
        let before = control
            .workers
            .read()
            .await
            .iter()
            .take(3)
            .map(facts)
            .collect::<Vec<_>>();
        let new_pid = control.workers.read().await[3].pid().unwrap();
        let confirm = confirm_added_worker(
            control,
            &state,
            3,
            4,
            new_pid,
            OperationId::from_raw(1).unwrap(),
        );
        let boot = async {
            tokio::task::yield_now().await;
            assert_eq!(
                state.worker_count(),
                3,
                "count must not commit before boot confirmation"
            );
            control.workers.write().await[3].set_phase(WorkerPhase::Running);
            control.changed.notify_waiters();
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            let (result, ()) = tokio::join!(confirm, boot);
            result.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(state.worker_count(), 4);
        let workers = control.workers.read().await;
        assert_eq!(
            workers.iter().take(3).map(facts).collect::<Vec<_>>(),
            before
        );
        assert!(
            workers[2].is_stopped(),
            "growth must not start a stopped retained slot"
        );
        assert_eq!(workers[3].pid(), Some(new_pid));
        assert!(workers[3].is_running());
        for row in workers.iter().filter_map(|row| row.process.as_ref()) {
            assert!(
                row.identity().is_alive(),
                "growth must not terminate retained processes"
            );
        }
    }
}

#[tokio::test]
async fn configured_table_ignores_stopped_tombstones_above_fleet_size() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    *supervisor.workers.write().await = vec![running(1), stopped(2)];
    supervisor.validate_resize_table(1).await.unwrap();
}

#[tokio::test]
async fn growth_cannot_exceed_physical_worker_capacity() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    *supervisor.workers.write().await = vec![running(1)];
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("resize-test".into()),
        ".",
    );

    let error = supervisor
        .resize_workers(&state, 1, 2, OperationId::from_raw(1).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("capacity"));
    assert_eq!(state.worker_count(), 1);
}

#[tokio::test]
async fn shrink_cannot_remove_the_final_worker() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("resize-test".into()),
        ".",
    );

    let error = supervisor
        .resize_workers(&state, 1, 0, OperationId::from_raw(1).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("non-zero worker count"));
    assert_eq!(state.worker_count(), 1);
}

#[tokio::test]
async fn shrink_retires_only_the_highest_slot() {
    let supervisor = WorkerControl::new(Arc::new(HeartbeatObserver::new()));
    *supervisor.workers.write().await =
        vec![running(1), running(2), running(3), stopped(4)];
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(4),
        FleetCapacity::new(5).unwrap(),
        Some("resize-test".into()),
        ".",
    );
    let retained_before = supervisor
        .workers
        .read()
        .await
        .iter()
        .take(3)
        .map(|worker| (worker.id, worker.phase(), worker.incarnation))
        .collect::<Vec<_>>();

    supervisor
        .resize_workers(&state, 4, 3, OperationId::from_raw(1).unwrap())
        .await
        .unwrap();

    assert_eq!(state.worker_count(), 3);
    let workers = supervisor.workers.read().await;
    let retained_after = workers
        .iter()
        .take(3)
        .map(|worker| (worker.id, worker.phase(), worker.incarnation))
        .collect::<Vec<_>>();
    assert_eq!(
        retained_after, retained_before,
        "retained slots must not be replaced"
    );
    assert!(
        workers[3].is_stopped(),
        "the retired high slot remains a tombstone"
    );
}

fn running(id: u8) -> WorkerProcess {
    worker(id, WorkerPhase::Running, WorkerIntent::EnsureRunning)
}

fn stopped(id: u8) -> WorkerProcess {
    worker(
        id,
        WorkerPhase::Exited { outcome: ExitOutcome::Planned },
        WorkerIntent::Stopped,
    )
}

fn worker(
    id: u8,
    phase: WorkerPhase,
    intent: WorkerIntent,
) -> WorkerProcess {
    WorkerProcess::new(WorkerId::new(id), None, phase, intent)
}
