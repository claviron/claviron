use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
};
use tokio::time::{sleep, timeout};

use super::{PoolTask, eligible_runtime_count, run};
use crate::config::TaskServiceConfig;
use crate::registry::TaskScope;
use crate::runner::TaskRunner;

#[test]
fn pool_participant_count_follows_scope() {
    use claviron_orbit::service::{
        FleetMember, FleetMemberRole, FleetMembership,
    };

    struct Members(AtomicUsize);
    impl FleetMembership for Members {
        fn members(
            &self,
            _: &AppState,
        ) -> Result<Vec<FleetMember>, String> {
            Ok((0..=self.0.load(Ordering::Relaxed) as u16)
                .map(|node_id| FleetMember {
                    node_id,
                    role: if node_id == 0 {
                        FleetMemberRole::Master
                    } else {
                        FleetMemberRole::Worker
                    },
                })
                .collect())
        }
    }
    let state = AppState::initialize(
        RunMode::Worker(WorkerId::new(3)),
        FleetNodeId::new(3),
        WorkerCount::new(8),
        FleetCapacity::new(9).unwrap(),
        None,
        PathBuf::from("."),
    );

    assert!(eligible_runtime_count(TaskScope::Workers, &state).is_err());
    let source = Arc::new(Members(AtomicUsize::new(4)));
    state.registry_ref().insert(source.clone()).unwrap();
    state
        .registry_ref()
        .bind_dyn::<dyn FleetMembership, Members>(|s| s)
        .unwrap();
    for count in [4, 8, 3] {
        source.0.store(count, Ordering::Relaxed);
        assert_eq!(
            eligible_runtime_count(TaskScope::Master, &state).unwrap(),
            1
        );
        assert_eq!(
            eligible_runtime_count(TaskScope::Workers, &state).unwrap(),
            count as u16
        );
        assert_eq!(
            eligible_runtime_count(TaskScope::Fleet, &state).unwrap(),
            count as u16 + 1
        );
    }
    assert_eq!(state.worker_count(), 8);
}

#[test]
fn fleet_slot_has_one_owner_and_is_reclaimable_after_release() {
    use claviron_orbit::Fleet;
    use claviron_orbit::lock::{LockOwner, OrbitLock};

    use super::Coordinator;

    let fleet = Arc::new(Fleet::join("task_pool_slots", 2).expect("fleet"));
    let first = Coordinator::Fleet {
        locks: OrbitLock::new(Arc::clone(&fleet)).expect("first locks"),
        owner: LockOwner::from("worker:1"),
        lease_ttl: Duration::from_secs(30),
    };
    let second = Coordinator::Fleet {
        locks: OrbitLock::new(fleet).expect("second locks"),
        owner: LockOwner::from("worker:2"),
        lease_ttl: Duration::from_secs(30),
    };

    let ownership =
        first.acquire("queue", 1).expect("first acquire").expect("owner");
    assert!(second.acquire("queue", 1).expect("second acquire").is_none());

    drop(ownership);
    assert!(second.acquire("queue", 1).expect("reacquire").is_some());
}

#[tokio::test]
async fn standalone_controller_runs_the_configured_pool_capacity() {
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let current_for_task = Arc::clone(&current);
    let peak_for_task = Arc::clone(&peak);
    let mut runner = TaskRunner::from(
        move |_: AppState, token: tokio_util::sync::CancellationToken| {
            let current = Arc::clone(&current_for_task);
            let peak = Arc::clone(&peak_for_task);
            async move {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                token.cancelled().await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    runner.set_name("queue");
    let runner = Arc::new(runner);

    let parsed: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  queue:
    dispatch: pool
    concurrency: 3
    timing: { kind: daemon }
"#,
    )
    .expect("task config");
    let cfg = parsed.tasks.get("queue").expect("queue").clone();
    let state = AppState::standalone("task-pool-test", PathBuf::from("."));

    let controller_token = tokio_util::sync::CancellationToken::new();
    let controller = tokio::spawn(run(
        PoolTask::new(
            Arc::clone(&runner),
            cfg,
            TaskScope::Fleet,
            super::TaskDispatch::Pool,
        ),
        state,
        controller_token.clone(),
        Duration::from_millis(30),
    ));

    timeout(Duration::from_secs(1), async {
        while current.load(Ordering::SeqCst) != 3 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("pool reached desired capacity");

    controller_token.cancel();
    timeout(Duration::from_secs(1), controller)
        .await
        .expect("controller stopped")
        .expect("controller joined");

    assert_eq!(peak.load(Ordering::SeqCst), 3);
    assert_eq!(current.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn standalone_controller_runs_one_daemon_body() {
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let current_for_task = Arc::clone(&current);
    let peak_for_task = Arc::clone(&peak);
    let mut runner = TaskRunner::from(
        move |_: AppState, token: tokio_util::sync::CancellationToken| {
            let current = Arc::clone(&current_for_task);
            let peak = Arc::clone(&peak_for_task);
            async move {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                token.cancelled().await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    runner.set_name("singleton");
    let runner = Arc::new(runner);

    let parsed: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  singleton:
    dispatch: one
    timing: { kind: daemon }
"#,
    )
    .expect("task config");
    let cfg = parsed.tasks.get("singleton").expect("singleton").clone();
    let state = AppState::standalone("task-one-test", PathBuf::from("."));

    let controller_token = tokio_util::sync::CancellationToken::new();
    let controller = tokio::spawn(run(
        PoolTask::new(
            Arc::clone(&runner),
            cfg,
            TaskScope::Fleet,
            super::TaskDispatch::One,
        ),
        state,
        controller_token.clone(),
        Duration::from_millis(30),
    ));

    timeout(Duration::from_secs(1), async {
        while current.load(Ordering::SeqCst) != 1 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("one reached its single body");

    controller_token.cancel();
    timeout(Duration::from_secs(1), controller)
        .await
        .expect("controller stopped")
        .expect("controller joined");

    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(current.load(Ordering::SeqCst), 0);
}
