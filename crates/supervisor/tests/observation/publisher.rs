use std::time::Duration;

use claviron_runtime::{FleetCapacity, FleetNodeId, RunMode, WorkerCount};

use super::*;

#[test]
fn publication_projection_tracks_only_snapshot_policy() {
    let mut config = crate::config::SupervisorConfig::default();
    let before: SnapshotPublication =
        serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
    config.monitor.interval *= 2;
    let unrelated: SnapshotPublication =
        serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
    assert_eq!(before, unrelated);
    config.snapshot.max_age *= 2;
    let changed: SnapshotPublication =
        serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
    assert_ne!(before, changed);
    assert_eq!(changed.max_age, config.snapshot.max_age);
}

#[test]
fn publication_projection_rejects_invalid_policy() {
    let mut config = crate::config::SupervisorConfig::default();
    config.snapshot.publish_interval = Duration::ZERO;
    assert!(
        serde_json::from_value::<SnapshotPublication>(
            serde_json::to_value(config).unwrap()
        )
        .is_err()
    );
}

#[tokio::test]
async fn stopped_publisher_does_not_publish_and_restart_keeps_the_fleet() {
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(0),
        FleetCapacity::new(1).unwrap(),
        Some("publication-test".into()),
        ".",
    );
    let control = Arc::new(WorkerControl::new(Arc::new(
        crate::observer::HeartbeatObserver::new(),
    )));
    let mut config = crate::config::SupervisorConfig::default();
    config.snapshot.publish_interval = Duration::from_millis(1);
    control.config.store(Some(Arc::new(config)));
    control.establish_master_for_tests();
    let publisher = WorkerSnapshotPublisher::new(control);
    let fleet = claviron_orbit::Fleet::join_as(
        "publisher-generation-test",
        1,
        claviron_orbit::NodeId::ZERO,
    )
    .unwrap();
    let first_master = publisher.supervisor.master().unwrap();
    let mut last = None;
    for _ in 0..2 {
        let cancel = tokio_util::sync::CancellationToken::new();
        let run = publisher.publish_until_stopped(
            state.clone(),
            RunContext::new(cancel.clone()),
            &fleet,
        );
        tokio::pin!(run);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut run)
                .await
                .is_err()
        );
        cancel.cancel();
        run.await.unwrap();
        let captured =
            crate::snapshot::collect(&fleet, std::time::SystemTime::now())
                .unwrap();
        if let Some(previous) = last {
            assert!(captured.captured_at > previous);
        }
        last = Some(captured.captured_at);
        publisher
            .publish_until_stopped(
                state.clone(),
                RunContext::new(cancel),
                &fleet,
            )
            .await
            .unwrap();
        assert_eq!(
            crate::snapshot::collect(&fleet, std::time::SystemTime::now())
                .unwrap()
                .captured_at,
            captured.captured_at
        );
        assert_eq!(publisher.supervisor.master().unwrap(), first_master);
        assert!(!publisher.supervisor.is_shutting_down());
    }
}
