#[tokio::test]
async fn publication_restart_preserves_process_clock_and_counters() {
    use std::time::Duration;

    use claviron_runtime::{FleetCapacity, FleetNodeId, RunMode, WorkerCount};

    use super::*;
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(0),
        FleetCapacity::new(1).unwrap(),
        Some("metrics-generation-test".into()),
        ".",
    );
    let mut control =
        WorkerControl::new(Arc::new(crate::observer::HeartbeatObserver::new()));
    control.metrics_started_at = Instant::now() - Duration::from_secs(60);
    control
        .worker_restarts_total
        .store(7, std::sync::atomic::Ordering::Relaxed);
    let control = Arc::new(control);
    let mut config = SupervisorConfig::default();
    config.metrics.publish_interval = Duration::from_millis(1);
    control.config.store(Some(Arc::new(config)));
    let publisher = SupervisorMetricsPublisher::master(
        control.clone(),
        control.config.clone(),
    );
    let fleet = Arc::new(
        claviron_orbit::Fleet::join_as(
            "metrics-generation-test",
            1,
            claviron_orbit::NodeId::ZERO,
        )
        .unwrap(),
    );
    for _ in 0..2 {
        let cancel = tokio_util::sync::CancellationToken::new();
        let run = publisher.publish_until_stopped(
            state.clone(),
            RunContext::new(cancel.clone()),
            OrbitMetricPublisher::new(fleet.clone()),
        );
        tokio::pin!(run);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut run)
                .await
                .is_err()
        );
        cancel.cancel();
        run.await.unwrap();
        let collected =
            OrbitMetricCollector::<ProcessMetricsSnapshot>::new(fleet.clone())
                .fresh_by_node(unix_now(), 30);
        let snapshot = collected.get(&0).unwrap().snapshot;
        assert!(snapshot.uptime_seconds >= 60);
        assert_eq!(snapshot.worker_restarts_total, 7);
        assert_eq!(snapshot.pid, std::process::id());
        // A canceled generation returns without touching the ring.
        publisher
            .publish_until_stopped(
                state.clone(),
                RunContext::new(cancel),
                OrbitMetricPublisher::new(fleet.clone()),
            )
            .await
            .unwrap();
    }
}
use super::*;

#[test]
fn process_snapshot_codec_is_exact_and_round_trips() {
    let snapshot = ProcessMetricsSnapshot {
        node_id: 2,
        worker_id: 2,
        pid: 42,
        captured_at: 10,
        rss_bytes: 20,
        uptime_seconds: 30,
        worker_restarts_total: 40,
        worker_rotations_total: 50,
        workers_configured: 3,
        workers_alive: 2,
    };

    let encoded = snapshot.encode().expect("encode");
    assert_eq!(encoded.len(), PROCESS_METRICS_WIRE_LEN);
    assert_eq!(
        encoded.len(),
        ProcessMetricsSnapshot::RING_SPEC.payload_capacity
    );
    assert_eq!(
        ProcessMetricsSnapshot::decode(&encoded).expect("decode"),
        snapshot
    );
}

#[test]
fn process_formatter_contains_only_process_metrics() {
    let snapshot = ProcessMetricsSnapshot {
        node_id: 1,
        worker_id: 1,
        pid: 42,
        captured_at: 10,
        rss_bytes: 20,
        uptime_seconds: 30,
        worker_restarts_total: 0,
        worker_rotations_total: 0,
        workers_configured: 0,
        workers_alive: 0,
    };
    let mut output = String::new();
    write_process_metrics_header(&mut output);
    write_process_metrics_sample(&mut output, "worker", &snapshot);

    assert!(output.contains("claviron_supervisor_process_rss_bytes"));
    assert!(output.contains("claviron_supervisor_process_uptime_seconds"));
    assert!(!output.contains("http"));
    assert!(!output.contains("cache"));
    assert!(!output.contains("tcp"));
    assert!(!output.contains("php"));
}

#[test]
fn standalone_registers_local_process_metrics_without_worker_counters() {
    let state = AppState::standalone("supervisor-metrics-test", ".");
    crate::register(&state).expect("register supervisor metrics");

    let output = state
        .registry_ref()
        .resolve::<MetricsProvider>()
        .expect("metrics provider")
        .render_prometheus(&state)
        .expect("render metrics");

    assert!(output.contains("role=\"standalone\""));
    assert!(output.contains("claviron_supervisor_process_rss_bytes"));
    assert!(!output.contains("claviron_supervisor_worker_restarts_total"));
    assert!(!output.contains("claviron_supervisor_workers_configured"));
}

#[test]
fn master_formatter_contains_only_supervisor_owned_fleet_values() {
    let snapshot = ProcessMetricsSnapshot {
        node_id: 0,
        worker_id: 0,
        pid: 42,
        captured_at: 10,
        rss_bytes: 20,
        uptime_seconds: 30,
        worker_restarts_total: 4,
        worker_rotations_total: 5,
        workers_configured: 3,
        workers_alive: 2,
    };
    let mut output = String::new();
    write_master_metrics(&mut output, &snapshot);

    assert!(output.contains("claviron_supervisor_worker_restarts_total 4"));
    assert!(output.contains("claviron_supervisor_worker_rotations_total 5"));
    assert!(output.contains("claviron_supervisor_workers_configured 3"));
    assert!(output.contains("claviron_supervisor_workers_alive 2"));
    assert!(!output.contains("heartbeat"));
}
