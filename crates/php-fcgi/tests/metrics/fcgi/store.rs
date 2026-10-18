use super::*;

/// Each worker owns its own pool, so both the census and configured
/// capacity add across workers.
#[test]
fn the_aggregate_adds_process_local_censuses_and_capacity() {
    let worker = |live: u64, idle: u64| FcgiScalarMetricsSnapshot {
        captured_at: 100,
        live,
        idle,
        max_live: 56,
        requests: FcgiRequestMetricsSnapshot::default(),
    };

    let mut fleet = worker(20, 4);
    fleet.merge(&worker(16, 2));

    assert_eq!(fleet.live, 36, "connections held add up");
    assert_eq!(fleet.idle, 6, "connections idle add up");
    assert_eq!(
        fleet.max_live, 112,
        "process-local ceilings add across workers"
    );
}
