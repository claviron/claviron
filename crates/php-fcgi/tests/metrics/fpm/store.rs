use orbitive::metrics::{OrbitMetricSnapshot, OrbitTyped};

use super::{FpmPoolMetrics, FpmPoolOrbitMetrics};

#[test]
fn fpm_pool_orbit_metrics_round_trips() {
    let pool = FpmPoolMetrics {
        pool: "default".to_string(),
        fpm_pool: "www".to_string(),
        process_manager: "dynamic".to_string(),
        up: true,
        start_time: 10,
        start_since: 20,
        accepted_conn: 30,
        listen_queue: 1,
        max_listen_queue: 2,
        listen_queue_len: 3,
        idle_processes: 4,
        active_processes: 5,
        total_processes: 9,
        max_active_processes: 6,
        max_children_reached: 7,
        slow_requests: 8,
        memory_peak: 1024,
    };

    let row = FpmPoolOrbitMetrics::from_pool(123, &pool);
    let encoded = row.encode().unwrap();
    let decoded = FpmPoolOrbitMetrics::decode(&encoded).unwrap();

    assert!(encoded.len() <= FpmPoolOrbitMetrics::RING_SPEC.payload_capacity);
    assert_eq!(decoded, row);
    assert_eq!(decoded.into_pool().pool, "default");
}
