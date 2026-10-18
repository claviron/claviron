use orbitive::metrics::{OrbitMetricSnapshot, OrbitTyped};

use super::*;

#[test]
fn codec_round_trip_matches_exact_ring_payload() {
    let expected = PhpEmbedOrbitMetrics {
        node_id: 3,
        captured_at: 10,
        requests_in_flight: 11,
        requests_total: 12,
        bailouts_total: 13,
        request_bytes_total: 14,
        response_bytes_total: 15,
        threads_spawned_total: 16,
        threads_active: 17,
        engine_errors_total: 18,
    };

    let encoded = expected.encode().unwrap();
    assert_eq!(encoded.len(), PhpEmbedOrbitMetrics::RING_SPEC.payload_capacity);
    assert_eq!(PhpEmbedOrbitMetrics::decode(&encoded).unwrap(), expected);
}
