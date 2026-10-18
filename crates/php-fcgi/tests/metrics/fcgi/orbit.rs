use orbitive::metrics::{OrbitMetricSnapshot, OrbitTyped};

use super::*;

#[test]
fn codec_round_trip_matches_exact_ring_payload() {
    let expected = FcgiOrbitMetrics {
        node_id: 3,
        captured_at: 10,
        live: 11,
        idle: 12,
        max_live: 13,
        requests_in_flight: 14,
        requests_total: 15,
        streams_in_flight: 16,
        streams_total: 17,
        stream_bytes_total: 18,
    };
    let encoded = expected.encode().unwrap();
    assert_eq!(encoded.len(), FcgiOrbitMetrics::RING_SPEC.payload_capacity);
    assert_eq!(FcgiOrbitMetrics::decode(&encoded).unwrap(), expected);
}
