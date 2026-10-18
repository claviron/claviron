use orbitive::metrics::{OrbitMetricSnapshot, OrbitTyped};

use super::*;

#[test]
fn http_codec_round_trip_matches_exact_ring_payload() {
    let expected = HttpMetricsRecord::new(
        3,
        10,
        HttpMetricsSnapshot {
            requests_in_flight: 11,
            requests_total: 12,
            sessions_open: 3,
            cache_hit_plus_total: 13,
            cache_hits_total: 14,
            cache_misses_total: 15,
            memory_cache_entries: 16,
            memory_cache_bytes: 17,
        },
    );

    let encoded = expected.encode().unwrap();
    assert_eq!(encoded.len(), HttpMetricsRecord::RING_SPEC.payload_capacity);
    assert_eq!(HttpMetricsRecord::decode(&encoded).unwrap(), expected);
}

#[test]
fn server_codec_round_trip_matches_exact_ring_payload() {
    let expected = ServerMetricsRecord::new(
        3,
        10,
        ServerMetricsSnapshot {
            tcp_accepts_total: 11,
            udp_accepts_total: 12,
            conn_in_flight: 13,
            conn_total: 14,
            tls_handshake_failures: 15,
            vhost_rejects: 16,
        },
    );

    let encoded = expected.encode().unwrap();
    assert_eq!(encoded.len(), ServerMetricsRecord::RING_SPEC.payload_capacity);
    assert_eq!(ServerMetricsRecord::decode(&encoded).unwrap(), expected);
}
