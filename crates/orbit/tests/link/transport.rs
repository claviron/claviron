use super::*;

/// The two ends open the same segments or neither boots: Orbit refuses a
/// segment whose geometry differs from the code's, so a drifted constant does
/// not degrade, it stops a process. Pinning the kinds here is the cheapest
/// place to notice a number being changed by accident.
#[test]
fn the_transport_owns_its_own_kinds() {
    assert_eq!(TRANSPORT_INBOX_KIND, 195);
    assert_eq!(TRANSPORT_STREAM_KIND, 211);
    assert_eq!(TRANSPORT_STREAM_SPEC.kind, TRANSPORT_STREAM_KIND);

    // Not orbit-stream's default table. Worker coordination and body transfer
    // want different geometry, and sharing a kind would force one on both.
    assert_ne!(TRANSPORT_STREAM_KIND, orbitive::stream::STREAM_KIND);
}

/// `StreamSpec` validates on open, not on construction, so a geometry that is
/// not a power of two fails at boot rather than here. Catch it at build time
/// instead.
#[test]
fn the_stream_geometry_is_openable() {
    assert!(TRANSPORT_STREAM_SPEC.lane_capacity.is_power_of_two());
    assert!(TRANSPORT_STREAM_SPEC.buffer_bytes.is_power_of_two());
    assert!(TRANSPORT_INBOX_CAPACITY.is_power_of_two());
}

/// A frame carries the exchange header, and an HTTP server that accepts more
/// header than this would be accepting what it cannot forward. A build-time
/// assertion: the geometry comes from `option_env!`, so a deployment that
/// lowers it fails to compile rather than to serve.
const _: () = assert!(TRANSPORT_INBOX_PAYLOAD_CAPACITY >= 8 * 1024);
