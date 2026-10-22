use super::*;

#[test]
fn invoke_limits_use_the_service_frame_not_http_geometry() {
    let limits = Limits {
        pending_calls: 2,
        accepted_calls: 1,
        operation_bytes: 8,
        request_bytes: 61,
        response_bytes: 256,
    };
    limits.validate(128).unwrap();
    assert!(limits.validate(127).is_err());
    assert!(Limits { accepted_calls: 0, ..limits }.validate(128).is_err());
    assert!(
        Limits { request_bytes: usize::MAX, ..limits }.validate(128).is_err()
    );
}
