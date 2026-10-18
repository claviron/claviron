use super::*;

#[test]
fn client_forwarding_cannot_override_cgi_scheme() {
    for name in ["Forwarded", "X-Forwarded-Proto", "Cookie", "Content-Type"] {
        assert!(is_request_denied(name));
    }
    assert!(!is_request_denied("Authorization"));
    assert!(!is_request_denied("X-Application-Header"));
    assert!(!is_denied("Forwarded"), "response policy is independent");
}
