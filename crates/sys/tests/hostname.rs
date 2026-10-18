use super::*;

#[test]
fn removes_the_dns_suffix() {
    assert_eq!(
        short_hostname_from(OsStr::new("worker-2.example.test")),
        "worker-2"
    );
}

#[test]
fn empty_hostname_has_a_stable_fallback() {
    assert_eq!(short_hostname_from(OsStr::new("")), "unknown");
}
