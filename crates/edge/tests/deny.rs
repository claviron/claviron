#![cfg(feature = "server")]

use claviron_edge::deny::DenyService;
use http::Request;
use regex::RegexSet;

#[test]
fn matcher_reuses_compiled_rules() {
    let set = RegexSet::new(["^/private(?:/|$)"]).unwrap();
    let matcher = DenyService::from_set(&set);
    assert!(matcher.is_blocked("/private"));
    assert!(matcher.clone().is_blocked("/private/file"));
    assert!(!matcher.is_blocked("/privateer"));
    assert!(!DenyService::empty().is_blocked("/private"));
}

#[test]
fn early_check_retains_the_original_request() {
    let set = RegexSet::new(["^/private(?:/|$)"]).unwrap();
    let matcher = DenyService::from_set(&set);
    let request = Request::builder().uri("/public?raw=%2F").body(42).unwrap();
    let checked = matcher.check_request(request).ok().unwrap();
    assert_eq!(checked.request().uri(), "/public?raw=%2F");
    let request = matcher.clone().accept_checked(checked).unwrap();
    assert_eq!(request.uri(), "/public?raw=%2F");
    assert_eq!(*request.body(), 42);

    let denied = Request::builder().uri("/private/secret").body(7).unwrap();
    let denied = matcher.check_request(denied).err().unwrap();
    assert_eq!(denied.uri(), "/private/secret");
    assert_eq!(*denied.body(), 7);
}

#[test]
fn another_matcher_cannot_reuse_an_early_allow_as_a_bypass() {
    let empty = DenyService::empty();
    let set = RegexSet::new(["^/private(?:/|$)"]).unwrap();
    let matcher = DenyService::from_set(&set);
    for (path, allowed) in [("/private/secret", false), ("/public", true)] {
        let request = Request::builder().uri(path).body(()).unwrap();
        let checked = empty.check_request(request).ok().unwrap();
        assert_eq!(matcher.accept_checked(checked).is_ok(), allowed);
    }
}
