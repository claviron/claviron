use super::*;
use claviron_http::RouteResponse;
use claviron_http::config::HttpSnapshot;

fn apply_policy(
    policy: &ExchangePolicy,
    path: &str,
) -> RouteResponse {
    let (mut head, ()) =
        http::Request::builder().uri(path).body(()).unwrap().into_parts();
    let (reply, scope) = policy
        .select(path)
        .unwrap()
        .prepare_head(&mut head, ExchangeState::default());
    assert!(reply.is_none());
    scope.finish(RouteResponse::new(claviron_http::empty_body()))
}

#[test]
fn server_headers_keep_vhost_and_route_overrides() {
    let defaults: HttpSnapshot = serde_yaml::from_str(
        "defaults: {headers: {server: edge, set: {x-policy: global, x-remove: global}, unset: [x-revive, server]}}",
    ).unwrap();
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nheaders: {server: host, set: {x-policy: host, x-revive: host}, unset: [x-remove]}\nroutes:\n  /api:\n    headers: {server: api}\n",
    ).unwrap();
    let policy =
        ExchangePolicy::compile(&host, Some(&defaults.headers)).unwrap();
    let response = apply_policy(&policy, "/");
    assert_eq!(response.headers()[http::header::SERVER], "host");
    assert_eq!(response.headers()["x-policy"], "host");
    assert_eq!(response.headers()["x-revive"], "host");
    assert!(!response.headers().contains_key("x-remove"));
    let response = apply_policy(&policy, "/api/test");
    assert_eq!(response.headers()[http::header::SERVER], "api");
    assert_eq!(response.headers()["x-policy"], "global");
}

#[test]
fn server_headers_identity_only_needs_no_exchange_policy() {
    let defaults: HttpSnapshot =
        serde_yaml::from_str("defaults: {headers: {server: edge}}").unwrap();
    let host = serde_yaml::from_str("root: public").unwrap();
    assert_eq!(defaults.headers.server.as_deref(), Some("edge"));
    assert!(ExchangePolicy::compile(&host, Some(&defaults.headers)).is_none());
    let other: HttpSnapshot =
        serde_yaml::from_str("defaults: {headers: {server: other}}").unwrap();
    assert!(defaults != other);
}

#[test]
fn server_headers_reject_invalid_identity_before_compilation() {
    assert!(
        serde_yaml::from_str::<HttpSnapshot>(
            "defaults: {headers: {server: \"edge\\ninvalid\"}}",
        )
        .is_err()
    );
    assert!(
        serde_yaml::from_str::<HostConfig>(
            "root: public\nheaders: {server: \"host\\ninvalid\"}",
        )
        .is_err()
    );
}
