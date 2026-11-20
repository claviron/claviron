use claviron_http::config::HttpYaml;

use super::*;

/// A listener that states a client policy requires it for every host on it:
/// a host that declares none, or the same one, gets the listener's; a host
/// that declares a different one is refused, by name, with its listener.
/// Without a listener policy each host keeps its own.
#[test]
fn a_listener_policy_applies_to_its_hosts_and_refuses_a_different_one() {
    let http = serde_yaml::from_str::<HttpYaml>(
        "defaults: {listener: {requests: {max_body_size: off, body_read_timeout: off}}}
listeners:
  mutual: {transport: tcp, listen: '127.0.0.1:8443', protocols: [h2, h1], tls: {client_auth: {authority: {local: example}}}}
  open: {transport: tcp, listen: '127.0.0.1:8444', protocols: [h2, h1], tls: on}
",
    )
    .unwrap()
    .resolve()
    .unwrap();
    let mutual = &http.listeners[&"mutual".parse().unwrap()];
    let open = &http.listeners[&"open".parse().unwrap()];
    let host = |yaml: &str| serde_yaml::from_str::<HostConfig>(yaml).unwrap();
    let silent = host("root: public");
    let same = host(
        "{root: public, tls: {client_auth: {authority: {local: example}}}}",
    );
    let other = host(
        "{root: public, tls: {client_auth: {authority: {local: example}, organization: Example, unit: Operations}}}",
    );
    let canonical: Host = "app.test".parse().unwrap();

    let required = mutual.client_auth();
    assert!(required.is_some());
    assert_eq!(client_auth(mutual, &canonical, &silent).unwrap(), required);
    assert_eq!(client_auth(mutual, &canonical, &same).unwrap(), required);
    let error =
        client_auth(mutual, &canonical, &other).unwrap_err().to_string();
    assert!(
        error.contains("TLS host app.test")
            && error.contains("listener 'mutual'")
            && error.contains("declare the policy in one place"),
        "{error}"
    );

    assert_eq!(client_auth(open, &canonical, &silent).unwrap(), None);
    assert_eq!(
        client_auth(open, &canonical, &other).unwrap(),
        other.tls.client_auth.as_ref()
    );
}
