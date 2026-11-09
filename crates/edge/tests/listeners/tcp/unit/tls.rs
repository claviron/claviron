use claviron_http::config::{HttpYaml, ListenerSettings};

use super::*;

#[test]
fn host_alpn_preserves_explicit_preference_and_inherits_only_when_omitted() {
    let http = serde_yaml::from_str::<HttpYaml>("listeners: {tls: {transport: tcp, tls: true, protocols: [h2, h1], listen: '127.0.0.1:443', requests: {max_body_size: off, body_read_timeout: off}}}").unwrap().resolve().unwrap();
    let listener = http.listeners.values().next().unwrap();
    let ListenerSettings::Tcp(tcp) = &listener.settings else { unreachable!() };
    let mut host: crate::config::HostConfig =
        serde_yaml::from_str("root: public").unwrap();
    assert_eq!(allowed_protocols(tcp, &host), [H2, H1]);
    host.transport.protocols = Some(vec![H1, H2]);
    assert_eq!(allowed_protocols(tcp, &host), [H1, H2]);
    let mut h1_first = tcp.clone();
    h1_first.protocols = vec![H1, H2];
    host.transport.protocols = Some(vec![H2, H1]);
    assert_eq!(allowed_protocols(&h1_first, &host), [H2, H1]);
    host.transport.protocols = None;
    assert_eq!(allowed_protocols(&h1_first, &host), [H1, H2]);
    host.transport.protocols = Some(vec![H1]);
    assert_eq!(allowed_protocols(tcp, &host), [H1]);
    host.transport.protocols = Some(vec![]);
    assert!(allowed_protocols(tcp, &host).is_empty());
    let mut h2_only = tcp.clone();
    h2_only.protocols = vec![H2];
    host.transport.protocols = Some(vec![H1]);
    assert!(
        allowed_protocols(&h2_only, &host).is_empty(),
        "host cannot add a protocol disabled on its listener"
    );
    host.transport.protocols = Some(vec![claviron_tls::Proto::H3, H1, H2]);
    assert_eq!(allowed_protocols(&h2_only, &host), [H2]);
}
