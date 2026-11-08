use std::sync::Arc;

use super::*;

#[test]
fn scheme_follows_tls_facts_not_port_or_application_protocol() {
    let mut ctx = ConnCtx {
        transport: crate::listener::ListenerTransport::Tcp,
        state: Default::default(),
        listener: "test".parse().unwrap(),
        proto: Some(Proto::H1),
        remote: "127.0.0.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 443,
        real_ip: None,
        country_code: None,
        tls: None,
    };
    assert_eq!(ctx.scheme(), http::uri::Scheme::HTTP);
    let mut extensions = http::Extensions::new();
    assert_eq!(crate::ext::request_scheme(&extensions), None);
    extensions.insert(http::uri::Scheme::HTTPS);
    assert_eq!(
        crate::ext::request_scheme(&extensions),
        Some(http::uri::Scheme::HTTPS)
    );
    extensions.insert(Arc::new(ctx.clone()));
    assert_eq!(
        crate::ext::request_scheme(&extensions),
        Some(http::uri::Scheme::HTTP)
    );
    ctx.local_port = 8080;
    ctx.tls = Some(TlsInfo {
        protocol_version: None,
        cipher_suite: None,
        client_auth: ClientAuthStatus::NotProvided,
        client_cert: None,
    });
    assert_eq!(ctx.scheme(), http::uri::Scheme::HTTPS);
}

#[test]
fn connection_values_are_one_per_type_and_nothing_prepared_holds_nothing() {
    assert!(ConnStateBuilder::default().build().is_empty());

    let mut values = ConnStateBuilder::default();
    assert_eq!(values.insert(7_u16), None);
    assert_eq!(values.insert("peer"), None);
    // A second value of a type replaces the first and hands it back.
    assert_eq!(values.insert(9_u16), Some(7));
    let state = values.build();
    assert_eq!(state.get::<u16>(), Some(&9));
    assert_eq!(state.get::<&str>(), Some(&"peer"));
    assert_eq!(state.get::<u32>(), None);
    // A copy of the facts reads the same values.
    assert_eq!(state.clone().get::<u16>(), Some(&9));
}
