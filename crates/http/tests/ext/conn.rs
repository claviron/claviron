use std::sync::Arc;

use super::*;

#[test]
fn scheme_follows_tls_facts_not_port_or_application_protocol() {
    let mut ctx = ConnCtx {
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
