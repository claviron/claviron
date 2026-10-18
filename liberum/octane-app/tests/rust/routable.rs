use super::*;
use lumenize::axum::body::Body;

#[test]
fn reports_trusted_origin_without_inventing_a_local_connection() {
    let mut request = Request::builder()
        .uri("/demo-service/runtime")
        .body(Body::empty())
        .unwrap();
    let client = "192.0.2.10:12345".parse::<SocketAddr>().unwrap();
    let server = "192.0.2.20:443".parse::<SocketAddr>().unwrap();
    request.extensions_mut().insert(Forwarded {
        client: Some(client),
        server: Some(server),
        scheme: Some("https".parse().unwrap()),
        host: None,
    });
    request.extensions_mut().insert(RealIpExt(client.ip()));

    let info = RequestInfo::from_request(&request);

    assert_eq!(info.client_ip, Some(client.ip()));
    assert_eq!(info.client_address, Some(client));
    assert_eq!(info.server_address, Some(server));
    assert_eq!(info.scheme.as_deref(), Some("https"));
    assert!(info.forwarded_context);
    assert!(info.connection.is_none());
    assert!(info.country.is_none());
}

#[test]
fn reports_available_connection_country_and_listener() {
    let mut request = Request::new(Body::empty());
    request.extensions_mut().insert(Arc::new(ConnCtx {
        listener: "example-listener".parse().unwrap(),
        proto: None,
        remote: "192.0.2.10:12345".parse().unwrap(),
        local_ip: "192.0.2.20".parse().unwrap(),
        local_port: 80,
        real_ip: None,
        country_code: Some("TR".to_owned()),
        tls: None,
    }));

    let info = RequestInfo::from_request(&request);

    assert_eq!(info.country.as_deref(), Some("TR"));
    assert_eq!(info.client_ip, Some("192.0.2.10".parse().unwrap()));
    assert_eq!(info.scheme.as_deref(), Some("http"));
    assert_eq!(info.connection.unwrap().listener, "example-listener");
    assert!(!info.forwarded_context);
}

#[test]
fn excludes_credentials_and_does_not_trust_raw_forwarding_headers() {
    let request = Request::builder()
        .uri("/demo-service/runtime?token=private-value")
        .header("host", "example.test")
        .header("authorization", "Bearer private-value")
        .header("cookie", "session=private-value")
        .header("x-api-key", "private-value")
        .header("x-forwarded-for", "192.0.2.50")
        .header("x-forwarded-proto", "https")
        .body(Body::empty())
        .unwrap();

    let info = RequestInfo::from_request(&request);

    assert_eq!(info.path, "/demo-service/runtime");
    assert_eq!(
        info.headers,
        BTreeMap::from([("host", "example.test".to_owned())])
    );
    assert!(info.client_ip.is_none());
    assert!(info.scheme.is_none());
}

#[test]
fn exposes_only_public_environment_values() {
    let values = std::collections::HashMap::from([
        ("APP_NAME".to_owned(), "example-app".to_owned()),
        ("APP_KEY".to_owned(), "private-value".to_owned()),
        ("DATABASE_URL".to_owned(), "private-value".to_owned()),
        ("PUSHER_APP_SECRET".to_owned(), "private-value".to_owned()),
    ]);

    let exposed = public_environment(&values);

    assert_eq!(
        exposed,
        BTreeMap::from([("APP_NAME", "example-app".to_owned())])
    );
}
