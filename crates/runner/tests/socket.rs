use super::*;

#[test]
fn trusted_forwarding_prepares_client_identity_without_a_network_peer() {
    let trust = Trust::default();
    let request = Request::builder()
        .header("x-forwarded-for", "192.0.2.4")
        .body(claviron_http::empty_body())
        .unwrap();
    let request = prepare(request, None, &trust);
    assert!(request.extensions().get::<ReceivedAt>().is_some());
    assert_eq!(
        request.extensions().get::<RealIpExt>().unwrap().0,
        "192.0.2.4".parse::<std::net::IpAddr>().unwrap()
    );
    assert!(request.extensions().get::<Arc<ConnCtx>>().is_none());
}
