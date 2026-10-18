use super::*;

#[test]
fn forwarded_identity_requires_a_trusted_peer_and_selected_header() {
    let peer: IpAddr = "10.1.2.3".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", "203.0.113.5".parse().unwrap());
    headers.insert("x-real-ip", "203.0.113.6".parse().unwrap());
    let trust = vec!["10.0.0.0/8".parse().unwrap()];
    let order = vec![HeaderName::from_static("x-real-ip")];
    assert_eq!(
        resolve_real_ip_from_headers(&headers, peer, &trust, &order),
        "203.0.113.6".parse::<IpAddr>().unwrap()
    );
    assert_eq!(resolve_real_ip_from_headers(&headers, peer, &[], &order), peer);
    let untrusted = "192.0.2.4".parse().unwrap();
    assert_eq!(
        resolve_real_ip_from_headers(&headers, untrusted, &trust, &order),
        untrusted
    );
    assert_eq!(resolve_real_ip_from_headers(&headers, peer, &trust, &[]), peer);
}
