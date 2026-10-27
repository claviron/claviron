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

#[test]
fn regression_appended_forwarding_chain_stops_at_the_first_untrusted_hop() {
    let peer = "192.0.2.10".parse().unwrap();
    let trusted = ["192.0.2.0/24".parse().unwrap()];
    let order = [HeaderName::from_static("x-forwarded-for")];
    let mut headers = HeaderMap::new();
    headers.append("x-forwarded-for", "203.0.113.99".parse().unwrap());
    headers
        .append("x-forwarded-for", "198.51.100.7, 192.0.2.11".parse().unwrap());
    assert_eq!(
        resolve_real_ip_from_headers(&headers, peer, &trusted, &order),
        "198.51.100.7".parse::<IpAddr>().unwrap()
    );

    headers.insert(
        "x-forwarded-for",
        "203.0.113.99, unknown, 192.0.2.11".parse().unwrap(),
    );
    assert_eq!(
        resolve_real_ip_from_headers(&headers, peer, &trusted, &order),
        "192.0.2.11".parse::<IpAddr>().unwrap()
    );
    headers.insert(
        "x-forwarded-for",
        "203.0.113.99, 2001:db8::1".parse().unwrap(),
    );
    assert_eq!(
        resolve_real_ip_from_headers(&headers, peer, &trusted, &order),
        "2001:db8::1".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn regression_single_address_headers_cannot_smuggle_a_chain_or_override_a_malformed_priority()
 {
    let peer = "192.0.2.10".parse().unwrap();
    let trusted = ["192.0.2.0/24".parse().unwrap()];
    let order = [
        HeaderName::from_static("x-real-ip"),
        HeaderName::from_static("x-forwarded-for"),
    ];
    let mut headers = HeaderMap::new();
    headers.insert("x-real-ip", "203.0.113.99, 198.51.100.7".parse().unwrap());
    headers.insert("x-forwarded-for", "203.0.113.99".parse().unwrap());
    assert_eq!(
        resolve_real_ip_from_headers(&headers, peer, &trusted, &order),
        peer
    );
    headers.insert("x-real-ip", "198.51.100.7".parse().unwrap());
    assert_eq!(
        resolve_real_ip_from_headers(&headers, peer, &trusted, &order),
        "198.51.100.7".parse::<IpAddr>().unwrap()
    );
}
