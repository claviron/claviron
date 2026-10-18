use super::*;

#[test]
fn forwarding_scheme_is_explicit_and_transport_owned() {
    let mut req = http::Request::builder()
        .uri("/a%2Fb?q=%2F")
        .header("x-forwarded-proto", "https")
        .body(())
        .unwrap();
    assert!(ForwardedRequestContext::from_request(&req).proto.is_none());
    req.extensions_mut().insert(ForwardedRequestContext {
        real_ip: None,
        proto: Some(Scheme::HTTPS),
    });
    req.extensions_mut().insert(Scheme::HTTP);
    assert_eq!(
        ForwardedRequestContext::from_request(&req).proto,
        Some(Scheme::HTTP)
    );
    assert_eq!(req.uri(), "/a%2Fb?q=%2F");
    let req =
        http::Request::builder().uri("http://app.test/x").body(()).unwrap();
    assert_eq!(
        ForwardedRequestContext::from_request(&req).proto,
        Some(Scheme::HTTP)
    );
}

#[test]
fn strips_static_and_connection_token_hop_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(CONNECTION, HeaderValue::from_static("Foo, Upgrade"));
    headers.insert("foo", HeaderValue::from_static("bar"));
    headers.insert("upgrade", HeaderValue::from_static("websocket"));
    headers.insert("proxy-connection", HeaderValue::from_static("keep-alive"));
    headers.insert("x-safe", HeaderValue::from_static("1"));

    strip_hop_by_hop_headers(&mut headers);

    assert!(!headers.contains_key(CONNECTION));
    assert!(!headers.contains_key("foo"));
    assert!(!headers.contains_key("upgrade"));
    assert!(!headers.contains_key("proxy-connection"));
    assert_eq!(headers.get("x-safe").unwrap(), "1");
}

#[test]
fn rejects_content_length_transfer_encoding_conflict() {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("5"));
    headers.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));

    // Only HTTP/3 reaches this rule: hyper and h2 refuse it themselves.
    assert_eq!(
        reject_request_smuggling(Version::HTTP_3, &headers),
        Err(RequestSmugglingError::ContentLengthWithTransferEncoding)
    );
}

#[test]
fn rejects_conflicting_content_lengths() {
    let mut headers = HeaderMap::new();
    headers.append(CONTENT_LENGTH, HeaderValue::from_static("5"));
    headers.append(CONTENT_LENGTH, HeaderValue::from_static("7"));

    // h2 reads only the first value, so HTTP/2 still needs this.
    assert_eq!(
        reject_request_smuggling(Version::HTTP_2, &headers),
        Err(RequestSmugglingError::ConflictingContentLength)
    );
}

#[test]
fn applies_forwarded_headers_from_trusted_context() {
    let mut headers = HeaderMap::new();
    headers.insert(FORWARDED, HeaderValue::from_static("for=spoof"));

    apply_forwarded_headers(
        &mut headers,
        ForwardedHeaderInfo {
            real_ip: Some("127.0.0.1".parse().unwrap()),
            proto: Some("https"),
            host: Some(HeaderValue::from_static("app.test")),
            via: Some("1.1 claviron"),
        },
    );

    assert!(!headers.contains_key(FORWARDED));
    assert_eq!(headers.get(X_FORWARDED_FOR).unwrap(), "127.0.0.1");
    assert_eq!(headers.get(X_FORWARDED_PROTO).unwrap(), "https");
    assert_eq!(headers.get(X_FORWARDED_HOST).unwrap(), "app.test");
    assert_eq!(headers.get(VIA).unwrap(), "1.1 claviron");
}
#[test]
fn content_lengths_compare_numeric_values_and_reject_non_digits() {
    let mut headers = HeaderMap::new();
    headers.append(CONTENT_LENGTH, HeaderValue::from_static("05"));
    headers.append(CONTENT_LENGTH, HeaderValue::from_static("5"));
    assert_eq!(reject_request_smuggling(Version::HTTP_3, &headers), Ok(()));
    for invalid in ["+5", "5,5", "", "18446744073709551616"] {
        headers.insert(CONTENT_LENGTH, HeaderValue::from_str(invalid).unwrap());
        assert_eq!(
            reject_request_smuggling(Version::HTTP_3, &headers),
            Err(RequestSmugglingError::InvalidContentLength)
        );
    }
}
#[test]
fn strips_tokens_from_every_connection_value_case_insensitively() {
    let mut headers = HeaderMap::new();
    headers.append(CONNECTION, HeaderValue::from_static("Connection, Foo"));
    headers.append(CONNECTION, HeaderValue::from_static("BAR"));
    headers.insert("foo", HeaderValue::from_static("1"));
    headers.insert("bar", HeaderValue::from_static("2"));
    headers.insert("x-safe", HeaderValue::from_static("3"));
    strip_hop_by_hop_headers(&mut headers);
    assert_eq!(headers.len(), 1);
    assert_eq!(headers["x-safe"], "3");
}
#[test]
fn forwarding_formats_ipv6_without_truncation() {
    let ip = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap();
    assert_eq!(ip_header_value(ip).unwrap(), ip.to_string());
}
