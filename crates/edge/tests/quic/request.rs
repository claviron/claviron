use super::*;

#[test]
fn interrupted_body_does_not_generate_a_bad_request_response() {
    assert_eq!(
        RequestBodyError { kind: BodyErrorKind::Interrupted }.response_status(),
        None
    );
    assert_eq!(
        RequestBodyError::malformed().response_status(),
        Some(StatusCode::BAD_REQUEST)
    );
}

#[test]
fn unsupported_websocket_connect_is_decided_from_the_head_only() {
    let mut request = Request::builder()
        .method(http::Method::CONNECT)
        .uri("https://example.test/socket")
        .body(())
        .unwrap();
    request.extensions_mut().insert(h3::ext::Protocol::WEBSOCKET);
    assert!(unsupported_websocket_connect(&request));
    request.extensions_mut().insert(h3::ext::Protocol::WEB_TRANSPORT);
    assert!(!unsupported_websocket_connect(&request));
    request.extensions_mut().remove::<h3::ext::Protocol>();
    assert!(!unsupported_websocket_connect(&request));
    *request.method_mut() = http::Method::GET;
    request.extensions_mut().insert(h3::ext::Protocol::WEBSOCKET);
    assert!(!unsupported_websocket_connect(&request));
}

#[test]
fn framing_validation_is_independent_of_body_policy() {
    for (value, expected) in [
        ("16", Ok(())),
        ("0", Ok(())),
        ("17000000", Ok(())),
        ("bad", Err(BodyErrorKind::Malformed)),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_LENGTH, value.parse().unwrap());
        assert_eq!(
            checked_request_framing(&headers).map_err(|e| e.kind),
            expected
        );
    }
    checked_request_framing(&HeaderMap::new()).unwrap();

    // Connection-level fields describe an H1 connection, not the message on a
    // QUIC stream, and RFC 9114 §4.2 makes carrying one malformed. Same set
    // h2 refuses, so the two transports answer a request the same way.
    for field in CONNECTION_SPECIFIC_FIELDS {
        let mut headers = HeaderMap::new();
        headers.insert(&field, "chunked".parse().unwrap());
        headers.insert(http::header::CONTENT_LENGTH, "1".parse().unwrap());
        assert_eq!(
            checked_request_framing(&headers).map_err(|e| e.kind),
            Err(BodyErrorKind::Malformed),
            "{field} should be refused"
        );
    }

    // `TE` is the exception H3 keeps, and only at this value.
    for (value, expected) in
        [("trailers", Ok(())), ("gzip", Err(BodyErrorKind::Malformed))]
    {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::TE, value.parse().unwrap());
        headers.insert(http::header::CONTENT_LENGTH, "1".parse().unwrap());
        assert_eq!(
            checked_request_framing(&headers).map_err(|e| e.kind),
            expected
        );
    }
}
