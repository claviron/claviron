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
    assert_eq!(
        RequestBodyError::too_large().response_status(),
        Some(StatusCode::PAYLOAD_TOO_LARGE)
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
fn declared_body_limits_reject_before_body_adaptation() {
    for (value, expected) in [
        ("16", Ok(Some(16))),
        ("0", Ok(Some(0))),
        ("17", Err(BodyErrorKind::TooLarge)),
        ("bad", Err(BodyErrorKind::Malformed)),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_LENGTH, value.parse().unwrap());
        assert_eq!(
            checked_request_framing(&headers, 16).map_err(|e| e.kind()),
            expected
        );
    }
    assert_eq!(checked_request_framing(&HeaderMap::new(), 0).unwrap(), None);

    // Connection-level fields describe an H1 connection, not the message on a
    // QUIC stream, and RFC 9114 §4.2 makes carrying one malformed. Same set
    // h2 refuses, so the two transports answer a request the same way.
    for field in CONNECTION_SPECIFIC_FIELDS {
        let mut headers = HeaderMap::new();
        headers.insert(&field, "chunked".parse().unwrap());
        headers.insert(http::header::CONTENT_LENGTH, "1".parse().unwrap());
        assert_eq!(
            checked_request_framing(&headers, 16).map_err(|e| e.kind()),
            Err(BodyErrorKind::Malformed),
            "{field} should be refused"
        );
    }

    // `TE` is the exception H3 keeps, and only at this value.
    for (value, expected) in
        [("trailers", Ok(Some(1))), ("gzip", Err(BodyErrorKind::Malformed))]
    {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::TE, value.parse().unwrap());
        headers.insert(http::header::CONTENT_LENGTH, "1".parse().unwrap());
        assert_eq!(
            checked_request_framing(&headers, 16).map_err(|e| e.kind()),
            expected
        );
    }
}

#[test]
fn shared_decisions_keep_h3_wire_status_contract() {
    assert_eq!(
        host_response(RequestRejection::PathDenied.into()).status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        host_response(RequestRejection::AccessDenied.into()).status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        host_response(RequestRejection::HostRemoved.into()).status(),
        StatusCode::MISDIRECTED_REQUEST
    );
    assert_eq!(
        host_response(RequestRejection::AmbiguousPath.into()).status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        host_response(RequestRejection::TooEarly.into()).status(),
        StatusCode::TOO_EARLY
    );
    assert_eq!(
        host_response(RequestRejection::Misdirected.into()).status(),
        StatusCode::MISDIRECTED_REQUEST
    );
}

#[test]
fn host_redirect_keeps_location() {
    let response = host_response(HostResponse::Redirect {
        status: StatusCode::TEMPORARY_REDIRECT,
        location: http::HeaderValue::from_static(
            "https://new.test/old?raw=%2F",
        ),
    });
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        response.headers()[http::header::LOCATION],
        "https://new.test/old?raw=%2F"
    );
}
