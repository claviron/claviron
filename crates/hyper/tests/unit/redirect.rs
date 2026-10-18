use http::Request;

use super::*;

#[test]
fn preserves_ipv6_and_path_query_with_explicit_redirect_port() {
    for (host, port, expected) in [
        ("[::1]:8080", 443, "https://[::1]/path?a=1"),
        ("app.test:8080", 8443, "https://app.test:8443/path?a=1"),
    ] {
        let request = Request::builder()
            .uri(format!("http://{host}/path?a=1"))
            .header(header::HOST, host)
            .body(())
            .unwrap();
        let authority = RequestAuthority::from_cleartext(&request).unwrap();
        let response = handle_redirect(
            &authority,
            request.uri(),
            NonZeroU16::new(port).unwrap(),
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(response.headers()[header::LOCATION], expected);
        assert_eq!(response.headers()[header::CONNECTION], "close");
    }
}
