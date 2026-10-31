use super::*;

fn request(
    method: Method,
    content_length: Option<&str>,
) -> http::Request<()> {
    let mut builder = http::Request::builder().method(method).uri("/");
    if let Some(content_length) = content_length {
        builder = builder.header(http::header::CONTENT_LENGTH, content_length);
    }
    builder.body(()).unwrap()
}

#[test]
fn streamed_request_body_is_only_required_when_a_body_is_expected() {
    assert!(!requires_streamed_request_body(&request(Method::GET, None)));
    assert!(!requires_streamed_request_body(&request(Method::HEAD, None)));
    assert!(!requires_streamed_request_body(&request(Method::OPTIONS, None)));
    assert!(!requires_streamed_request_body(&request(Method::GET, Some("0"))));
    assert!(!requires_streamed_request_body(&request(
        Method::OPTIONS,
        Some("0")
    )));

    assert!(requires_streamed_request_body(&request(Method::GET, Some("5"))));
    assert!(requires_streamed_request_body(&request(
        Method::OPTIONS,
        Some("5")
    )));
    assert!(requires_streamed_request_body(&request(Method::POST, None)));
    assert!(requires_streamed_request_body(&request(Method::POST, Some("5"))));
    assert!(requires_streamed_request_body(&request(Method::PUT, None)));
    assert!(requires_streamed_request_body(&request(Method::PATCH, None)));

    assert!(!requires_streamed_request_body(&request(Method::POST, Some("0"))));
}
