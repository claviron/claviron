use super::*;

#[test]
fn udp_port_sharing_is_only_enabled_for_workers() {
    let worker = RunMode::Worker(claviron_runtime::WorkerId::new(1));
    for mode in [RunMode::Standalone, RunMode::Master] {
        let socket = bind_socket("127.0.0.1:0".parse().unwrap(), mode).unwrap();
        let addr = socket.local_addr().unwrap();
        for competitor in [mode, worker] {
            assert_eq!(
                bind_socket(addr, competitor).unwrap_err().kind(),
                std::io::ErrorKind::AddrInUse
            );
        }
    }
    let first = bind_socket("127.0.0.1:0".parse().unwrap(), worker).unwrap();
    let second_worker = RunMode::Worker(claviron_runtime::WorkerId::new(2));
    let _second =
        bind_socket(first.local_addr().unwrap(), second_worker).unwrap();
}

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
