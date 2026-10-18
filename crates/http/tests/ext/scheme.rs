use super::*;

#[test]
fn accepts_only_explicit_http_schemes_without_a_server_connection() {
    let mut extensions = http::Extensions::new();
    assert_eq!(request_scheme(&extensions), None);
    extensions.insert(http::uri::Scheme::HTTPS);
    assert_eq!(request_scheme(&extensions), Some(http::uri::Scheme::HTTPS));
    extensions.insert("custom".parse::<http::uri::Scheme>().unwrap());
    assert_eq!(request_scheme(&extensions), None);
}
