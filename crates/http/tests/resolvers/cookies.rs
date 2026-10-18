use http::HeaderValue;

use super::*;

#[test]
fn join_returns_none_when_absent() {
    let headers = HeaderMap::new();
    assert_eq!(join_cookie_header(&headers), None);
}

#[test]
fn single_cookie_unchanged() {
    let mut headers = HeaderMap::new();
    headers.insert(COOKIE, HeaderValue::from_static("a=1; b=2"));
    assert_eq!(join_cookie_header(&headers).as_deref(), Some("a=1; b=2"));
}

#[test]
fn http2_split_joined_with_separator() {
    let mut headers = HeaderMap::new();
    headers.append(COOKIE, HeaderValue::from_static("a=1"));
    headers.append(COOKIE, HeaderValue::from_static("b=2"));
    headers.append(COOKIE, HeaderValue::from_static("c=3"));
    assert_eq!(join_cookie_header(&headers).as_deref(), Some("a=1; b=2; c=3"));
}

#[test]
fn empty_value_skipped() {
    let mut headers = HeaderMap::new();
    headers.append(COOKIE, HeaderValue::from_static("a=1"));
    headers.append(COOKIE, HeaderValue::from_static(""));
    headers.append(COOKIE, HeaderValue::from_static("b=2"));
    assert_eq!(join_cookie_header(&headers).as_deref(), Some("a=1; b=2"));
}
