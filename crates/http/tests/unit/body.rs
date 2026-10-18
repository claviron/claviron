use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use http::header::CONTENT_LENGTH;
use http_body_util::{BodyExt, Full, StreamBody};

use super::{BodyLimit, GuardedBody, declared_length_within};

#[test]
fn a_declared_length_over_the_limit_is_refused_before_reading() {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_LENGTH, "17".parse().unwrap());

    assert_eq!(
        declared_length_within(&headers, Some(16)),
        Err(BodyLimit::TooLarge { limit: 16 })
    );
    assert_eq!(declared_length_within(&headers, Some(17)), Ok(()));
}

#[test]
fn an_absent_or_malformed_length_is_left_to_the_counting() {
    let mut headers = HeaderMap::new();
    assert_eq!(declared_length_within(&headers, Some(1)), Ok(()));

    // Not ours to reject, and calling it oversized would name the wrong fault.
    headers.insert(CONTENT_LENGTH, "banana".parse().unwrap());
    assert_eq!(declared_length_within(&headers, Some(1)), Ok(()));
}

#[tokio::test]
async fn a_body_within_the_limit_passes_through_whole() {
    let body = GuardedBody::new(
        Full::<Bytes>::new(Bytes::from_static(b"hello")),
        Some(16),
        None,
    );

    assert_eq!(
        body.collect().await.unwrap().to_bytes(),
        Bytes::from_static(b"hello")
    );
}

#[tokio::test]
async fn an_undeclared_body_past_the_limit_fails_while_being_read() {
    let body = GuardedBody::new(
        Full::<Bytes>::new(Bytes::from_static(b"hello")),
        Some(4),
        None,
    );
    let error = body.collect().await.unwrap_err();

    assert_eq!(
        error.downcast_ref::<BodyLimit>(),
        Some(&BodyLimit::TooLarge { limit: 4 })
    );
}

#[tokio::test]
async fn a_body_that_stops_arriving_fails_on_the_read_timeout() {
    // Never yields a frame and never ends: the shape of a peer holding the
    // connection open without sending.
    let stalled = StreamBody::new(futures_util::stream::pending::<
        Result<http_body::Frame<Bytes>, Infallible>,
    >());
    let body =
        GuardedBody::new(stalled, Some(1024), Some(Duration::from_millis(50)));
    let error = body.collect().await.unwrap_err();

    assert!(matches!(
        error.downcast_ref::<BodyLimit>(),
        Some(BodyLimit::Stalled { .. })
    ));
}
