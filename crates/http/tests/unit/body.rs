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

#[tokio::test(start_paused = true)]
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

#[test]
fn specialized_body_guards_preserve_ready_frames_without_a_reactor() {
    use super::{guard_idle, guard_size, guard_size_and_idle};
    use futures_util::FutureExt;
    for mode in 0..3 {
        let payload = Bytes::from(vec![0, 1, 2, 3]);
        let pointer = payload.as_ptr();
        let body = crate::boxed_body(Full::new(payload));
        let mut body = match mode {
            0 => guard_size(body, 4),
            1 => guard_idle(body, Duration::from_secs(1)),
            _ => guard_size_and_idle(body, 4, Duration::from_secs(1)),
        };
        let frame = body.frame().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!(frame.data_ref().unwrap().as_ptr(), pointer);
        assert!(body.frame().now_or_never().unwrap().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn specialized_body_idle_guard_rearms_after_progress() {
    use futures_util::{FutureExt, stream};
    use std::task::{Context, Waker};
    let timeout = Duration::from_secs(2);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let stream = stream::poll_fn(move |cx| receiver.poll_recv(cx));
    let mut body =
        super::guard_idle(crate::boxed_body(StreamBody::new(stream)), timeout);
    let frame = body.frame();
    let mut frame = std::pin::pin!(frame);
    assert!(
        frame
            .as_mut()
            .poll_unpin(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tokio::time::advance(timeout / 2).await;
    sender
        .try_send(Ok::<_, Infallible>(http_body::Frame::data(
            Bytes::from_static(b"progress"),
        )))
        .unwrap();
    assert!(frame.await.unwrap().unwrap().is_data());
    let frame = body.frame();
    let mut frame = std::pin::pin!(frame);
    assert!(
        frame
            .as_mut()
            .poll_unpin(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tokio::time::advance(timeout / 2).await;
    assert!(
        frame
            .as_mut()
            .poll_unpin(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tokio::time::advance(timeout / 2).await;
    let error = frame.await.unwrap().unwrap_err();
    assert_eq!(
        error.downcast_ref::<BodyLimit>(),
        Some(&BodyLimit::Stalled { after: timeout })
    );
}

#[test]
fn a_size_guard_never_admits_later_bytes_after_an_overrun() {
    use futures_util::FutureExt;
    use http_body::Frame;
    let stream = futures_util::stream::iter([
        Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"oversized"))),
        Ok(Frame::data(Bytes::from_static(b"x"))),
    ]);
    let mut body =
        super::guard_size(crate::boxed_body(StreamBody::new(stream)), 4);
    for _ in 0..2 {
        let error = body.frame().now_or_never().unwrap().unwrap().unwrap_err();
        assert_eq!(
            error.downcast_ref::<BodyLimit>(),
            Some(&BodyLimit::TooLarge { limit: 4 })
        );
    }
}
