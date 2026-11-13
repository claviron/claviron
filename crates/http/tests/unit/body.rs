use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use http::header::CONTENT_LENGTH;
use http_body_util::{BodyExt, Full, StreamBody};

use super::{BodyLimit, GuardedBody, declared_length_within};

#[test]
fn reply_delivery_holds_a_complete_frame_and_streams_on_overflow() {
    use futures_util::FutureExt;
    use http::{Method, Response};
    use http_body::{Body, Frame};

    let policy = crate::policy::ResponsePolicy {
        buffering: crate::policy::Buffering::BufferDefault,
        buffer_size: Some(2),
        ..crate::policy::ResponsePolicy::unlimited()
    };
    let first = Bytes::from_static(b"OK");
    let pointer = first.as_ptr();
    let one =
        StreamBody::new(futures_util::stream::iter([Ok::<_, Infallible>(
            Frame::data(first.clone()),
        )]));
    let response = super::deliver_response(
        Response::new(crate::boxed_body(one)),
        &Method::GET,
        &policy,
    )
    .now_or_never()
    .unwrap();
    assert_eq!(response.body().size_hint().exact(), Some(2));
    let held = response.into_body().collect().now_or_never().unwrap().unwrap();
    assert_eq!(held.to_bytes().as_ptr(), pointer);

    let extra = Bytes::from_static(b"extra");
    let tail =
        futures_util::stream::pending::<Result<Frame<Bytes>, Infallible>>();
    use futures_util::StreamExt;
    let source = futures_util::stream::iter([
        Ok(Frame::data(first)),
        Ok(Frame::data(extra.clone())),
    ])
    .chain(tail);
    let mut response = super::deliver_response(
        Response::new(crate::boxed_body(StreamBody::new(source))),
        &Method::GET,
        &policy,
    )
    .now_or_never()
    .expect("overflow returns without polling the rest");
    let body = response.body_mut();
    for expected in [pointer, extra.as_ptr()] {
        let data = body.frame().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!(data.data_ref().unwrap().as_ptr(), expected);
    }
    assert!(body.frame().now_or_never().is_none());
}

#[test]
fn reply_delivery_streaming_signals_leave_the_body_unpolled() {
    use futures_util::FutureExt;
    use http::{Method, Response};
    use http_body::Frame;

    let policy = crate::policy::ResponsePolicy {
        buffering: crate::policy::Buffering::BufferDefault,
        buffer_size: Some(4),
        ..crate::policy::ResponsePolicy::unlimited()
    };
    for (name, value) in [
        ("content-type", "text/event-stream; charset=utf-8"),
        ("content-type", "application/grpc+proto"),
        (crate::policy::X_ACCEL_BUFFERING, "no"),
    ] {
        let stream =
            futures_util::stream::pending::<Result<Frame<Bytes>, Infallible>>();
        let response = Response::builder()
            .header(name, value)
            .body(crate::boxed_body(StreamBody::new(stream)))
            .unwrap();
        let response = super::deliver_response(response, &Method::GET, &policy)
            .now_or_never()
            .expect("explicit streaming returns before the first frame");
        assert!(
            !response.headers().contains_key(crate::policy::X_ACCEL_BUFFERING)
        );
    }

    let stream =
        futures_util::stream::pending::<Result<Frame<Bytes>, Infallible>>();
    let mut response = Response::new(crate::boxed_body(StreamBody::new(stream)));
    response.extensions_mut().insert(super::ExplicitStream);
    let response = super::deliver_response(response, &Method::GET, &policy)
        .now_or_never()
        .expect("the producer's streaming decision bypasses buffering");
    assert!(response.extensions().get::<super::ExplicitStream>().is_none());

    let stream =
        futures_util::stream::pending::<Result<Frame<Bytes>, Infallible>>();
    let response = Response::new(crate::boxed_body(StreamBody::new(stream)));
    super::deliver_response(response, &Method::HEAD, &policy)
        .now_or_never()
        .expect("HEAD must not poll a response body for buffering");
}

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

#[test]
fn ordinary_replies_do_not_construct_shutdown_state() {
    use futures_util::FutureExt;
    let bytes = Bytes::from_static(b"ordinary reply");
    let pointer = bytes.as_ptr();
    let response = http::Response::new(crate::bytes_body(bytes));
    let constructed = std::cell::Cell::new(false);
    let mut response = super::ends_at_shutdown(response, || {
        constructed.set(true);
        std::future::pending::<()>()
    });
    assert!(!constructed.get());
    let frame =
        response.body_mut().frame().now_or_never().unwrap().unwrap().unwrap();
    assert_eq!(frame.data_ref().unwrap().as_ptr(), pointer);
}

#[test]
fn event_stream_shutdown_releases_its_body_before_the_wrapper_is_dropped() {
    use http_body::{Body, Frame, SizeHint};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::task::{Context, Poll, Waker};

    struct Retained(Arc<AtomicBool>);
    impl Drop for Retained {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    impl Body for Retained {
        type Data = Bytes;
        type Error = Infallible;
        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            Poll::Pending
        }
        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(16)
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let (shutdown, cancelled) = tokio::sync::oneshot::channel();
    let response = http::Response::builder()
        .header(http::header::CONTENT_TYPE, "text/event-stream")
        .body(crate::boxed_body(Retained(dropped.clone())))
        .unwrap();
    let mut response = super::ends_at_shutdown(response, || async {
        let _ = cancelled.await;
    });
    let mut context = Context::from_waker(Waker::noop());
    assert!(
        std::pin::Pin::new(response.body_mut())
            .poll_frame(&mut context)
            .is_pending()
    );
    assert!(!dropped.load(Ordering::Relaxed));
    shutdown.send(()).unwrap();
    assert!(matches!(
        std::pin::Pin::new(response.body_mut()).poll_frame(&mut context),
        Poll::Ready(None)
    ));
    assert!(dropped.load(Ordering::Relaxed));
    assert!(response.body().is_end_stream());
    assert_eq!(response.body().size_hint().exact(), Some(0));
}

#[test]
fn event_stream_wrapper_preserves_frames_and_trailers() {
    use futures_util::FutureExt;
    use http_body::Frame;
    let bytes = Bytes::from_static(b"data: original\n\n");
    let pointer = bytes.as_ptr();
    let mut trailers = HeaderMap::new();
    trailers.insert("x-finished", "yes".parse().unwrap());
    let frames = futures_util::stream::iter([
        Ok::<_, Infallible>(Frame::data(bytes)),
        Ok(Frame::trailers(trailers.clone())),
    ]);
    let response = http::Response::builder()
        .header(http::header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .body(crate::boxed_body(StreamBody::new(frames)))
        .unwrap();
    let mut response =
        super::ends_at_shutdown(response, std::future::pending::<()>);
    let frame =
        response.body_mut().frame().now_or_never().unwrap().unwrap().unwrap();
    assert_eq!(frame.data_ref().unwrap().as_ptr(), pointer);
    let frame =
        response.body_mut().frame().now_or_never().unwrap().unwrap().unwrap();
    assert_eq!(frame.trailers_ref(), Some(&trailers));
    assert!(response.body_mut().frame().now_or_never().unwrap().is_none());
}
