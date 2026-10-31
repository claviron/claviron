use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use claviron_http::{boxed_body, empty_body};
use http::StatusCode;
use http_body::{Body, Frame};

use super::*;

#[test]
fn reply_preparation_registers_only_declared_body_limits() {
    assert!(ReplyPolicy::new(ResponsePolicy::unlimited()).is_none());
    assert!(ReplyPolicy::new(ResponsePolicy {
        write_timeout: Some(Duration::from_secs(1)),
        buffer_size: Some(1024),
        ..ResponsePolicy::unlimited()
    }).is_none());
    let policy = ReplyPolicy::new(ResponsePolicy {
        max_body_size: Some(7),
        ..ResponsePolicy::unlimited()
    }).unwrap();
    let mut builder = HostPipelineBuilder::<()>::new();
    builder.layer(policy).unwrap();
    let pipeline = builder.build();
    assert!(pipeline.before.is_empty());
    assert!(pipeline.after.is_empty());
    assert_eq!(pipeline.body.len(), 1);

    // Even a declared idle timer is not created at preparation or reply head.
    let policy = ReplyPolicy::new(ResponsePolicy {
        body_idle_timeout: Some(Duration::from_secs(1)),
        ..ResponsePolicy::unlimited()
    }).unwrap();
    let response = policy.finish(RouteResponse::new(empty_body()));
    assert!(response.body().is_end_stream());
}

struct Frames {
    frames: VecDeque<Frame<Bytes>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Body for Frames {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn direct_and_composed_reply_guards_keep_frames_trailers_and_drop_ownership() {
    static PAYLOAD: &[u8] = b"payload";
    for composed in [false, true] {
        let policy = ReplyPolicy::new(ResponsePolicy {
            max_body_size: Some(PAYLOAD.len()),
            ..ResponsePolicy::unlimited()
        }).unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let make_response = || {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-finished", http::HeaderValue::from_static("yes"));
            let body = boxed_body(Frames {
                frames: VecDeque::from([
                    Frame::data(Bytes::from_static(PAYLOAD)),
                    Frame::trailers(trailers),
                ]),
                polls: polls.clone(),
                drops: drops.clone(),
            });
            Response::builder().status(StatusCode::CREATED)
                .header("x-example", "kept").body(body).unwrap()
        };
        let response = if composed {
            let mut builder = HostPipelineBuilder::<()>::new();
            builder.layer(policy).unwrap();
            let pipeline = builder.build();
            let (mut head, ()) = Request::new(()).into_parts();
            let (reply, scope) = pipeline.prepare_head(&mut head, ());
            assert!(reply.is_none());
            scope.finish(make_response())
        } else {
            policy.finish(make_response())
        };
        assert_eq!(polls.load(Ordering::Relaxed), 0);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-example"], "kept");
        let mut body = response.into_body();
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut cx) else {
            panic!("the data frame must be forwarded");
        };
        assert_eq!(frame.into_data().unwrap().as_ptr(), PAYLOAD.as_ptr());
        let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut cx) else {
            panic!("the trailers must be forwarded");
        };
        assert_eq!(frame.into_trailers().unwrap()["x-finished"], "yes");
        drop(body);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        // Cancellation before the first body poll still drops the source once.
        drop(policy.finish(make_response()));
        assert_eq!(polls.load(Ordering::Relaxed), 2);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}
