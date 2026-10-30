use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use super::*;
use http_body::Body;

static PAYLOAD: &[u8] = b"payload";

struct WatchedBody {
    data: Option<Bytes>,
    dropped: Arc<AtomicUsize>,
}

impl Body for WatchedBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Infallible>>> {
        Poll::Ready(
            self.data.take().map(|data| Ok(hyper::body::Frame::data(data))),
        )
    }
}

impl Drop for WatchedBody {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

struct LargeReply {
    _buffer: [u8; 4096],
    request: Option<Request<RouteBody>>,
    ready: bool,
    dropped: Arc<AtomicUsize>,
}

impl Future for LargeReply {
    type Output = RouteResponse;

    fn poll(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<RouteResponse> {
        if !self.ready {
            return Poll::Pending;
        }
        let body = self.request.take().unwrap().into_body();
        Poll::Ready(
            Response::builder().status(StatusCode::CREATED).body(body).unwrap(),
        )
    }
}

impl Drop for LargeReply {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

fn reply(
    ready: bool,
    future_drop: Arc<AtomicUsize>,
    body_drop: Arc<AtomicUsize>,
) -> LargeReply {
    LargeReply {
        _buffer: [0; 4096],
        request: Some(Request::new(boxed_body(WatchedBody {
            data: Some(Bytes::from_static(PAYLOAD)),
            dropped: body_drop,
        }))),
        ready,
        dropped: future_drop,
    }
}

#[test]
fn forwarding_keeps_one_large_future_and_cancel_drops_its_body_once() {
    for poll_first in [false, true] {
        let future_drop = Arc::new(AtomicUsize::new(0));
        let body_drop = Arc::new(AtomicUsize::new(0));
        let answering = reply(false, future_drop.clone(), body_drop.clone());
        let size = std::mem::size_of_val(&answering);
        let forwarding = response_future(answering);
        assert!(std::mem::size_of_val(&forwarding) < size * 2);
        let mut forwarding = Box::pin(forwarding);
        if poll_first {
            let mut context = Context::from_waker(Waker::noop());
            assert!(forwarding.as_mut().poll(&mut context).is_pending());
        }
        assert_eq!(future_drop.load(Ordering::Relaxed), 0);
        assert_eq!(body_drop.load(Ordering::Relaxed), 0);
        drop(forwarding);
        assert_eq!(future_drop.load(Ordering::Relaxed), 1);
        assert_eq!(body_drop.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn forwarding_transfers_response_body_ownership_without_copying_payload() {
    let future_drop = Arc::new(AtomicUsize::new(0));
    let body_drop = Arc::new(AtomicUsize::new(0));
    let answering = reply(true, future_drop.clone(), body_drop.clone());
    let mut forwarding = Box::pin(response_future(answering));
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(response)) = forwarding.as_mut().poll(&mut context)
    else {
        panic!("a ready response must be forwarded immediately");
    };
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(future_drop.load(Ordering::Relaxed), 1);
    drop(forwarding);
    assert_eq!(future_drop.load(Ordering::Relaxed), 1);
    assert_eq!(body_drop.load(Ordering::Relaxed), 0);
    let mut body = response.into_body();
    let Poll::Ready(Some(Ok(frame))) =
        Pin::new(&mut body).poll_frame(&mut context)
    else {
        panic!("the response body must still own its payload");
    };
    let bytes = frame.into_data().unwrap();
    assert_eq!(bytes.as_ptr(), PAYLOAD.as_ptr());
    assert_eq!(bytes, "payload");
    drop(body);
    assert_eq!(body_drop.load(Ordering::Relaxed), 1);
}
