use super::*;
use bytes::Bytes;
use http::{HeaderValue, Response, StatusCode};
use http_body::{Body, Frame};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct Counts {
    polls: AtomicUsize,
    drops: AtomicUsize,
    calls: AtomicUsize,
    clones: AtomicUsize,
}

struct Frames {
    frames: VecDeque<Frame<Bytes>>,
    counts: Arc<Counts>,
}

impl Body for Frames {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.counts.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn request(counts: &Arc<Counts>) -> (Request<RouteBody>, *const u8) {
    let bytes = Bytes::from(vec![0, 255, 13, 10]);
    let address = bytes.as_ptr();
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-example-end", HeaderValue::from_static("complete"));
    let body = Frames {
        frames: VecDeque::from([Frame::data(bytes), Frame::trailers(trailers)]),
        counts: counts.clone(),
    };
    (Request::new(claviron_http::boxed_body(body)), address)
}

struct Backend(Arc<Counts>);

impl Clone for Backend {
    fn clone(&self) -> Self {
        self.0.clones.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}

impl ApplicationHandler for Backend {
    fn call(
        self,
        request: Request<RouteBody>,
    ) -> impl Future<Output = RouteResponse> + Send {
        self.0.calls.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Response::new(request.into_body()))
    }
}

struct Pass(Arc<AtomicUsize>);

impl DispatchStep for Pass {
    fn call(
        &self,
        mut request: Request<RouteBody>,
    ) -> DispatchFuture {
        self.0.fetch_add(1, Ordering::Relaxed);
        request
            .headers_mut()
            .append("x-example-pass", HeaderValue::from_static("yes"));
        DispatchFuture::ready(ServiceResult::Unhandled(request))
    }
}

#[test]
fn unhandled_contributions_pass_original_data_and_trailers_without_reading() {
    let counts = Arc::new(Counts::default());
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    let pipeline = DispatchChain::new(
        Backend(counts.clone()),
        vec![Arc::new(Pass(first.clone())), Arc::new(Pass(second.clone()))],
    );
    let (request, address) = request(&counts);
    let mut answering = Box::pin(pipeline.handle(request));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(response) = answering.as_mut().poll(&mut cx) else {
        panic!("head processing must not wait for body frames");
    };
    drop(answering);
    assert_eq!(first.load(Ordering::Relaxed), 1);
    assert_eq!(second.load(Ordering::Relaxed), 1);
    assert_eq!(counts.calls.load(Ordering::Relaxed), 1);
    assert_eq!(counts.clones.load(Ordering::Relaxed), 1);
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    let mut body = response.into_body();
    let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut cx)
    else {
        panic!("the original data frame must remain available");
    };
    assert_eq!(frame.into_data().unwrap().as_ptr(), address);
    let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut cx)
    else {
        panic!("the original trailers must remain available");
    };
    assert_eq!(frame.into_trailers().unwrap()["x-example-end"], "complete");
    drop(body);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
}

struct Claim;

impl DispatchStep for Claim {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchFuture {
        let mut response = Response::new(request.into_body());
        *response.status_mut() = StatusCode::NOT_FOUND;
        DispatchFuture::ready(ServiceResult::Handled(response))
    }
}

#[test]
fn handled_404_does_not_run_later_contributions_or_clone_the_backend() {
    let counts = Arc::new(Counts::default());
    let later = Arc::new(AtomicUsize::new(0));
    let pipeline = DispatchChain::new(
        Backend(counts.clone()),
        vec![Arc::new(Claim), Arc::new(Pass(later.clone()))],
    );
    let (request, _) = request(&counts);
    let mut answering = Box::pin(pipeline.handle(request));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(response) = answering.as_mut().poll(&mut cx) else {
        panic!("the selected application must answer");
    };
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(later.load(Ordering::Relaxed), 0);
    assert_eq!(counts.calls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.clones.load(Ordering::Relaxed), 0);
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    drop(answering);
    drop(response);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
}

struct Pending(Arc<AtomicUsize>);

impl DispatchStep for Pending {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchFuture {
        self.0.fetch_add(1, Ordering::Relaxed);
        DispatchFuture::new(futures_util::future::poll_fn(move |_| {
            let _owned = &request;
            Poll::Pending
        }))
    }
}

#[test]
fn cancellation_releases_the_unread_body_before_and_during_dispatch() {
    for poll in [false, true] {
        let counts = Arc::new(Counts::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline = DispatchChain::new(
            Backend(counts.clone()),
            vec![Arc::new(Pending(calls.clone()))],
        );
        let generation = Arc::downgrade(&pipeline.plan);
        let (request, _) = request(&counts);
        let mut answering = Box::pin(pipeline.handle(request));
        if poll {
            assert!(
                answering
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert!(generation.upgrade().is_some());
        assert_eq!(calls.load(Ordering::Relaxed), usize::from(poll));
        assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
        assert_eq!(counts.drops.load(Ordering::Relaxed), 0);
        drop(answering);
        assert!(generation.upgrade().is_none());
        assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
        assert_eq!(counts.calls.load(Ordering::Relaxed), 0);
    }
}
