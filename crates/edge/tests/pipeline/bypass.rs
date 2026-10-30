use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use claviron_http::{RouteResponse, empty_body};

use super::*;

struct PendingReply {
    _buffer: [u8; 4096],
    _request: Request<RouteBody>,
    dropped: Arc<AtomicUsize>,
}

impl Future for PendingReply {
    type Output = Result<RouteResponse, std::convert::Infallible>;

    fn poll(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingReply {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct Handler {
    allocated: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl BypassHandler for Handler {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        assert_eq!(request.uri(), "/example");
        assert_eq!(request.headers()["x-example"], "forwarded");
        let future: RouteFuture = Box::pin(PendingReply {
            _buffer: [0; 4096],
            _request: request,
            dropped: self.dropped.clone(),
        });
        self.allocated.store(
            future.as_ref().get_ref() as *const _ as *const () as usize,
            Ordering::SeqCst,
        );
        future
    }
}

#[test]
fn large_backend_future_is_forwarded_without_a_dispatch_wrapper() {
    let allocated = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let service = BypassService::new(Handler {
        allocated: allocated.clone(),
        dropped: dropped.clone(),
    });
    assert!(!service.times_its_head());
    let request = Request::builder()
        .uri("/example")
        .header("x-example", "forwarded")
        .body(empty_body())
        .unwrap();
    let mut future = service.handle(request);
    let address = future.as_ref().get_ref() as *const _ as *const () as usize;
    assert_ne!(allocated.load(Ordering::SeqCst), 0);
    assert_eq!(address, allocated.load(Ordering::SeqCst));
    let mut context = Context::from_waker(Waker::noop());
    assert!(future.as_mut().poll(&mut context).is_pending());

    drop(service);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(future);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

struct SelfTimed;

impl BypassHandler for SelfTimed {
    fn times_its_head(&self) -> bool {
        true
    }

    fn call(
        &self,
        _: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(async { Ok(Response::new(empty_body())) })
    }
}

#[test]
fn handler_head_timing_ownership_is_preserved() {
    assert!(BypassService::new(SelfTimed).times_its_head());
    assert!(!BypassService::Noop.times_its_head());
}

#[test]
fn noop_keeps_its_empty_success_response() {
    use http_body_util::BodyExt;

    let mut future = BypassService::Noop.handle(Request::new(empty_body()));
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(response)) = future.as_mut().poll(&mut context) else {
        panic!("the empty handler must be ready immediately");
    };
    assert_eq!(response.status(), http::StatusCode::OK);
    let mut body = response.into_body();
    let mut frame = std::pin::pin!(body.frame());
    assert!(matches!(frame.as_mut().poll(&mut context), Poll::Ready(None)));
}
