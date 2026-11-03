use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

use bytes::Bytes;
use http_body::{Body, Frame};

use super::*;
use crate::pipeline::host::HostRequest;

struct UnreadBody(Arc<AtomicUsize>);

impl Body for UnreadBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        panic!("dispatch must not poll an unclaimed body");
    }
}

impl Drop for UnreadBody {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn request(dropped: Arc<AtomicUsize>) -> Request<RouteBody> {
    Request::builder()
        .uri("/example")
        .header("x-example", "original")
        .body(claviron_http::boxed_body(UnreadBody(dropped)))
        .unwrap()
}

#[test]
fn ready_fallthrough_keeps_the_unread_request() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let call =
        DispatchCall::ready(ServiceResult::Unhandled(request(dropped.clone())));
    let DispatchCall::Ready(ServiceResult::Unhandled(request)) = call else {
        panic!("ready fallthrough must remain synchronous");
    };
    assert_eq!(request.uri(), "/example");
    assert_eq!(request.headers()["x-example"], "original");
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(request);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

struct PendingRequest {
    _request: Request<RouteBody>,
    _buffer: [u8; 4096],
}

impl Future for PendingRequest {
    type Output = DispatchResult;

    fn poll(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        Poll::Pending
    }
}

#[test]
fn pending_state_stays_small_and_cancellation_releases_the_request() {
    // A ready Request must never become part of the suspended future again.
    assert!(size_of::<DispatchFuture>() <= 4 * size_of::<usize>());
    let dropped = Arc::new(AtomicUsize::new(0));
    let call = DispatchCall::new(PendingRequest {
        _request: request(dropped.clone()),
        _buffer: [0; 4096],
    });
    let DispatchCall::Pending(mut future) = call else {
        panic!("pending request must expose only its future");
    };
    assert!(
        Pin::new(&mut future)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(future);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn application_future_reuses_its_erasure_and_returns_the_request() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let original: HostHandlerFuture = Box::pin(std::future::ready(
        ServiceResult::Unhandled(HostRequest::new(request(dropped.clone()))),
    ));
    let address = original.as_ref().get_ref() as *const _ as *const ();
    let DispatchCall::Pending(mut future) = DispatchCall::application(original)
    else {
        panic!("application call must retain its existing future");
    };
    let Work::Application(stored) = &future.inner else {
        panic!("application erasure must not be wrapped");
    };
    assert_eq!(address, stored.as_ref().get_ref() as *const _ as *const ());
    let Poll::Ready(ServiceResult::Unhandled(request)) =
        Pin::new(&mut future).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("application must return its original request");
    };
    assert_eq!(request.uri(), "/example");
    drop(future);
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(request);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}
