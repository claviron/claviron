use super::super::dispatch::{Control, Mailbox};
use super::*;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn request() -> Request<RouteBody> {
    Request::builder()
        .uri("/example")
        .extension(claviron_http::policy::HttpPolicy::unlimited())
        .body(claviron_http::empty_body())
        .unwrap()
}

#[test]
fn application_request_busy_inbox_is_fixed_and_cancelled_wakeup_passes_to_next_caller()
 {
    let requests = ApplicationRequests::new(3, Arc::new(Notify::new()));
    let (sender, mut receiver) = mpsc::channel(1);
    let control = Arc::new(Control::new(
        Arc::clone(&requests.wake),
        Arc::clone(&requests.lifecycle),
    ));
    control.publish_ready();
    requests
        .dispatch
        .store(Arc::new(vec![Mailbox::new(&sender, Arc::clone(&control))]));
    let mut active = Box::pin(requests.execute(request()));
    let mut cancelled = Box::pin(requests.execute(request()));
    let mut queued = Box::pin(requests.execute(request()));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(active.as_mut().poll(&mut cx).is_pending());
    let job = receiver.try_recv().unwrap();
    assert!(job.request.body().is_empty());
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    assert!(queued.as_mut().poll(&mut cx).is_pending());
    assert_eq!(requests.dispatch.load().len(), 1);
    assert_eq!(requests.admission.available_permits(), 0);
    job.reply
        .send(Ok(Response::new(ApplicationBody::Whole(Bytes::new()))))
        .ok()
        .unwrap();
    control.complete(true);
    // The first notified waiter disappears without claiming the free instance.
    drop(cancelled);
    assert_eq!(requests.admission.available_permits(), 1);
    assert!(queued.as_mut().poll(&mut cx).is_pending());
    let job = receiver
        .try_recv()
        .expect("cancelled wake must reach the next queued caller");
    job.reply
        .send(Ok(Response::new(ApplicationBody::Whole(Bytes::new()))))
        .ok()
        .unwrap();
    control.complete(true);
    assert!(matches!(active.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));
    assert!(matches!(queued.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));
    assert_eq!(requests.admission.available_permits(), 3);
}

#[test]
fn application_request_stopped_inbox_wakes_a_queued_caller() {
    let requests = ApplicationRequests::new(1, Arc::new(Notify::new()));
    let mut waiting = Box::pin(requests.execute(request()));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    requests.stop();
    assert!(matches!(
        waiting.as_mut().poll(&mut cx),
        Poll::Ready(Err(SapiGatewayError::ServiceUnavailable(_)))
    ));
    assert_eq!(requests.admission.available_permits(), 1);
}

#[test]
fn application_request_cancelled_body_reader_releases_admission() {
    let requests = ApplicationRequests::new(1, Arc::new(Notify::new()));
    let body =
        http_body_util::StreamBody::new(futures_util::stream::pending::<
            Result<http_body::Frame<Bytes>, std::convert::Infallible>,
        >());
    let request = Request::builder()
        .method(Method::POST)
        .uri("/example")
        .extension(claviron_http::policy::HttpPolicy::unlimited())
        .body(claviron_http::boxed_body(body))
        .unwrap();
    let mut buffering = Box::pin(requests.execute(request));
    assert!(
        buffering
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(requests.admission.available_permits(), 0);
    drop(buffering);
    assert_eq!(requests.admission.available_permits(), 1);
}
