use super::*;
use crate::pipeline::host::{
    HostLayer, HostLayerBuilder, HostPipeline, HostPipelineBuilder,
};
use bytes::Bytes;
use claviron_http::{ServiceResult, boxed_body};
use http::{HeaderValue, Response};
use http_body::{Body, Frame};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct Counts {
    ready: AtomicBool,
    calls: AtomicUsize,
    owners: AtomicUsize,
    body_polls: AtomicUsize,
    body_drops: AtomicUsize,
    replies: AtomicUsize,
}

// The terminal API must accept the original non-Clone body without erasure.
struct OriginalBody {
    frame: Option<Bytes>,
    counts: Arc<Counts>,
}

impl Body for OriginalBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.counts.body_polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frame.take().map(|bytes| Ok(Frame::data(bytes))))
    }
}

impl Drop for OriginalBody {
    fn drop(&mut self) {
        self.counts.body_drops.fetch_add(1, Ordering::Relaxed);
    }
}

struct Owner(Arc<Counts>);

impl Drop for Owner {
    fn drop(&mut self) {
        self.0.owners.fetch_add(1, Ordering::Relaxed);
    }
}

struct Reply {
    request: Option<Request<OriginalBody>>,
    owner: Owner,
}

impl Future for Reply {
    type Output = RouteResponse;

    fn poll(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<RouteResponse> {
        if !self.owner.0.ready.load(Ordering::Relaxed) {
            return Poll::Pending;
        }
        let body = self.request.take().unwrap().into_body();
        Poll::Ready(Response::new(boxed_body(body)))
    }
}

impl ApplicationHandler<OriginalBody> for Owner {
    fn call(
        self,
        request: Request<OriginalBody>,
    ) -> impl Future<Output = RouteResponse> + Send {
        self.0.calls.fetch_add(1, Ordering::Relaxed);
        Reply { request: Some(request), owner: self }
    }
}

struct RecordReply(Arc<Counts>);

impl HostLayer for RecordReply {
    fn prepare(self, layer: &mut HostLayerBuilder) -> crate::Result<()> {
        layer.after(move |head, _| {
            self.0.replies.fetch_add(1, Ordering::Relaxed);
            head.headers.insert(
                "x-example-reply", HeaderValue::from_static("complete"),
            );
        });
        Ok(())
    }
}

fn pipeline(counts: &Arc<Counts>) -> HostPipeline {
    let mut builder = HostPipelineBuilder::new();
    builder.layer(RecordReply(counts.clone())).unwrap();
    builder.build()
}

fn request(counts: &Arc<Counts>) -> (Request<OriginalBody>, *const u8) {
    let bytes = Bytes::from(vec![0, 255, 13, 10]);
    let address = bytes.as_ptr();
    (
        Request::new(OriginalBody { frame: Some(bytes), counts: counts.clone() }),
        address,
    )
}

#[test]
fn typed_application_is_not_called_before_first_poll() {
    let counts = Arc::new(Counts::default());
    let pipeline = pipeline(&counts);
    let (request, _) = request(&counts);
    let ServiceResult::Unhandled(request) = pipeline.prepare(request) else {
        panic!("the terminal owner must receive the unclaimed request");
    };
    let answering = request.dispatch_to(Owner(counts.clone()));
    assert_eq!(counts.calls.load(Ordering::Relaxed), 0);
    drop(answering);
    assert_eq!(counts.owners.load(Ordering::Relaxed), 1);
    assert_eq!(counts.body_drops.load(Ordering::Relaxed), 1);
    assert_eq!(counts.body_polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.replies.load(Ordering::Relaxed), 0);
}

#[test]
fn pending_typed_application_cancellation_drops_body_and_owner_once() {
    let counts = Arc::new(Counts::default());
    let pipeline = pipeline(&counts);
    let (request, _) = request(&counts);
    let ServiceResult::Unhandled(request) = pipeline.prepare(request) else {
        panic!("the terminal owner must receive the unclaimed request");
    };
    let mut answering = Box::pin(request.dispatch_to(Owner(counts.clone())));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(answering.as_mut().poll(&mut cx).is_pending());
    assert_eq!(counts.calls.load(Ordering::Relaxed), 1);
    assert_eq!(counts.owners.load(Ordering::Relaxed), 0);
    assert_eq!(counts.body_drops.load(Ordering::Relaxed), 0);
    drop(answering);
    assert_eq!(counts.owners.load(Ordering::Relaxed), 1);
    assert_eq!(counts.body_drops.load(Ordering::Relaxed), 1);
    assert_eq!(counts.body_polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.replies.load(Ordering::Relaxed), 0);
}

#[test]
fn typed_application_transfers_original_payload_and_finishes_response_scope() {
    let counts = Arc::new(Counts::default());
    counts.ready.store(true, Ordering::Relaxed);
    let pipeline = pipeline(&counts);
    let (request, address) = request(&counts);
    let ServiceResult::Unhandled(request) = pipeline.prepare(request) else {
        panic!("the terminal owner must receive the unclaimed request");
    };
    let mut answering = Box::pin(request.dispatch_to(Owner(counts.clone())));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(response) = answering.as_mut().poll(&mut cx) else {
        panic!("reply-head delivery must not read the request body");
    };
    drop(answering);
    assert_eq!(counts.calls.load(Ordering::Relaxed), 1);
    assert_eq!(counts.owners.load(Ordering::Relaxed), 1);
    assert_eq!(counts.body_polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.body_drops.load(Ordering::Relaxed), 0);
    assert_eq!(counts.replies.load(Ordering::Relaxed), 1);
    assert_eq!(response.headers()["x-example-reply"], "complete");

    let mut body = response.into_body();
    let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut cx) else {
        panic!("the original payload must remain available in the response");
    };
    let bytes = frame.into_data().unwrap();
    assert_eq!(bytes.as_ptr(), address);
    assert_eq!(bytes.as_ref(), &[0, 255, 13, 10]);
    drop(body);
    assert_eq!(counts.body_polls.load(Ordering::Relaxed), 1);
    assert_eq!(counts.body_drops.load(Ordering::Relaxed), 1);
}
