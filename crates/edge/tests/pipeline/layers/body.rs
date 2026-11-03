use super::*;
use crate::pipeline::host::HostPipelineBuilder;
use bytes::Bytes;
use futures_util::FutureExt;
use http::{Request, Response, StatusCode};
use http_body::{Body, Frame};
use http_body_util::{BodyExt, StreamBody};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};

struct Unread(Arc<AtomicUsize>);
impl Body for Unread {
    type Data = Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        panic!("a refused length must not poll the body");
    }
}
impl Drop for Unread {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn body_layer_refuses_declared_size_before_consuming_any_frame() {
    let mut policy = RequestPolicy::unlimited();
    policy.max_body_size = Some(4);
    let mut builder = HostPipelineBuilder::new();
    builder.layer(RequestBodyLayer::new(policy).unwrap()).unwrap();
    let pipeline = builder.build();
    let drops = Arc::new(AtomicUsize::new(0));
    let request = Request::builder()
        .header("content-length", "5")
        .body(claviron_http::boxed_body(Unread(drops.clone())))
        .unwrap();
    let prepared = pipeline.prepare(request);
    let ServiceResult::Unhandled(prepared) = prepared else {
        panic!("consuming action is dispatched by the driver");
    };
    let response = prepared
        .dispatch(|_| async {
            panic!("oversized body must not reach the handler")
        })
        .now_or_never()
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn body_layer_guards_frames_without_copying_and_keeps_trailers() {
    let payload = Bytes::from(vec![1, 2, 3, 4]);
    let pointer = payload.as_ptr();
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-example-trailer", "end".parse().unwrap());
    let body = claviron_http::boxed_body(StreamBody::new(
        futures_util::stream::iter([
            Ok::<_, std::convert::Infallible>(Frame::data(payload)),
            Ok(Frame::trailers(trailers)),
        ]),
    ));
    let mut policy = RequestPolicy::unlimited();
    policy.max_body_size = Some(4);
    policy.body_read_timeout = Some(Duration::from_secs(1));
    let mut builder = HostPipelineBuilder::new();
    builder.layer(RequestBodyLayer::new(policy).unwrap()).unwrap();
    let pipeline = builder.build();
    let ServiceResult::Unhandled(prepared) =
        pipeline.prepare(Request::new(body))
    else {
        panic!("header admission must keep the body");
    };
    // Immediate frames use neither a Tokio reactor nor an idle timer.
    let response = prepared
        .dispatch(|request| async { Response::new(request.into_body()) })
        .now_or_never()
        .unwrap();
    let mut body = response.into_body();
    let data = body
        .frame()
        .now_or_never()
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(data.as_ptr(), pointer);
    let trailers = body
        .frame()
        .now_or_never()
        .unwrap()
        .unwrap()
        .unwrap()
        .into_trailers()
        .unwrap();
    assert_eq!(trailers["x-example-trailer"], "end");
    assert!(body.frame().now_or_never().unwrap().is_none());
}

#[test]
fn body_layer_size_only_has_no_timer_and_unknown_lengths_are_counted() {
    let mut policy = RequestPolicy::unlimited();
    policy.max_body_size = Some(3);
    let mut builder = HostPipelineBuilder::new();
    builder.layer(RequestBodyLayer::new(policy).unwrap()).unwrap();
    let pipeline = builder.build();
    let body = claviron_http::boxed_body(http_body_util::Full::new(
        Bytes::from_static(b"1234"),
    ));
    let ServiceResult::Unhandled(prepared) =
        pipeline.prepare(Request::new(body))
    else {
        panic!("unknown declared size must be read lazily");
    };
    let response = prepared
        .dispatch(|request| async { Response::new(request.into_body()) })
        .now_or_never()
        .unwrap();
    let error =
        response.into_body().collect().now_or_never().unwrap().unwrap_err();
    assert_eq!(
        error.downcast_ref::<claviron_http::body::BodyLimit>(),
        Some(&claviron_http::body::BodyLimit::TooLarge { limit: 3 })
    );
}
