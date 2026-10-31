use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use claviron_http::boxed_body;
use http::HeaderValue;
use http_body::{Body, Frame};

use super::*;

#[derive(Default)]
struct State {
    identity: IdentityState,
    log: AccessLogState,
}

impl AsRef<IdentityState> for State {
    fn as_ref(&self) -> &IdentityState { &self.identity }
}

impl AsMut<AccessLogState> for State {
    fn as_mut(&mut self) -> &mut AccessLogState { &mut self.log }
}

#[test]
fn explicit_access_log_opt_out_contributes_no_pipeline_actions() {
    let policy = AccessLogLayer::new(
        HostKey(Arc::new("example.test".parse().unwrap())), Some("off"),
    );
    assert!(policy.is_none());
    let mut builder = HostPipelineBuilder::<State>::new();
    if let Some(policy) = policy {
        builder.layer(policy).unwrap();
    }
    let pipeline = builder.build();
    assert!(pipeline.before.is_empty());
    assert!(pipeline.after.is_empty());
    assert!(pipeline.body.is_empty());
}

struct BodyDrop(Arc<AtomicUsize>);

impl Drop for BodyDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn inactive_access_record_keeps_head_and_cancellation_owned_by_the_response() {
    let drops = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let counted = polls.clone();
    let owner = BodyDrop(drops.clone());
    let source = futures_util::stream::poll_fn(move |_| {
        let _owner = &owner;
        counted.fetch_add(1, Ordering::Relaxed);
        Poll::Pending::<Option<Result<Frame<Bytes>, Infallible>>>
    });
    let mut state = AccessLogState::default();
    state.admitted();
    let mut response = RouteResponse::new(boxed_body(http_body_util::StreamBody::new(source)));
    *response.status_mut() = StatusCode::CREATED;
    response.headers_mut().insert("x-example", HeaderValue::from_static("kept"));
    let mut response = state.finish(response);
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()["x-example"], "kept");
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert!(Pin::new(response.body_mut())
        .poll_frame(&mut Context::from_waker(Waker::noop())).is_pending());
    drop(response);
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
