use super::*;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use claviron_http::RouteBody;
use http::Request;
use http_body::{Body, Frame};

use crate::pipeline::host::{ApplicationHandler, DispatchChain};

#[derive(Default)]
struct Counts {
    scripts: AtomicUsize,
    fallback: AtomicUsize,
    polls: AtomicUsize,
    drops: AtomicUsize,
}

struct Payload { frames: VecDeque<Frame<Bytes>>, counts: Arc<Counts> }

impl Body for Payload {
    type Data = Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>, _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.counts.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}

impl Drop for Payload {
    fn drop(&mut self) { self.counts.drops.fetch_add(1, Ordering::Relaxed); }
}

struct ScriptDeclines(Arc<Counts>);
impl DispatchStep for ScriptDeclines {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        self.0.scripts.fetch_add(1, Ordering::Relaxed);
        // Exercise the trusted resolver handoff, including capability restore.
        DispatchFuture::ready(ServiceResult::Unhandled(request))
    }
}

struct Answer(&'static str);
impl DispatchStep for Answer {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        drop(request);
        let response = Response::builder().status(StatusCode::SERVICE_UNAVAILABLE)
            .header("x-example-owner", self.0).body(claviron_http::empty_body()).unwrap();
        DispatchFuture::ready(ServiceResult::Handled(response))
    }
}

#[derive(Clone)]
struct Fallback(Arc<Counts>);
impl ApplicationHandler for Fallback {
    fn call(self, request: Request<RouteBody>) -> impl Future<Output = RouteResponse> + Send {
        self.0.fallback.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Response::new(request.into_body()))
    }
}

fn prefix(path: &str) -> Option<PathPrefix> {
    Some(serde_yaml::from_str(path).unwrap())
}

#[test]
fn selected_application_never_runs_the_script_owner_and_unhandled_keeps_body() {
    for (path, status, scripts, owner) in [
        ("/live/item", StatusCode::SERVICE_UNAVAILABLE, 0, Some("application")),
        ("/live/admin/item", StatusCode::SERVICE_UNAVAILABLE, 0, Some("admin")),
        ("/lively/item", StatusCode::OK, 1, None),
    ] {
        let counts = Arc::new(Counts::default());
        let backend = PhpBackend::from_mounts(vec![
            Mount { path: None, owner: Arc::new(ScriptDeclines(counts.clone())) },
            Mount { path: prefix("/live"), owner: Arc::new(Answer("application")) },
            Mount { path: prefix("/live/admin"), owner: Arc::new(Answer("admin")) },
        ]).unwrap();
        let data = Bytes::from(vec![0, 255, 13, 10]);
        let address = data.as_ptr();
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-example-end", http::HeaderValue::from_static("complete"));
        let request = Request::builder().uri(path).body(claviron_http::boxed_body(Payload {
            frames: VecDeque::from([Frame::data(data), Frame::trailers(trailers)]),
            counts: counts.clone(),
        })).unwrap();
        let pipeline = DispatchChain::new(Fallback(counts.clone()), vec![Arc::new(backend)]);
        let mut call = Box::pin(pipeline.handle(request));
        let mut context = Context::from_waker(Waker::noop());
        let Poll::Ready(response) = call.as_mut().poll(&mut context) else {
            panic!("mount selection must not wait for the body or start PHP");
        };
        assert_eq!(response.status(), status);
        assert_eq!(response.headers().get("x-example-owner").map(|value| value.to_str().unwrap()), owner);
        assert_eq!(counts.scripts.load(Ordering::Relaxed), scripts);
        assert_eq!(counts.fallback.load(Ordering::Relaxed), scripts);
        assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
        drop(call);
        let mut body = response.into_body();
        if scripts == 1 {
            let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut context) else {
                panic!("fallthrough must retain the original data frame");
            };
            assert_eq!(frame.into_data().unwrap().as_ptr(), address);
            let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut context) else {
                panic!("fallthrough must retain the original trailers");
            };
            assert_eq!(frame.into_trailers().unwrap()["x-example-end"], "complete");
        }
        drop(body);
        assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn equally_specific_mounts_retain_the_last_declared_owner() {
    let backend = PhpBackend::from_mounts(vec![
        Mount { path: prefix("/app"), owner: Arc::new(Answer("first")) },
        Mount { path: prefix("/app"), owner: Arc::new(Answer("last")) },
    ]).unwrap();
    let counts = Arc::new(Counts::default());
    let pipeline = DispatchChain::new(Fallback(counts.clone()), vec![Arc::new(backend)]);
    let mut call = Box::pin(pipeline.handle(Request::builder().uri("/app/item")
        .body(claviron_http::empty_body()).unwrap()));
    let Poll::Ready(response) = call.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("prepared owners must be called directly");
    };
    assert_eq!(response.headers()["x-example-owner"], "last");
    assert_eq!(counts.fallback.load(Ordering::Relaxed), 0);
    assert!(PhpBackend::from_mounts(Vec::new()).is_none());
}
