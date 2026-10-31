use super::*;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use claviron_http::{RouteBody, RouteResponse};
use http::{HeaderValue, Request, Response, StatusCode};
use http_body::{Body, Frame};

use crate::pipeline::host::{ApplicationHandler, DispatchChain};

fn routes(source: &str) -> Vec<LuaRouteConfig> {
    serde_yaml::from_str(source).unwrap()
}

#[test]
fn prepared_mounts_preserve_segment_matching_specificity_and_tie_order() {
    let backend = LuaBackend::new(None, &routes(
        "- { application: fallback }\n- { path: /, application: root }\n- { path: /api, application: first }\n- { path: /api, application: last }\n- { path: /api/admin, application: admin }\n",
    )).unwrap();
    for (path, expected) in [
        ("/api", "last"),
        ("/api/item", "last"),
        ("/api/admin/item", "admin"),
        ("/apiary/item", "root"),
        ("/", "root"),
        ("*", "fallback"),
    ] {
        assert_eq!(
            backend.select(path).unwrap().adapter.application(),
            expected,
            "{path}"
        );
    }
    assert!(LuaBackend::new(None, &[]).is_none());
}

#[derive(Default)]
struct Counts {
    polls: AtomicUsize,
    drops: AtomicUsize,
    fallback: AtomicUsize,
}

struct Payload {
    frames: VecDeque<Frame<Bytes>>,
    counts: Arc<Counts>,
}

impl Body for Payload {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.counts.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct Fallback(Arc<Counts>);

impl ApplicationHandler for Fallback {
    fn call(
        self,
        request: Request<RouteBody>,
    ) -> impl Future<Output = RouteResponse> + Send {
        self.0.fallback.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Response::new(request.into_body()))
    }
}

#[test]
fn only_nonmatching_mounts_yield_the_original_body_and_trailers() {
    let backend = LuaBackend::new(
        None,
        &routes("- { path: /lua, application: example }"),
    )
    .unwrap();
    for (path, status, fallbacks) in [
        ("/luau/item", StatusCode::OK, 1),
        ("/lua/item", StatusCode::SERVICE_UNAVAILABLE, 0),
    ] {
        let counts = Arc::new(Counts::default());
        let data = Bytes::from(vec![0, 255, 13, 10]);
        let address = data.as_ptr();
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-example-end", HeaderValue::from_static("complete"));
        let request = Request::builder()
            .uri(path)
            .body(claviron_http::boxed_body(Payload {
                frames: VecDeque::from([
                    Frame::data(data),
                    Frame::trailers(trailers),
                ]),
                counts: counts.clone(),
            }))
            .unwrap();
        let pipeline = DispatchChain::new(
            Fallback(counts.clone()),
            vec![Arc::new(backend.clone())],
        );
        let mut call = Box::pin(pipeline.handle(request));
        let mut context = Context::from_waker(Waker::noop());
        let Poll::Ready(response) = call.as_mut().poll(&mut context) else {
            panic!(
                "mount selection must not wait for the body or boot an engine"
            );
        };
        assert_eq!(response.status(), status);
        assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
        assert_eq!(counts.fallback.load(Ordering::Relaxed), fallbacks);
        drop(call);
        let mut body = response.into_body();
        if fallbacks == 1 {
            let Poll::Ready(Some(Ok(frame))) =
                Pin::new(&mut body).poll_frame(&mut context)
            else {
                panic!("the original data must remain available");
            };
            assert_eq!(frame.into_data().unwrap().as_ptr(), address);
            let Poll::Ready(Some(Ok(frame))) =
                Pin::new(&mut body).poll_frame(&mut context)
            else {
                panic!("the original trailers must remain available");
            };
            assert_eq!(
                frame.into_trailers().unwrap()["x-example-end"],
                "complete"
            );
        }
        drop(body);
        assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    }
}
