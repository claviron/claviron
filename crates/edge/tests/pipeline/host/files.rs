use super::*;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use claviron_http::config::statics::StaticConfig;
use claviron_http::{RouteBody, RouteResponse};
use http::{Request, Response, StatusCode};
use http_body::{Body, Frame};
use http_body_util::BodyExt;

use crate::pipeline::host::{ApplicationHandler, DispatchChain};

#[derive(Default)]
struct Counts {
    polls: AtomicUsize,
    drops: AtomicUsize,
    backend: AtomicUsize,
}

struct Payload {
    data: Option<Bytes>,
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
        Poll::Ready(self.data.take().map(|data| Ok(Frame::data(data))))
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct Echo(Arc<Counts>);

impl ApplicationHandler for Echo {
    fn call(
        self,
        request: Request<RouteBody>,
    ) -> impl Future<Output = RouteResponse> + Send {
        assert_eq!(self.0.polls.load(Ordering::Relaxed), 0);
        self.0.backend.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Response::new(request.into_body()))
    }
}

fn request(
    path: &str,
    counts: &Arc<Counts>,
) -> (Request<RouteBody>, *const u8) {
    let data = Bytes::from(vec![0, 255, 13, 10]);
    let address = data.as_ptr();
    let body = Payload { data: Some(data), counts: counts.clone() };
    (
        Request::builder()
            .uri(path)
            .body(claviron_http::boxed_body(body))
            .unwrap(),
        address,
    )
}

fn mount(none_found: bool) -> StaticConfig {
    StaticConfig {
        path: claviron_http::ext::PathPrefix::root(),
        dir: "static".into(),
        none_found,
        rules: None,
    }
}

#[tokio::test]
async fn static_wins_then_try_files_then_the_backend_receives_the_unread_body()
{
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("static")).unwrap();
    std::fs::write(directory.path().join("static/item.txt"), "static").unwrap();
    std::fs::write(directory.path().join("item.txt"), "try-files").unwrap();
    std::fs::write(directory.path().join("other.txt"), "try-files").unwrap();
    let mut config = HostConfig::new(directory.path());
    config.files.statics = Some(vec![mount(true)]);
    config.files.try_files = true;

    for (path, expected, backend_calls) in [
        ("/item.txt?version=1", &b"static"[..], 0),
        ("/other.txt", &b"try-files"[..], 0),
        ("/missing.txt", &[0, 255, 13, 10][..], 1),
    ] {
        let counts = Arc::new(Counts::default());
        let pipeline =
            DispatchChain::new(Echo(counts.clone()), scope_steps(&config));
        let (request, address) = request(path, &counts);
        let response = pipeline.handle(request).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
        assert_eq!(counts.backend.load(Ordering::Relaxed), backend_calls);
        let collected =
            response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(collected, expected);
        if backend_calls == 1 {
            assert_eq!(collected.as_ptr(), address);
        }
        assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn a_terminal_static_miss_cannot_fall_through_to_try_files() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("static")).unwrap();
    std::fs::write(directory.path().join("item.txt"), "must not be served")
        .unwrap();
    let mut config = HostConfig::new(directory.path());
    config.files.statics = Some(vec![mount(false)]);
    config.files.try_files = true;
    let counts = Arc::new(Counts::default());
    let pipeline =
        DispatchChain::new(Echo(counts.clone()), scope_steps(&config));
    let (request, _) = request("/item.txt", &counts);
    let response = pipeline.handle(request).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(counts.backend.load(Ordering::Relaxed), 0);
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
}

#[test]
fn dynamic_names_reach_the_backend_without_filesystem_or_body_work() {
    let mut config = HostConfig::new("/unused/example");
    config.files.statics = Some(vec![mount(false)]);
    config.files.try_files = true;
    let counts = Arc::new(Counts::default());
    let pipeline =
        DispatchChain::new(Echo(counts.clone()), scope_steps(&config));
    let (request, _) = request("/index.php", &counts);
    let mut answering = Box::pin(pipeline.handle(request));
    let Poll::Ready(response) =
        answering.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("dynamic names must not wait for disk work");
    };
    assert_eq!(counts.backend.load(Ordering::Relaxed), 1);
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    drop(answering);
    drop(response);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
}
