use super::*;

use bytes::Bytes;
use claviron_http::{
    OtherHandler, OtherService, RouteBody, RouteFuture, boxed_body,
};
use futures_util::FutureExt;
use http::{HeaderMap, Response};
use http_body::{Body, Frame};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

static FIRST: &[u8] = b"abc";
static SECOND: &[u8] = b"def";

struct Frames {
    frames: VecDeque<Frame<Bytes>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Body for Frames {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, Infallible>>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

struct Reply {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl OtherHandler for Reply {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        drop(request);
        let mut trailers = HeaderMap::new();
        trailers.insert("x-finished", http::HeaderValue::from_static("yes"));
        let response = Response::builder()
            .status(StatusCode::CREATED)
            .header("x-original", "kept")
            .body(boxed_body(Frames {
                frames: VecDeque::from([
                    Frame::data(Bytes::from_static(FIRST)),
                    Frame::data(Bytes::from_static(SECOND)),
                    Frame::trailers(trailers),
                ]),
                polls: self.polls.clone(),
                drops: self.drops.clone(),
            }))
            .unwrap();
        Box::pin(std::future::ready(Ok(response)))
    }
}

#[test]
fn listener_and_prefix_reply_bounds_keep_streaming_frames_and_cancellation_ownership()
 {
    let state = AppState::standalone("reply-bounds", ".");
    let http = Arc::new(crate::HttpConfigProvider::empty("http.yaml".into()));
    http.replace_from_yaml(
        r#"
defaults:
  listener:
    requests: {max_body_size: off, body_read_timeout: off}
listeners:
  small:
    transport: tcp
    listen: 127.0.0.1:8080
    protocols: h1
    responses: {max_body_size: 4}
  large:
    transport: tcp
    listen: 127.0.0.1:8081
    protocols: h1
    responses: {max_body_size: 8}
  idle:
    transport: tcp
    listen: 127.0.0.1:8082
    protocols: h1
    responses: {body_idle_timeout: 2s}
  open:
    transport: tcp
    listen: 127.0.0.1:8083
    protocols: h1
"#,
    )
    .unwrap();
    state.registry_ref().insert(http.clone()).unwrap();
    let registry = HostAppRegistry::default();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let observed_polls = polls.clone();
    let observed_drops = drops.clone();
    registry.register_other("example.test", move |_, _, _| {
        Ok(OtherService::new(Reply {
            polls: observed_polls.clone(),
            drops: observed_drops.clone(),
        }))
    });
    let mut table: crate::loader::table::VhostTable = serde_yaml::from_str(
        "example.test:\n  responses: {max_body_size: 12}\n  routes:\n    /narrow: {responses: {max_body_size: 2}}\n    /narrow/child: {}\n    /off: {responses: {max_body_size: off}}\n",
    ).unwrap();
    let snapshot = registry.build_table(&state, &table, None).unwrap();
    table.set_prepared(
        std::any::TypeId::of::<HostAppSnapshot>(),
        Arc::new(snapshot),
    );
    let host = "example.test".parse().unwrap();
    for (listener, path, successful_frames, fails) in [
        ("small", "/", 1, true),
        ("large", "/", 2, false),
        ("idle", "/", 2, false),
        ("open", "/", 2, false),
        ("large", "/narrow/child", 0, true),
        ("small", "/off", 1, true),
    ] {
        let id = listener.parse().unwrap();
        let input = crate::transport::publication::select_listener(
            &http.snapshot(),
            &id,
        )
        .unwrap();
        let prepared =
            registry.prepare_listener(&state, &table, &input, None).unwrap();
        let (_, owner) =
            prepared.resolve_application_on(&host, Some(&id)).unwrap();
        let before = polls.load(Ordering::Relaxed);
        let dropped = drops.load(Ordering::Relaxed);
        let response = owner
            .unwrap()
            .handle(
                Request::builder()
                    .uri(path)
                    .body(claviron_http::empty_body())
                    .unwrap(),
            )
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(polls.load(Ordering::Relaxed), before);
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-original"], "kept");
        let mut body = response.into_body();
        for expected in [FIRST, SECOND].into_iter().take(successful_frames) {
            let frame = body.frame().now_or_never().unwrap().unwrap().unwrap();
            assert_eq!(frame.data_ref().unwrap().as_ptr(), expected.as_ptr());
        }
        let frame = body.frame().now_or_never().unwrap().unwrap();
        if fails {
            assert!(frame.is_err(), "{listener}: {path}");
        } else {
            assert_eq!(
                frame.unwrap().trailers_ref().unwrap()["x-finished"],
                "yes"
            );
            assert!(body.frame().now_or_never().unwrap().is_none());
        }
        drop(body);
        assert_eq!(drops.load(Ordering::Relaxed), dropped + 1);
        let before = polls.load(Ordering::Relaxed);
        // Cancelling before the first body poll releases exactly this source.
        drop(
            owner
                .unwrap()
                .handle(Request::new(claviron_http::empty_body()))
                .now_or_never()
                .unwrap()
                .unwrap(),
        );
        assert_eq!(polls.load(Ordering::Relaxed), before);
        assert_eq!(drops.load(Ordering::Relaxed), dropped + 2);
    }
}

#[test]
fn compiled_reply_guard_covers_an_early_registered_layer_response() {
    struct Early;
    impl RouteLayer for Early {
        fn prepare(
            &self,
            layer: &mut HostLayerBuilder,
        ) -> crate::Result<()> {
            layer.before(|_, _| {
                std::ops::ControlFlow::Break(Response::new(
                    claviron_http::bytes_body(Bytes::from_static(FIRST)),
                ))
            });
            Ok(())
        }
    }
    let state = AppState::standalone("early-reply-bounds", ".");
    let config = Arc::new(
        serde_yaml::from_str("responses: {max_body_size: 2}").unwrap(),
    );
    let mut contributions = HostContributions::default();
    contributions.layers.push(Arc::new(Early));
    let owner = crate::pipeline::host::compile::HostCompiler {
        state: &state,
        http: None,
        protocol: None,
    }
    .prepare(
        &"example.test".parse().unwrap(),
        config,
        None,
        &contributions,
        #[cfg(feature = "rate-limit")]
        None,
        #[cfg(feature = "websocket")]
        None,
    )
    .unwrap();
    let response = owner
        .handle(Request::new(claviron_http::empty_body()))
        .now_or_never()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.into_body().frame().now_or_never().unwrap().unwrap().is_err()
    );
}
