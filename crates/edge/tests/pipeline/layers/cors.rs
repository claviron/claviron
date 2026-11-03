use super::*;

use claviron_http::{ServiceResult, empty_body};
use futures_util::FutureExt;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body::{Body, Frame};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

struct Unread(Arc<AtomicUsize>);

impl Body for Unread {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        panic!("CORS must not poll the request body");
    }
}

impl Drop for Unread {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn cors(config: &str) -> CorsLayer {
    CorsLayer::new(serde_yaml::from_str(config).unwrap()).unwrap()
}

fn execute(
    pipeline: &super::super::super::host::HostPipeline<CorsState>,
    request: Request<RouteBody>,
    calls: &AtomicUsize,
) -> RouteResponse {
    match pipeline.prepare_with_state(request, CorsState::default()) {
        ServiceResult::Handled(reply) => reply.finish(),
        ServiceResult::Unhandled(request) => request
            .dispatch(|request| async {
                calls.fetch_add(1, Ordering::Relaxed);
                Response::new(request.into_body())
            })
            .now_or_never()
            .expect("header-only fixture must be ready"),
    }
}

fn request(
    method: Method,
    drops: &Arc<AtomicUsize>,
    preflight: bool,
) -> Request<RouteBody> {
    let mut request = Request::builder()
        .method(method)
        .header("origin", "https://ui.example.test");
    if preflight {
        request = request
            .header("access-control-request-method", "POST")
            .header("access-control-request-headers", "content-type");
    }
    request.body(claviron_http::boxed_body(Unread(drops.clone()))).unwrap()
}

#[test]
fn cors_only_preflight_claims_options_and_never_reads_the_body() {
    let mut builder = HostPipelineBuilder::<CorsState>::new();
    builder.layer(cors("permissive")).unwrap();
    let pipeline = builder.build();
    let calls = AtomicUsize::new(0);
    let drops = Arc::new(AtomicUsize::new(0));
    let reply =
        execute(&pipeline, request(Method::OPTIONS, &drops, true), &calls);
    assert_eq!(reply.status(), StatusCode::NO_CONTENT);
    assert_eq!(reply.headers()["access-control-allow-origin"], "*");
    assert_eq!(reply.headers()["access-control-allow-methods"], "POST");
    assert!(!reply.headers().contains_key("access-control-max-age"));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    let reply =
        execute(&pipeline, request(Method::OPTIONS, &drops, false), &calls);
    assert_eq!(reply.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(reply);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn cors_compiles_regexes_at_preparation_and_matches_literal_origins_exactly() {
    assert!(
        CorsLayer::new(
            serde_yaml::from_str("policy: allowlist\norigins: ['^(']").unwrap()
        )
        .is_err()
    );
    let mut builder = HostPipelineBuilder::<CorsState>::new();
    builder
        .layer(cors(
            r#"policy: allowlist
origins: ['https://ui.example.test', '^https://preview-[0-9]+\.example\.test$']
"#,
        ))
        .unwrap();
    let pipeline = builder.build();
    let calls = AtomicUsize::new(0);
    for (origin, expected) in [
        ("https://ui.example.test", StatusCode::OK),
        ("https://uiXexampleXtest", StatusCode::FORBIDDEN),
        ("https://ui.example.test.evil", StatusCode::FORBIDDEN),
        ("https://preview-12.example.test", StatusCode::OK),
    ] {
        let reply = execute(
            &pipeline,
            Request::builder()
                .header("origin", origin)
                .body(empty_body())
                .unwrap(),
            &calls,
        );
        assert_eq!(reply.status(), expected, "{origin}");
    }
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

struct Refuse;

impl HostLayer<CorsState> for Refuse {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<CorsState>,
    ) -> crate::Result<()> {
        layer.before(|_, _| {
            let mut reply = Response::new(empty_body());
            *reply.status_mut() = StatusCode::TOO_MANY_REQUESTS;
            reply.headers_mut().append(
                "vary",
                HeaderValue::from_static("Accept-Encoding, Origin"),
            );
            ControlFlow::Break(reply)
        });
        Ok(())
    }
}

#[test]
fn cors_response_context_covers_early_refusals_without_duplicate_vary_values() {
    let (capture, admission) = cors("policy: allowlist\ncredentials: true\norigins: ['https://ui.example.test']").split();
    let mut builder = HostPipelineBuilder::<CorsState>::new();
    builder
        .layer(capture)
        .unwrap()
        .layer(Refuse)
        .unwrap()
        .layer(admission)
        .unwrap();
    let pipeline = builder.build();
    let calls = AtomicUsize::new(0);
    for origin in [
        Some("https://ui.example.test"),
        Some("https://other.example.test"),
        None,
    ] {
        let mut request = Request::builder();
        if let Some(origin) = origin {
            request = request.header("origin", origin);
        }
        let reply =
            execute(&pipeline, request.body(empty_body()).unwrap(), &calls);
        assert_eq!(reply.status(), StatusCode::TOO_MANY_REQUESTS);
        if origin == Some("https://ui.example.test") {
            assert_eq!(
                reply.headers()["access-control-allow-origin"],
                origin.unwrap()
            );
            assert_eq!(
                reply.headers()["access-control-allow-credentials"],
                "true"
            );
        } else {
            assert!(
                !reply.headers().contains_key("access-control-allow-origin")
            );
        }
        assert_eq!(reply.headers().get_all("vary").iter().count(), 1);
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

struct Wait;

impl super::super::super::host::AsyncHeadAction<CorsState> for Wait {
    fn call<'a>(
        &'a self,
        _: &'a mut http::request::Parts,
        _: &'a mut CorsState,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = ControlFlow<RouteResponse>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(std::future::pending())
    }
}

struct Pending;

impl HostLayer<CorsState> for Pending {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<CorsState>,
    ) -> crate::Result<()> {
        layer.before_async(Wait);
        Ok(())
    }
}

#[test]
fn cors_cancellation_drops_the_original_body_while_admission_is_pending() {
    let (capture, admission) = cors("permissive").split();
    let mut builder = HostPipelineBuilder::<CorsState>::new();
    builder
        .layer(capture)
        .unwrap()
        .layer(Pending)
        .unwrap()
        .layer(admission)
        .unwrap();
    let pipeline = builder.build();
    let drops = Arc::new(AtomicUsize::new(0));
    let prepared = match pipeline.prepare_with_state(
        request(Method::POST, &drops, false),
        CorsState::default(),
    ) {
        ServiceResult::Unhandled(prepared) => prepared,
        ServiceResult::Handled(_) => panic!("capture must not refuse"),
    };
    {
        let mut future = pin!(prepared.dispatch(|_| async {
            panic!("pending admission must not reach a handler")
        }));
        assert!(
            future
                .as_mut()
                .poll_unpin(&mut Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn cors_vary_includes_absent_origin_and_preserves_wildcard() {
    let config = serde_yaml::from_str(
        "policy: allowlist\norigins: ['https://ui.example.test']",
    )
    .unwrap();
    let mut headers = HeaderMap::new();
    rules::finish(&config, None, &mut headers);
    assert_eq!(headers["vary"], "origin");
    headers.insert("vary", HeaderValue::from_static("*"));
    rules::finish(
        &config,
        Some(&HeaderValue::from_static("https://ui.example.test")),
        &mut headers,
    );
    assert_eq!(headers.get_all("vary").iter().count(), 1);
    assert_eq!(headers["vary"], "*");
}

#[test]
fn cors_explicit_disabled_keeps_origin_refusal_and_passes_non_cors_requests() {
    let mut builder = HostPipelineBuilder::<CorsState>::new();
    builder.layer(cors("disabled")).unwrap();
    let pipeline = builder.build();
    let calls = AtomicUsize::new(0);
    let drops = Arc::new(AtomicUsize::new(0));
    let reply = execute(&pipeline, request(Method::GET, &drops, false), &calls);
    assert_eq!(reply.status(), StatusCode::FORBIDDEN);
    assert!(!reply.headers().contains_key("access-control-allow-origin"));
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    let reply = execute(&pipeline, Request::new(empty_body()), &calls);
    assert_eq!(reply.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
