use std::sync::atomic::{AtomicUsize, Ordering};

use claviron_http::empty_body;
use http::{Method, StatusCode, header};

use super::*;

#[test]
fn exchange_registers_only_declared_actions() {
    let empty = ExchangeLayer::new(None, [], []).into_pipeline();
    assert!(empty.before.is_empty());
    assert!(empty.after.is_empty());
    assert!(empty.body.is_empty());

    let headers = ExchangeLayer::new(
        None,
        [(HeaderName::from_static("x-policy"), HeaderValue::from_static("set"))],
        [],
    ).into_pipeline();
    assert!(headers.before.is_empty());
    assert_eq!(headers.after.len(), 1);
    assert!(headers.body.is_empty());

    let cors = ExchangeLayer::new(
        Some(serde_yaml::from_str("permissive").unwrap()), [], [],
    ).into_pipeline();
    assert_eq!(cors.before.len(), 1);
    assert_eq!(cors.after.len(), 1);
    assert!(cors.body.is_empty());
}

struct BodyOwner(Arc<AtomicUsize>);

impl Drop for BodyOwner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn borrowed_exchange_keeps_body_ownership_and_decorates_early_and_timeout_replies() {
    let layer = ExchangeLayer::new(
        Some(serde_yaml::from_str(
            "{policy: allowlist, origins: ['https://client.test']}",
        ).unwrap()),
        [
            (HeaderName::from_static("x-policy"), HeaderValue::from_static("set")),
            (header::VARY, HeaderValue::from_static("accept-encoding")),
        ],
        [HeaderName::from_static("x-remove")],
    );
    let pipeline = layer.into_pipeline();
    for (method, origin, expected, early) in [
        (Method::GET, "https://client.test", StatusCode::GATEWAY_TIMEOUT, false),
        (Method::OPTIONS, "https://client.test", StatusCode::NO_CONTENT, true),
        (Method::GET, "https://other.test", StatusCode::FORBIDDEN, true),
    ] {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut head, body) = Request::builder()
            .method(method)
            .uri("/resource?raw=%2F")
            .header(header::ORIGIN, origin)
            .body(BodyOwner(drops.clone()))
            .unwrap().into_parts();
        let (reply, scope) = pipeline.prepare_head(&mut head, ExchangeState::default());
        assert_eq!(reply.is_some(), early);
        assert_eq!(head.uri, "/resource?raw=%2F");
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        // A changed head must not cause a second origin decision at response time.
        head.headers.insert(header::ORIGIN, HeaderValue::from_static("https://other.test"));
        let mut response = reply.unwrap_or_else(|| {
            let mut response = RouteResponse::new(empty_body());
            *response.status_mut() = expected;
            response
        });
        response.headers_mut().insert("x-remove", HeaderValue::from_static("old"));
        let response = scope.finish(response);
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()["x-policy"], "set");
        assert!(!response.headers().contains_key("x-remove"));
        if origin == "https://client.test" {
            assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], origin);
            let vary = response.headers()[header::VARY].to_str().unwrap();
            assert!(vary.split(',').any(|value| value.trim() == "origin"));
            assert!(vary.split(',').any(|value| value.trim() == "accept-encoding"));
        } else {
            assert!(!response.headers().contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
        }
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(body);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn cancelled_response_scope_drops_state_without_running_response_actions() {
    struct State(Arc<AtomicUsize>);
    impl Drop for State {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let called = Arc::new(AtomicUsize::new(0));
    let counted = called.clone();
    let mut layer = HostLayerBuilder::<State>::default();
    layer.after(move |_, _| { counted.fetch_add(1, Ordering::Relaxed); });
    let pipeline = HostPipelineBuilder { layers: vec![layer] }.build();
    let (mut head, ()) = Request::new(()).into_parts();
    let (reply, scope) = pipeline.prepare_head(&mut head, State(drops.clone()));
    assert!(reply.is_none());
    drop(scope);
    assert_eq!(called.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
