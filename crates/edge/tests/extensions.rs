use super::*;
use crate::config::HostConfig;
use crate::pipeline::merged::routing::RouteTable;
use claviron_http::{ext::PathPrefix, routing::AxumPipeline};
use http::HeaderMap;

#[tokio::test]
async fn route_rules_delegate_navigation_without_entering_rust_routes() {
    use crate::pipeline::merged::routing::{
        ConfiguredRoute, RouteDispatch, dispatch,
    };
    use axum::{Router, body::Body, extract::Request, routing::post};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tower::ServiceExt;

    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let downstream = AxumPipeline::new(Router::new().fallback(
        |request: Request| async move {
            assert_eq!(request.uri(), "/games/42-title?keep=1");
            assert_eq!(request.method(), http::Method::POST);
            assert_eq!(request.headers()["x-fixture"], "preserved");
            let body =
                axum::body::to_bytes(request.into_body(), 64).await.unwrap();
            assert_eq!(&body[..], b"unchanged");
            http::StatusCode::ACCEPTED
        },
    ));
    let intercept = Router::new().route(
        "/games/{id}",
        post(move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::Relaxed);
                http::StatusCode::NO_CONTENT
            }
        }),
    );
    let state = RouteDispatch {
        entries: Arc::new(RouteTable::new([(
            PathPrefix::root(),
            ConfiguredRoute {
                pipeline: AxumPipeline::new(intercept),
                conditions: vec![Arc::new(AjaxCondition(true))],
            },
        )])),
        fallback: downstream.clone(),
        rejected: downstream,
    };
    let router = Router::new().fallback(dispatch::serve).with_state(state);
    let request = || {
        Request::builder()
            .method("POST")
            .uri("/games/42-title?keep=1")
            .header("x-fixture", "preserved")
            .body(Body::from("unchanged"))
            .unwrap()
    };
    assert_eq!(
        router.clone().oneshot(request()).await.unwrap().status(),
        http::StatusCode::ACCEPTED
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let mut ajax = request();
    ajax.headers_mut()
        .insert("x-requested-with", "XMLHttpRequest".parse().unwrap());
    assert_eq!(
        router.oneshot(ajax).await.unwrap().status(),
        http::StatusCode::NO_CONTENT
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

fn rules(yaml: &str) -> RouteTable<bool> {
    let config: HostConfig = serde_yaml::from_str(yaml).unwrap();
    RouteTable::new(config.routes.iter().map(|(prefix, route)| {
        (prefix.clone(), route.extensions["ajax_only"].as_bool().unwrap())
    }))
}

fn admits(
    rules: &RouteTable<bool>,
    path: &str,
    headers: &HeaderMap,
) -> bool {
    rules.find(path).is_none_or(|enabled| {
        AjaxCondition(*enabled).accepts(&RuleContext {
            method: &http::Method::GET,
            uri: &http::Uri::from_static("/"),
            headers,
        })
    })
}

#[test]
fn route_rules_single_map_requires_ajax_without_assuming_json_accept() {
    let rules = rules("root: public\nroutes: {ajax_only: true}");
    let mut headers = HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    assert!(!admits(&rules, "/games/42-title", &headers));
    headers.insert("x-requested-with", "xmlhttprequest".parse().unwrap());
    assert!(admits(&rules, "/games/42-title", &headers));
    headers.insert("x-requested-with", "other".parse().unwrap());
    assert!(!admits(&rules, "/search", &headers));
}

#[test]
fn route_rules_list_uses_longest_segment_prefix_and_explicit_exception() {
    let rules = rules(
        "root: public\nroutes:\n  - {ajax_only: true}\n  - {path: /public/, ajax_only: false}",
    );
    let headers = HeaderMap::new();
    assert!(admits(&rules, "/public", &headers));
    assert!(admits(&rules, "/public/item", &headers));
    assert!(!admits(&rules, "/publicity", &headers));
    assert!(!admits(&rules, "/profile", &headers));
    assert!(admits(&RouteTable::new([]), "/profile", &headers));
    assert!(
        serde_yaml::from_str::<HostConfig>(
            "root: public\nroutes: {path: invalid, ajax_only: true}"
        )
        .is_err()
    );
    assert!(
        serde_yaml::from_str::<HostConfig>(
            "root: public\nroutes: {ajax_onyl: true}"
        )
        .is_err()
    );
}
