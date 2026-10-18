use super::*;
use axum::{body::Body, extract::Request};
use claviron_http::ext::RealIpExt;
use http::{StatusCode, header};
use std::net::IpAddr;
use std::time::Duration;
use tower::ServiceExt;

fn request(
    host: &str,
    ip: IpAddr,
) -> Request {
    let mut request = Request::builder()
        .uri("/api")
        .header(header::HOST, host)
        .body(Body::empty())
        .unwrap();
    request.extensions_mut().insert(RealIpExt(ip));
    request
}

#[tokio::test]
async fn prepared_route_layers_share_counters_across_aliases_and_router_rebuilds()
 {
    let limiter = RateLimiter::new(Duration::from_secs(60), 2);
    let router = || {
        Layer(limiter.clone())
            .apply(Router::new().fallback(|| async { StatusCode::NO_CONTENT }))
    };
    let ip = "192.0.2.1".parse().unwrap();
    let first = router().oneshot(request("example.test", ip)).await.unwrap();
    assert_eq!(first.status(), StatusCode::NO_CONTENT);
    assert_eq!(first.headers()["x-ratelimit-remaining"], "1");

    let alias =
        router().oneshot(request("alias.example.test", ip)).await.unwrap();
    assert_eq!(alias.status(), StatusCode::NO_CONTENT);
    assert_eq!(alias.headers()["x-ratelimit-remaining"], "0");

    let denied = router().oneshot(request("example.test", ip)).await.unwrap();
    assert_eq!(denied.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(denied.headers().contains_key(header::RETRY_AFTER));

    let other = router()
        .oneshot(request("example.test", "192.0.2.2".parse().unwrap()))
        .await
        .unwrap();
    assert_eq!(other.status(), StatusCode::NO_CONTENT);
    assert_eq!(other.headers()["x-ratelimit-remaining"], "1");
}
