use ::warp::Filter;
use bytes::Bytes;
use http_body_util::BodyExt;

use super::*;
use crate::RequestBody;
use crate::routing::{Application, Routing};

#[tokio::test]
async fn warp_filter_receives_the_original_request_body() {
    let state = claviron_runtime::AppState::standalone("warp-test", ".");
    Routing::new(claviron_http::policy::HttpPolicy::unlimited())
        .register(&state)
        .unwrap();
    #[cfg(feature = "axum")]
    claviron_pipeline::routing::register(&state).unwrap();
    let filter = ::warp::path("echo")
        .and(::warp::body::bytes())
        .map(|body: Bytes| http::Response::new(body));
    register(filter)(&state).unwrap();
    let request = http::Request::builder()
        .method("POST")
        .uri("/echo")
        .body(RequestBody::new(http_body_util::Full::new(Bytes::from_static(
            b"warp request",
        ))))
        .unwrap();
    let response = state
        .registry_ref()
        .resolve::<Application>()
        .unwrap()
        .call(request)
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "warp request"
    );
}
