use super::*;

use bytes::Bytes;
use http::StatusCode;
use http_body_util::BodyExt;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn rest_namespaces_do_not_claim_unrelated_paths() {
    let publish =
        Endpoints::Publish(HashSet::from(["/apps/example/events".into()]));
    for path in [
        "/apps/example",
        "/apps/example/events/",
        "/apps/other/events",
        "/assets/app.js",
    ] {
        assert!(!publish.accepts(path), "{path}");
    }
    assert!(publish.accepts("/apps/example/events"));
}

#[tokio::test]
async fn rest_claims_only_its_paths_and_never_reinterprets_a_handled_404() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rest = Rest {
        endpoints: Endpoints::Publish(HashSet::from([
            "/apps/example/events".into()
        ])),
        owner:
            RestOwner::Fleet(
                axum::Router::new()
                    .fallback({
                        let calls = calls.clone();
                        move |request: Request<axum::body::Body>| {
                            calls.fetch_add(1, Ordering::Relaxed);
                            async move {
                                (StatusCode::NOT_FOUND, request.into_body())
                            }
                        }
                    })
                    .into(),
            ),
    };
    for (uri, handled) in
        [("/assets/app.js", false), ("/apps/example/events?x=1", true)]
    {
        let payload = Bytes::from(vec![0, 255, 13, 10]);
        let address = payload.as_ptr();
        let request = Request::builder()
            .uri(uri)
            .body(claviron_http::bytes_body(payload))
            .unwrap();
        let result = match rest.call(request) {
            DispatchCall::Ready(result) => result,
            DispatchCall::Pending(work) => work.await,
        };
        let body = match result {
            ServiceResult::Handled(response) => {
                assert!(handled);
                assert_eq!(response.status(), StatusCode::NOT_FOUND);
                response.into_body()
            }
            ServiceResult::Unhandled(request) => {
                assert!(!handled);
                assert_eq!(request.uri(), uri);
                request.into_body()
            }
        };
        assert_eq!(calls.load(Ordering::Relaxed), usize::from(handled));
        assert_eq!(body.collect().await.unwrap().to_bytes().as_ptr(), address);
    }
}
