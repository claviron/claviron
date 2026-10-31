use super::*;

use bytes::Bytes;
use claviron_http::{RouteFuture, RouteResponse};
use http::StatusCode;
use http_body_util::BodyExt;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pipeline::other::OtherHandler;

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

    for (path, accepted) in [
        ("/app/example", true),
        ("/apps/example", true),
        ("/apps/example/events", true),
        ("/apps/example/channels/one", true),
        ("/app/", false),
        ("/app/example/extra", false),
        ("/apps/", false),
        ("/apps/example/", false),
        ("/application/example", false),
        ("/assets/app.js", false),
        ("app/example", false),
    ] {
        assert_eq!(Endpoints::Http.accepts(path), accepted, "{path}");
    }
}

struct Echo(Arc<AtomicUsize>);

impl OtherHandler for Echo {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        self.0.fetch_add(1, Ordering::Relaxed);
        let mut response = RouteResponse::new(request.into_body());
        *response.status_mut() = StatusCode::NOT_FOUND;
        Box::pin(std::future::ready(Ok(response)))
    }
}

#[tokio::test]
async fn rest_claims_only_its_paths_and_never_reinterprets_a_handled_404() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rest = Rest {
        endpoints: Endpoints::Publish(HashSet::from([
            "/apps/example/events".into()
        ])),
        owner: OtherService::new(Echo(calls.clone())),
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
        let body = match rest.call(request).await {
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
