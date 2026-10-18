use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use http_body::Frame;

use super::*;

fn application<S>(
    policy: HttpPolicy,
    max_concurrent_requests: Option<usize>,
    service: S,
) -> Application
where
    S: Service<AppRequest, Response = AppResponse, Error = Infallible>
        + Clone
        + Send
        + Sync,
    S: 'static,
    S::Future: Send + 'static,
{
    Application {
        service: Arc::new(Erased(service)),
        #[cfg(feature = "broadcast")]
        broadcast: None,
        upgrades: None,
        policy,
        admission: Admission::limited(max_concurrent_requests),
    }
}

struct Stalled;

impl http_body::Body for Stalled {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        Poll::Pending
    }
}

/// A request that waited out the queue is refused, one whose head never came
/// is a 504, and neither keeps the place it held.
#[tokio::test(start_paused = true)]
async fn admission_and_head_expiry_release_capacity() {
    let mut policy = HttpPolicy::unlimited();
    policy.requests.queue_timeout = Some(Duration::from_secs(30));
    policy.requests.head_timeout = Some(Duration::from_secs(60));
    let pending = tower::service_fn(|_: AppRequest| {
        std::future::pending::<Result<AppResponse, Infallible>>()
    });
    let app = application(policy, Some(1), pending);

    let held = app.admission.enter(None).await.unwrap();
    assert_eq!(
        app.call(Request::new(RequestBody::empty())).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(held);
    assert_eq!(
        app.call(Request::new(RequestBody::empty())).await.status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    assert!(app.admission.enter(policy.requests.queue_timeout).await.is_some());
}

/// The place ends at the response head: a body still streaming does not hold
/// it.
#[tokio::test(start_paused = true)]
async fn response_head_releases_capacity_before_body_finishes() {
    let mut policy = HttpPolicy::unlimited();
    policy.requests.queue_timeout = Some(Duration::from_secs(30));
    let streaming = tower::service_fn(|_: AppRequest| async {
        Ok::<_, Infallible>(Response::new(route_body(Stalled)))
    });
    let app = application(policy, Some(1), streaming);

    let first = app.call(Request::new(RequestBody::empty())).await;
    assert_eq!(first.status(), StatusCode::OK);
    let second = app.call(Request::new(RequestBody::empty())).await;
    assert_eq!(second.status(), StatusCode::OK);
}

/// A declared body over the limit is refused before the service is reached.
#[tokio::test]
async fn declared_oversize_body_is_refused() {
    let mut policy = HttpPolicy::unlimited();
    policy.requests.max_body_size = Some(4);
    let unreachable = tower::service_fn(|_: AppRequest| async {
        panic!("the guard answers first");
        #[allow(unreachable_code)]
        Ok::<AppResponse, Infallible>(Response::new(empty_body()))
    });
    let app = application(policy, None, unreachable);
    let request = Request::builder()
        .header("content-length", "5")
        .body(RequestBody::empty())
        .unwrap();
    assert_eq!(app.call(request).await.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

/// A host-free routable behind the application's own routes and in front of
/// the owner: the application wins where both declare a path, the routable
/// answers its own, and the owner everything else.
#[cfg(feature = "axum")]
#[tokio::test]
async fn routables_stand_between_the_application_and_the_owner() {
    use axum::routing::get;
    use claviron_runtime::registry::Component;

    struct Contributed;
    impl Component<AppState> for Contributed {
        type Deps = ();

        fn name(&self) -> &'static str {
            "Contributed"
        }
    }
    impl claviron_http::routing::Routable for Contributed {
        fn routes(
            self: Arc<Self>,
            _: &AppState,
        ) -> registry::Result<Option<axum::Router<AppState>>> {
            Ok(Some(
                axum::Router::new()
                    .route("/shared", get(|| async { "routable" }))
                    .route("/routable", get(|| async { "routable" })),
            ))
        }
    }

    let state = AppState::standalone("routable-order", ".");
    register_owner(
        &state,
        Owner::new(tower::service_fn(|_: AppRequest| async {
            Ok::<_, Infallible>(Response::new(claviron_http::bytes_body(
                Bytes::from("owner"),
            )))
        })),
    )
    .unwrap();
    state
        .registry_ref()
        .insert(Arc::new(Routing::new(HttpPolicy::unlimited(), None)))
        .unwrap();
    claviron_http::routing::register(&state).unwrap();
    claviron_http::routing::register_routable(&state, Arc::new(Contributed))
        .unwrap();
    register_router(&state, |_| {
        axum::Router::new().route("/shared", get(|| async { "application" }))
    })
    .unwrap();

    let application = state.registry_ref().resolve::<Application>().unwrap();
    for (path, answer) in [
        ("/shared", "application"),
        ("/routable", "routable"),
        ("/elsewhere", "owner"),
    ] {
        let request =
            Request::builder().uri(path).body(RequestBody::empty()).unwrap();
        let body = application.call(request).await.into_body();
        let bytes = body.collect().await.unwrap().to_bytes();
        assert_eq!(bytes, answer, "{path}");
    }
}
