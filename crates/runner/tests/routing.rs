use std::pin::Pin;
use std::time::Duration;

use http_body_util::BodyExt;

use super::*;

fn application<S>(
    policy: HttpPolicy,
    service: S,
) -> Arc<Application>
where
    S: Service<AppRequest, Response = AppResponse, Error = Infallible>
        + Clone
        + Send
        + Sync,
    S: 'static,
    S::Future: Send + 'static,
{
    let state = AppState::standalone("runner-routing", ".");
    Routing::new(policy).register(&state).unwrap();
    compose(&state, service).unwrap();
    state.registry_ref().resolve::<Application>().unwrap()
}

/// An expired head deadline drops the outstanding owner call.
#[tokio::test(start_paused = true)]
async fn head_expiry_cancels_the_owner() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct PendingOwner(Arc<AtomicBool>);
    impl Drop for PendingOwner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    let mut policy = HttpPolicy::unlimited();
    policy.requests.head_timeout = Some(Duration::from_secs(60));
    let dropped = Arc::new(AtomicBool::new(false));
    let observed = dropped.clone();
    let pending = tower::service_fn(move |_: AppRequest| {
        let owner = PendingOwner(observed.clone());
        async move {
            let _owner = owner;
            std::future::pending::<Result<AppResponse, Infallible>>().await
        }
    });
    let app = application(policy, pending);

    assert_eq!(
        app.call(Request::new(RequestBody::empty())).await.status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    assert!(dropped.load(Ordering::Relaxed));
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
    let app = application(policy, unreachable);
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
    impl claviron_pipeline::routing::Routable for Contributed {
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
    Routing::new(HttpPolicy::unlimited()).register(&state).unwrap();
    claviron_pipeline::routing::register(&state).unwrap();
    claviron_pipeline::routing::register_routable(
        &state,
        Arc::new(Contributed),
    )
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

#[cfg(all(feature = "rust", feature = "axum"))]
#[tokio::test]
async fn rust_axum_needs_no_owner_and_keeps_its_fallback() {
    use axum::routing::get;

    let state = AppState::standalone("rust-routes", ".");
    Routing::new(HttpPolicy::unlimited()).register(&state).unwrap();
    claviron_pipeline::routing::register(&state).unwrap();
    assert!(
        state
            .registry_ref()
            .resolve::<claviron_pipeline::routing::RoutableRegistry>()
            .unwrap()
            .build(&state)
            .unwrap()
            .is_none()
    );
    let router = axum::Router::new()
        .route("/", get(|| async { "rust" }))
        .fallback(|| async { (StatusCode::IM_A_TEAPOT, "custom fallback") });
    register_prepared_router(&state, router.into()).unwrap();
    assert!(state.registry_ref().resolve::<Owner>().is_none());

    let application = state.registry_ref().resolve::<Application>().unwrap();
    for (path, status, expected) in [
        ("/", StatusCode::OK, "rust"),
        ("/missing", StatusCode::IM_A_TEAPOT, "custom fallback"),
    ] {
        let response = application
            .call(
                Request::builder()
                    .uri(path)
                    .body(RequestBody::empty())
                    .unwrap(),
            )
            .await;
        assert_eq!(response.status(), status);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            expected
        );
    }
}

#[cfg(feature = "rust")]
#[cfg(feature = "orbit")]
#[tokio::test]
async fn rust_other_keeps_body_bytes_without_an_owner() {
    let state = AppState::standalone("rust-service", ".");
    Routing::new(HttpPolicy::unlimited()).register(&state).unwrap();
    #[cfg(feature = "axum")]
    claviron_pipeline::routing::register(&state).unwrap();
    let service = tower::service_fn(|request: AppRequest| async move {
        Ok::<_, Infallible>(Response::new(request.into_body().into_inner()))
    });
    register_other(&state, service).unwrap();
    let application = state.registry_ref().resolve::<Application>().unwrap();
    assert!(state.registry_ref().resolve::<Owner>().is_none());
    let bytes = Bytes::from_static(b"unbuffered request");
    let data = bytes.as_ptr();
    let request = Request::builder()
        .method(Method::POST)
        .body(RequestBody::new(http_body_util::Full::new(bytes)))
        .unwrap();
    let reply = application
        .call(request)
        .await
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(reply, "unbuffered request");
    assert_eq!(reply.as_ptr(), data);
}

#[cfg(feature = "rust")]
#[test]
fn rust_other_cancellation_drops_the_owned_request_body() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Waker;

    struct PendingBody(Arc<AtomicBool>);
    impl http_body::Body for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
            Poll::Pending
        }
    }
    impl Drop for PendingBody {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let state = AppState::standalone("rust-cancel", ".");
    Routing::new(HttpPolicy::unlimited()).register(&state).unwrap();
    #[cfg(feature = "axum")]
    claviron_pipeline::routing::register(&state).unwrap();
    let service = tower::service_fn(|request: AppRequest| async move {
        let _request = request;
        std::future::pending::<Result<AppResponse, Infallible>>().await
    });
    register_other(&state, service).unwrap();
    let application = state.registry_ref().resolve::<Application>().unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    {
        let request =
            Request::new(RequestBody::new(PendingBody(dropped.clone())));
        let mut answering = std::pin::pin!(application.call(request));
        assert!(
            answering
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(!dropped.load(Ordering::Relaxed));
    }
    assert!(dropped.load(Ordering::Relaxed));
}

#[cfg(feature = "warp")]
#[tokio::test]
async fn warp_routes_select_before_body_and_keep_rejections_terminal() {
    use warp::Filter;
    let state = AppState::standalone("warp-fallback", ".");
    Routing::new(HttpPolicy::unlimited()).register(&state).unwrap();
    #[cfg(feature = "axum")]
    claviron_pipeline::routing::register(&state).unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = calls.clone();
    register_owner(
        &state,
        Owner::new(tower::service_fn(move |request: AppRequest| {
            observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                Ok::<_, Infallible>(Response::new(
                    request.into_body().into_inner(),
                ))
            }
        })),
    )
    .unwrap();
    let filter = warp::path("rust").and(warp::path("hello")).map(|| "warp");
    let prefix =
        serde_yaml::from_str::<claviron_http::ext::PathPrefix>("/rust")
            .unwrap();
    register_routes(
        &state,
        [(prefix, claviron_http::warp::router(filter).into())],
    )
    .unwrap();
    let app = state.registry_ref().resolve::<Application>().unwrap();
    let response = app
        .call(
            Request::builder()
                .uri("/rust/hello")
                .body(RequestBody::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "warp"
    );
    let response = app
        .call(
            Request::builder()
                .uri("/rust/missing")
                .body(RequestBody::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    let bytes = Bytes::from_static(b"owner request");
    let original = bytes.as_ptr();
    let response = app
        .call(
            Request::builder()
                .method(Method::POST)
                .uri("/rusty")
                .body(RequestBody::new(http_body_util::Full::new(bytes)))
                .unwrap(),
        )
        .await;
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes, "owner request");
    assert_eq!(bytes.as_ptr(), original);
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
}

/// A door with nothing to do: a test only reads what comes back.
struct Unused;

impl claviron_http::socket::Door for Unused {
    fn commit(
        self: Box<Self>,
        proposal: AppResponse,
        _: claviron_http::socket::Deferred,
    ) -> AppResponse {
        proposal
    }

    fn refuse(
        self: Box<Self>,
        response: AppResponse,
    ) -> AppResponse {
        claviron_http::socket::refusal(response)
    }
}

fn offered() -> claviron_http::socket::Offer {
    use claviron_http::socket::{Carrier, Offer, OfferInfo, SocketKind};

    Offer::new(
        OfferInfo { kind: SocketKind::WebSocket, carrier: Carrier::H1 },
        Unused,
    )
}

/// A session attempt passes through the site like any request, so what the site
/// answers or refuses never reaches who would take it.
#[tokio::test]
async fn a_site_stands_in_front_of_what_takes_a_connection_over() {
    let site: Arc<claviron_pipeline::Site> = Arc::new(
        serde_yaml::from_str(
            "root: .\nroutes:\n  /old:\n    redirect: { to: /, code: 307 }\n",
        )
        .unwrap(),
    );
    let answers = |code: u16| {
        tower::service_fn(move |_: AppRequest| async move {
            let mut response = AppResponse::new(claviron_http::empty_body());
            *response.status_mut() = StatusCode::from_u16(code).unwrap();
            Ok::<_, Infallible>(response)
        })
    };
    let application = |owner: Owner| {
        let state = AppState::standalone("runner-routing", ".");
        register_owner(&state, owner).unwrap();
        Routing::new(HttpPolicy::unlimited())
            .site(Some(Arc::clone(&site)))
            .register(&state)
            .unwrap();
        #[cfg(feature = "axum")]
        claviron_pipeline::routing::register(&state).unwrap();
        ensure_application(&state).unwrap();
        state.registry_ref().resolve::<Application>().unwrap()
    };
    let upgrade = |path: &str| {
        let mut request = Request::get(path)
            .header(http::header::UPGRADE, "websocket")
            .body(RequestBody::empty())
            .unwrap();
        request.extensions_mut().insert(offered());
        request
    };

    let taken = application(Owner::new(answers(200)).upgrades(answers(202)));
    assert_eq!(
        taken.call(upgrade("/old")).await.status(),
        StatusCode::TEMPORARY_REDIRECT
    );
    assert_eq!(
        taken.call(upgrade("/anywhere")).await.status(),
        StatusCode::ACCEPTED
    );

    let unclaimed = application(Owner::new(answers(200)));
    assert_eq!(
        unclaimed.call(upgrade("/old")).await.status(),
        StatusCode::TEMPORARY_REDIRECT
    );
    assert_eq!(
        unclaimed.call(upgrade("/anywhere")).await.status(),
        StatusCode::NOT_IMPLEMENTED
    );
}
