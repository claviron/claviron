use super::*;

use bytes::Bytes;
use http_body::{Body, Frame};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

struct Unread(Arc<AtomicUsize>);

impl Body for Unread {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<std::result::Result<Frame<Bytes>, Self::Error>>> {
        panic!("head routing and a declined file scope must not read the body");
    }
}

impl Drop for Unread {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
}

#[tokio::test]
async fn an_empty_child_scope_does_not_inherit_the_root_file_probe() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("api")).unwrap();
    std::fs::write(directory.path().join("api/item.txt"), "must not be served").unwrap();
    let config: HostConfig = serde_yaml::from_str(&format!(
        "root: {}\nroutes:\n  /:\n    try_files: true\n  /api: {{}}\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    )).unwrap();
    // AppState supplies only identity/context: no provider or engine boots.
    let state = AppState::standalone("native-scope", directory.path());
    let owner = prepare(&state, &"example.test".parse().unwrap(), Arc::new(config), &[],
        #[cfg(feature = "axum")]
        Vec::new(),
        #[cfg(feature = "websocket")]
        None,
    ).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let request = Request::builder().uri("/api/item.txt")
        .body(claviron_http::boxed_body(Unread(drops.clone()))).unwrap();
    let response = owner.handle(request).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn rejected_entry_skips_its_rust_application_and_keeps_the_original_body() {
    use axum::{Router, routing::any};
    use claviron_http::routing::{RouteCondition, RouteEffect};
    struct Reject;
    impl RouteCondition for Reject {
        fn accepts(&self, _: &RuleContext<'_>) -> bool { false }
    }
    let directory = tempfile::tempdir().unwrap();
    let state = AppState::standalone("native-condition", directory.path());
    let registry = Arc::new(HostAppRegistry::default());
    registry.register_route_extension(crate::pipeline::ext::directive::<bool, _>(
        "allowed", |_, _| Ok(RouteEffect::Condition(Arc::new(Reject))),
    )).unwrap();
    state.registry_ref().insert(registry).unwrap();
    let config: HostConfig = serde_yaml::from_str("routes:\n  /secret:\n    allowed: false\n").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let router: Router<AppState> = Router::new().route("/secret", any(move || {
        observed.fetch_add(1, Ordering::Relaxed);
        async { http::StatusCode::OK }
    }));
    let owner = prepare(&state, &"example.test".parse().unwrap(), Arc::new(config), &[], vec![router],
        #[cfg(feature = "websocket")]
        None,
    ).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let response = owner.handle(Request::builder().uri("/secret")
        .body(claviron_http::boxed_body(Unread(drops.clone()))).unwrap()).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
