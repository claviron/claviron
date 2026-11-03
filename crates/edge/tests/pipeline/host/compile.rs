use super::*;

use bytes::Bytes;
#[cfg(feature = "axum")]
use claviron_http::dispatch::RuleContext;
use http::Request;
use http_body::{Body, Frame};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

struct Unread(Arc<AtomicUsize>);

#[path = "compile/defense.rs"]
mod defense;

impl Body for Unread {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, Self::Error>>> {
        panic!("head routing and a declined file scope must not read the body");
    }
}

impl Drop for Unread {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn an_empty_child_scope_does_not_inherit_the_root_file_probe() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("api")).unwrap();
    std::fs::write(directory.path().join("api/item.txt"), "must not be served")
        .unwrap();
    let config: HostConfig = serde_yaml::from_str(&format!(
        "root: {}\nroutes:\n  /:\n    try_files: true\n  /api: {{}}\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    ))
    .unwrap();
    // AppState supplies only identity/context: no provider or engine boots.
    let state = AppState::standalone("native-scope", directory.path());
    let owner = HostCompiler { state: &state, http: None }
        .prepare(
            &"example.test".parse().unwrap(),
            Arc::new(config),
            None,
            &HostContributions::default(),
            #[cfg(feature = "rate-limit")]
            None,
            #[cfg(feature = "websocket")]
            None,
        )
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let request = Request::builder()
        .uri("/api/item.txt")
        .body(claviron_http::boxed_body(Unread(drops.clone())))
        .unwrap();
    let response = owner.handle(request).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[cfg(feature = "php_embed")]
#[tokio::test]
async fn try_files_precedes_a_declared_php_application_without_starting_php() {
    use http::StatusCode;
    use http_body_util::BodyExt;

    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("live")).unwrap();
    std::fs::write(
        directory.path().join("live/data.json"),
        "{\"source\":\"file\"}",
    )
    .unwrap();
    let config: HostConfig = serde_yaml::from_str(&format!(
        "root: {}\nroutes:\n  /live:\n    try_files: true\n    php: {{runner: application, application: example}}\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    )).unwrap();
    let state = AppState::standalone("native-php-files", directory.path());
    // No PHP service is registered: a selected application answers 503.
    let owner = HostCompiler { state: &state, http: None }
        .prepare(
            &"example.test".parse().unwrap(),
            Arc::new(config),
            None,
            &HostContributions::default(),
            #[cfg(feature = "rate-limit")]
            None,
            #[cfg(feature = "websocket")]
            None,
        )
        .unwrap();

    for (uri, status, expected) in [
        ("/live/data.json?x=1", StatusCode::OK, "{\"source\":\"file\"}"),
        ("/live/missing.json", StatusCode::SERVICE_UNAVAILABLE, ""),
        ("/live/index.php", StatusCode::SERVICE_UNAVAILABLE, ""),
        ("/lively/missing.json", StatusCode::NOT_FOUND, ""),
    ] {
        let drops = Arc::new(AtomicUsize::new(0));
        let response = owner
            .handle(
                Request::builder()
                    .uri(uri)
                    .body(claviron_http::boxed_body(Unread(drops.clone())))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{uri}");
        assert_eq!(drops.load(Ordering::Relaxed), 1, "{uri}");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            expected,
            "{uri}"
        );
    }
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn rejected_entry_skips_its_rust_application_and_keeps_the_original_body()
{
    use crate::pipeline::extension::{RouteCondition, RouteEffect};
    use axum::{Router, routing::any};
    struct Reject;
    impl RouteCondition for Reject {
        fn accepts(
            &self,
            _: &RuleContext<'_>,
        ) -> bool {
            false
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let state = AppState::standalone("native-condition", directory.path());
    let registry = Arc::new(HostAppRegistry::default());
    registry
        .register_route_extension(crate::pipeline::extension::directive::<
            bool,
            _,
        >("allowed", |_, _| {
            Ok(RouteEffect::Condition(Arc::new(Reject)))
        }))
        .unwrap();
    state.registry_ref().insert(registry).unwrap();
    let config: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /secret:\n    allowed: false\n",
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let router = Router::new().route(
        "/secret",
        any(move || {
            observed.fetch_add(1, Ordering::Relaxed);
            async { http::StatusCode::OK }
        }),
    );
    let mut contributions = HostContributions::default();
    contributions
        .applications
        .push(crate::pipeline::apps::axum::application(router));
    let owner = HostCompiler { state: &state, http: None }
        .prepare(
            &"example.test".parse().unwrap(),
            Arc::new(config),
            None,
            &contributions,
            #[cfg(feature = "rate-limit")]
            None,
            #[cfg(feature = "websocket")]
            None,
        )
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let response = owner
        .handle(
            Request::builder()
                .uri("/secret")
                .body(claviron_http::boxed_body(Unread(drops.clone())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
