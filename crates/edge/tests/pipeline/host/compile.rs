use super::*;
use crate::config::HostConfig;

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

#[path = "compile/headers.rs"]
mod headers;

#[path = "compile/redirect.rs"]
mod redirect;

struct RegisteredBackend;

#[test]
fn unregistered_backend_directives_fail_during_host_preparation() {
    let state = AppState::standalone("unregistered-backend", ".");
    state.registry_ref().insert(Arc::new(HostAppRegistry::default())).unwrap();
    for source in [
        "upstream: example\n",
        "routes: {'/': {upstream: example}}\n",
        "routes: {'/api': {upstream: example}}\n",
    ] {
        let config =
            Arc::new(serde_yaml::from_str::<HostConfig>(source).unwrap());
        let error = HostCompiler { state: &state, http: None, protocol: None }
            .prepare(
                &"example.test".parse().unwrap(),
                config,
                None,
                &HostContributions::default(),
                #[cfg(feature = "rate-limit")]
                None,
                #[cfg(feature = "websocket")]
                None,
            )
            .err()
            .expect(
                "a configured backend requires a registered preparation owner",
            );
        assert!(
            error.to_string().contains("unresolved route directive 'upstream'"),
            "{error}"
        );
    }
}

impl super::super::BackendHandler for RegisteredBackend {
    fn contract(&self) -> super::super::BackendContract {
        super::super::BackendContract {
            identity: super::super::BackendIdentity::None,
            reply: super::super::BackendReply::Owner,
        }
    }
}

impl super::super::DispatchStep for RegisteredBackend {
    fn call(
        &self,
        request: Request<claviron_http::RouteBody>,
        _context: Option<crate::pipeline::host::HostContext<'_>>,
    ) -> super::super::DispatchCall {
        assert!(request.extensions().is_empty());
        super::super::DispatchCall::ready(
            claviron_http::ServiceResult::Handled(http::Response::new(
                request.into_body(),
            )),
        )
    }
}

#[test]
fn backend_factories_resolve_only_during_preparation_and_preserve_body_ownership()
 {
    use futures_util::FutureExt;
    use http_body_util::BodyExt;
    let state = AppState::standalone("backend-contract", ".");
    let registry = Arc::new(HostAppRegistry::default());
    let preparations = Arc::new(AtomicUsize::new(0));
    let count = preparations.clone();
    registry
        .register_backend_factory(crate::pipeline::backend::factory::<bool, _>(
            "execute",
            crate::pipeline::backend::BackendPlacement::AfterFiles,
            move |context, enabled| {
                assert_eq!(context.policy, HttpPolicy::unlimited());
                count.fetch_add(1, Ordering::Relaxed);
                Ok(if enabled {
                    vec![Arc::new(RegisteredBackend)]
                } else {
                    Vec::new()
                })
            },
        ))
        .unwrap();
    state.registry_ref().insert(registry).unwrap();
    let config =
        Arc::new(serde_yaml::from_str::<HostConfig>("execute: true").unwrap());
    let owner = HostCompiler { state: &state, http: None, protocol: None }
        .prepare(
            &"example.test".parse().unwrap(),
            config,
            None,
            &HostContributions::default(),
            #[cfg(feature = "rate-limit")]
            None,
            #[cfg(feature = "websocket")]
            None,
        )
        .unwrap();
    let prepared = preparations.load(Ordering::Relaxed);
    assert!(prepared > 0);
    for _ in 0..2 {
        let bytes = Bytes::from_static(b"original request");
        let pointer = bytes.as_ptr();
        let mut response = owner
            .handle(Request::new(claviron_http::bytes_body(bytes)))
            .now_or_never()
            .unwrap()
            .unwrap();
        let frame = response
            .body_mut()
            .frame()
            .now_or_never()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(frame.data_ref().unwrap().as_ptr(), pointer);
    }
    assert_eq!(preparations.load(Ordering::Relaxed), prepared);

    for source in ["{}", "execute: null"] {
        HostCompiler { state: &state, http: None, protocol: None }
            .prepare(
                &"example.test".parse().unwrap(),
                Arc::new(serde_yaml::from_str::<HostConfig>(source).unwrap()),
                None,
                &HostContributions::default(),
                #[cfg(feature = "rate-limit")]
                None,
                #[cfg(feature = "websocket")]
                None,
            )
            .unwrap();
    }
    assert_eq!(preparations.load(Ordering::Relaxed), prepared);
}

#[tokio::test]
async fn backend_placement_preserves_explicit_precedence_with_static_files() {
    use crate::pipeline::backend::{BackendPlacement, factory};
    use http_body_util::BodyExt;
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("entry.txt"), "static").unwrap();
    for (placement, expected) in [
        (BackendPlacement::BeforeFiles, "request"),
        (BackendPlacement::AfterFiles, "static"),
    ] {
        let state = AppState::standalone("backend-placement", directory.path());
        let registry = Arc::new(HostAppRegistry::default());
        registry
            .register_backend_factory(factory::<bool, _>(
                "execute",
                placement,
                |_, enabled| {
                    Ok(if enabled {
                        vec![Arc::new(RegisteredBackend)]
                    } else {
                        Vec::new()
                    })
                },
            ))
            .unwrap();
        state.registry_ref().insert(registry).unwrap();
        let config = serde_yaml::from_str::<HostConfig>(&format!(
            "root: {}\nstatic: {{}}\nexecute: true\n",
            serde_yaml::to_string(directory.path()).unwrap().trim(),
        ))
        .unwrap();
        let owner = HostCompiler { state: &state, http: None, protocol: None }
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
        let request = Request::builder()
            .uri("/entry.txt")
            .body(claviron_http::bytes_body(Bytes::from_static(b"request")))
            .unwrap();
        let response = owner.handle(request).await.unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            expected
        );
    }
}

/// What a connection may hand over is the host's term, and it reaches a backend
/// under any route of that host, not only under its root.
#[test]
fn a_backend_under_a_route_sees_the_hosts_connection_terms() {
    use crate::pipeline::backend::{BackendPlacement, factory};
    let state = AppState::standalone("backend-terms", ".");
    let registry = Arc::new(HostAppRegistry::default());
    let seen = Arc::new(AtomicUsize::new(0));
    let observed = seen.clone();
    registry
        .register_backend_factory(factory::<bool, _>(
            "execute",
            BackendPlacement::AfterFiles,
            move |context, _| {
                if context.transport.h1.upgrade {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
                Ok(vec![Arc::new(RegisteredBackend)])
            },
        ))
        .unwrap();
    state.registry_ref().insert(registry).unwrap();
    let config = serde_yaml::from_str::<HostConfig>(
        "transport: {h1: {upgrade: true}}\nroutes: {'/api': {execute: true}}\n",
    )
    .unwrap();
    HostCompiler { state: &state, http: None, protocol: None }
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
    assert!(seen.load(Ordering::Relaxed) > 0);
}

fn two_backends() -> AppState {
    use crate::pipeline::backend::{BackendPlacement, factory};
    let state = AppState::standalone("backend-owners", ".");
    let registry = Arc::new(HostAppRegistry::default());
    for name in ["first", "second"] {
        registry
            .register_backend_factory(factory::<bool, _>(
                name,
                BackendPlacement::AfterFiles,
                |_, _| Ok(vec![Arc::new(RegisteredBackend)]),
            ))
            .unwrap();
    }
    state.registry_ref().insert(registry).unwrap();
    state
}

/// The edge does not know a backend by name, so the one-owner rule is held
/// where the registered backends are known, for flat and route declarations.
#[test]
fn a_scope_declaring_two_registered_owners_fails_at_preparation() {
    let state = two_backends();
    for (source, accepted) in [
        ("routes: {'/api': {first: true, second: true}}\n", false),
        ("first: true\nroutes: {'/': {second: true}}\n", false),
        ("first: true\nroutes: {'/api': {second: true}}\n", true),
        ("routes: {'/api': {first: true, second: null}}\n", true),
    ] {
        let config =
            Arc::new(serde_yaml::from_str::<HostConfig>(source).unwrap());
        let prepared =
            HostCompiler { state: &state, http: None, protocol: None }.prepare(
                &"example.test".parse().unwrap(),
                config,
                None,
                &HostContributions::default(),
                #[cfg(feature = "rate-limit")]
                None,
                #[cfg(feature = "websocket")]
                None,
            );
        match prepared {
            Ok(_) => assert!(accepted, "{source}"),
            Err(error) => assert!(
                !accepted
                    && error.to_string().contains(
                        "declares more than one application owner: first, second"
                    ),
                "{source}: {error}"
            ),
        }
    }
}

/// An owner takes its scope whole: a mount the scope only inherited does not
/// run in front of it. A policy-only child of the same mount keeps it.
#[tokio::test]
async fn an_owned_child_runs_without_the_static_mount_it_inherited() {
    use http_body_util::BodyExt;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(directory.path().join("files/app")).unwrap();
    std::fs::write(directory.path().join("files/app/entry.txt"), "static")
        .unwrap();
    let state = two_backends();
    for (child, expected) in
        [("cors: disabled", "static"), ("first: true", "request")]
    {
        let config = serde_yaml::from_str::<HostConfig>(&format!(
            "root: {}\nroutes:\n  /files:\n    static: {{}}\n  /files/app:\n    {child}\n",
            serde_yaml::to_string(directory.path()).unwrap().trim(),
        ))
        .unwrap();
        let owner = HostCompiler { state: &state, http: None, protocol: None }
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
        let request = Request::builder()
            .uri("/files/app/entry.txt")
            .body(claviron_http::bytes_body(Bytes::from_static(b"request")))
            .unwrap();
        let response = owner.handle(request).await.unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            expected,
            "{child}"
        );
    }
}

#[cfg(feature = "access-log")]
#[test]
fn access_log_off_overrides_the_host_channel_and_inherits_into_children() {
    let config: HostConfig = serde_yaml::from_str(
        "access_log: access\nroutes:\n  /private:\n    access_log: off\n  /private/child: {}\n  /private/explicit:\n    access_log: events\n",
    ).unwrap();
    for (prefix, expected) in [
        ("/", "access"),
        ("/public", "access"),
        ("/private", "off"),
        ("/private/child", "off"),
        ("/private/explicit", "events"),
    ] {
        let prefix: claviron_http::ext::PathPrefix =
            serde_yaml::from_str(prefix).unwrap();
        assert_eq!(
            config.setting_at(&prefix, |scope| scope.access_log()),
            Some(expected),
            "{prefix}",
        );
    }
}

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
    let owner = HostCompiler { state: &state, http: None, protocol: None }
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

struct UnavailableBackend;
impl super::super::BackendHandler for UnavailableBackend {
    fn contract(&self) -> super::super::BackendContract {
        RegisteredBackend.contract()
    }
}
impl super::super::DispatchStep for UnavailableBackend {
    fn call(
        &self,
        request: Request<claviron_http::RouteBody>,
        _context: Option<crate::pipeline::host::HostContext<'_>>,
    ) -> super::super::DispatchCall {
        drop(request);
        super::super::DispatchCall::ready(
            claviron_http::ServiceResult::Handled(claviron_http::err_status(
                http::StatusCode::SERVICE_UNAVAILABLE,
            )),
        )
    }
}

#[tokio::test]
async fn try_files_precedes_a_registered_backend() {
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
        "root: {}\nroutes:\n  /live:\n    try_files: true\n    execute: true\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    ))
    .unwrap();
    let state = AppState::standalone("native-backend-files", directory.path());
    let registry = Arc::new(HostAppRegistry::default());
    registry
        .register_backend_factory(crate::pipeline::backend::factory::<bool, _>(
            "execute",
            crate::pipeline::backend::BackendPlacement::AfterFiles,
            |_, enabled| {
                Ok(if enabled {
                    vec![Arc::new(UnavailableBackend)]
                } else {
                    Vec::new()
                })
            },
        ))
        .unwrap();
    state.registry_ref().insert(registry).unwrap();
    let owner = HostCompiler { state: &state, http: None, protocol: None }
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
    let owner = HostCompiler { state: &state, http: None, protocol: None }
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
