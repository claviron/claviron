use super::*;
#[cfg(feature = "axum")]
use ::axum::Router;
use claviron_http::OtherService;

use claviron_http::ServiceResult;
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pipeline::host::{HostHandlerFuture, HostLayerBuilder, HostRequest};

#[cfg(any(feature = "tcp", feature = "quic"))]
#[path = "apps/protocol.rs"]
mod protocol;

#[cfg(any(feature = "tcp", feature = "quic"))]
#[path = "apps/listeners.rs"]
mod listeners;

#[cfg(any(feature = "tcp", feature = "quic"))]
#[path = "apps/reply.rs"]
mod reply;

#[test]
fn duplicate_snapshot_binding_keeps_the_original_execution() {
    use futures_util::FutureExt;
    struct Reply(StatusCode);
    impl crate::pipeline::apps::other::OtherHandler for Reply {
        fn call(
            &self,
            request: Request<claviron_http::RouteBody>,
        ) -> claviron_http::RouteFuture {
            drop(request);
            Box::pin(std::future::ready(Ok(claviron_http::err_status(self.0))))
        }
    }
    let compile = |status| {
        let mut builder = crate::pipeline::HostPipelineBuilder::<()>::new();
        builder
            .layer(RustRouter::Other(OtherService::new(Reply(status))))
            .unwrap();
        builder.build().into_execution()
    };
    let host: Host = "example.test".parse().unwrap();
    let mut snapshot = HostAppSnapshot::default();
    snapshot.insert(host.clone(), compile(StatusCode::OK)).unwrap();
    assert!(
        snapshot
            .insert(host.clone(), compile(StatusCode::BAD_REQUEST))
            .is_err()
    );
    let owner = snapshot.resolve(&host).unwrap();
    let response = owner
        .handle(Request::new(claviron_http::empty_body()))
        .now_or_never()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

struct Tag;

#[test]
fn changed_http_defaults_recompile_unchanged_host_declarations() {
    use futures_util::FutureExt;
    let state = AppState::standalone("defense-reload", ".");
    let http = Arc::new(crate::HttpConfigProvider::empty("http.yaml".into()));
    state.registry_ref().insert(http.clone()).unwrap();
    let registry = HostAppRegistry::default();
    let builds = Arc::new(AtomicUsize::new(0));
    let observed = builds.clone();
    registry.register_router("example.test", move |_| {
        observed.fetch_add(1, Ordering::Relaxed);
        Router::new().fallback(|| async { StatusCode::OK })
    });
    let table = serde_yaml::from_str("example.test: {}\n").unwrap();
    let first = registry.build_table(&state, &table, None).unwrap();
    let unchanged = registry.build_table(&state, &table, Some(&first)).unwrap();
    assert_eq!(builds.load(Ordering::Relaxed), 1);
    http.replace_from_yaml("defaults:\n  deny: ['^/blocked$']\n").unwrap();
    let changed =
        registry.build_table(&state, &table, Some(&unchanged)).unwrap();
    assert_eq!(builds.load(Ordering::Relaxed), 2);
    for (snapshot, status) in
        [(&first, StatusCode::OK), (&changed, StatusCode::FORBIDDEN)]
    {
        let response = snapshot
            .resolve(&"example.test".parse().unwrap())
            .unwrap()
            .handle(
                Request::builder()
                    .uri("/blocked")
                    .body(claviron_http::empty_body())
                    .unwrap(),
            )
            .now_or_never()
            .expect("head-only fixture is ready")
            .unwrap();
        assert_eq!(response.status(), status);
    }
}

impl RouteLayer for Tag {
    fn prepare(
        &self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        layer.after(|response, _| {
            response.headers.insert(
                "x-example-layer",
                http::HeaderValue::from_static("native"),
            );
        });
        Ok(())
    }
}

struct Inactive;

impl RouteLayer for Inactive {
    fn prepare(
        &self,
        _: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        Ok(())
    }
}

#[test]
fn inactive_provider_layers_keep_the_direct_axum_call_boundary() {
    let state = AppState::standalone("inactive-layers", ".");
    let owner = build_host(
        &crate::pipeline::host::compile::HostCompiler {
            state: &state,
            http: None,
            protocol: None,
        },
        &"example.test".parse().unwrap(),
        Arc::new(HostConfig::new("public")),
        Some(RustRouter::Axum(Router::new())),
        HostContributions {
            layers: vec![Arc::new(Inactive)],
            ..Default::default()
        },
        &HashMap::new(),
        #[cfg(feature = "rate-limit")]
        None,
    )
    .unwrap();
    assert!(owner.0.into_axum().is_ok());
}

struct Pass(Arc<AtomicUsize>);

impl HostHandler for Pass {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(std::future::ready(ServiceResult::Unhandled(request)))
    }
}

struct Provider {
    prepares: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}

impl Component<AppState> for Provider {
    type Deps = ();
    fn name(&self) -> &'static str {
        "example-provider"
    }
}

impl Routable for Provider {
    fn match_host(
        &self,
        key: &Host,
        _: &HostConfig,
    ) -> bool {
        key.to_string() == "example.test"
    }

    fn application(
        self: Arc<Self>,
        _: &AppState,
        _: &Host,
        _: Arc<HostConfig>,
    ) -> Result<Option<Arc<dyn HostHandler>>> {
        Ok(Some(Arc::new(Pass(self.calls.clone()))))
    }

    fn layers(
        self: Arc<Self>,
        _: &AppState,
        _: &Host,
        _: Arc<HostConfig>,
    ) -> Result<Vec<Arc<dyn RouteLayer>>> {
        self.prepares.fetch_add(1, Ordering::Relaxed);
        Ok(vec![Arc::new(Tag)])
    }
}

#[tokio::test]
async fn provider_layers_surround_native_files_and_explicit_axum_without_an_axum_downstream()
 {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("asset.txt"), "native-file").unwrap();
    let mut config = HostConfig::new(directory.path());
    config.files.try_files = true;
    let config = Arc::new(config);
    let state = AppState::standalone("native-composition", directory.path());
    let host = "example.test".parse().unwrap();
    let registry = HostAppRegistry::default();
    let prepares = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    registry.register_routable(Arc::new(Provider {
        prepares: prepares.clone(),
        calls: calls.clone(),
    }));
    let prepared =
        registry.routable_binding(&state, &host, config.clone(), None).unwrap();
    let native = build_host(
        &crate::pipeline::host::compile::HostCompiler {
            state: &state,
            http: None,
            protocol: None,
        },
        &host,
        config.clone(),
        None,
        prepared.clone(),
        &HashMap::new(),
        #[cfg(feature = "rate-limit")]
        None,
    )
    .unwrap();
    let axum = build_host(
        &crate::pipeline::host::compile::HostCompiler {
            state: &state,
            http: None,
            protocol: None,
        },
        &host,
        config,
        Some(RustRouter::Axum(
            Router::new().fallback(|| async { StatusCode::NOT_FOUND }),
        )),
        prepared.clone(),
        &HashMap::new(),
        #[cfg(feature = "rate-limit")]
        None,
    )
    .unwrap();
    for (owner, status, body) in [
        (native.0, StatusCode::OK, "native-file"),
        (axum.0, StatusCode::NOT_FOUND, ""),
    ] {
        let response = owner
            .handle(
                Request::builder()
                    .uri("/asset.txt")
                    .body(claviron_http::empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-example-layer"], "native");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            body
        );
    }
    assert_eq!(prepares.load(Ordering::Relaxed), 1);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn explicit_rust_routers_observe_scoped_layers_and_unread_body_ownership()
{
    use crate::pipeline::extension::{RouteEffect, directive};
    use bytes::Bytes;
    use http_body::{Body, Frame};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct Guard;
    impl RouteLayer for Guard {
        fn prepare(
            &self,
            layer: &mut HostLayerBuilder,
        ) -> crate::Result<()> {
            layer.before(|_, _| {
                std::ops::ControlFlow::Break(claviron_http::err_status(
                    StatusCode::FORBIDDEN,
                ))
            });
            Ok(())
        }
    }
    struct Reply(Arc<AtomicUsize>);
    impl crate::pipeline::apps::other::OtherHandler for Reply {
        fn call(
            &self,
            request: Request<claviron_http::RouteBody>,
        ) -> claviron_http::RouteFuture {
            self.0.fetch_add(1, Ordering::Relaxed);
            drop(request);
            Box::pin(std::future::ready(Ok(http::Response::new(
                claviron_http::empty_body(),
            ))))
        }
    }
    struct Unread(Arc<AtomicUsize>);
    impl Body for Unread {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<std::result::Result<Frame<Bytes>, Self::Error>>>
        {
            panic!(
                "neither the guard nor the terminal router may read this body"
            );
        }
    }
    impl Drop for Unread {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    for axum in [false, true] {
        let state = AppState::standalone("scoped-rust-router", ".");
        let registry = Arc::new(HostAppRegistry::default());
        registry
            .register_route_extension(directive::<bool, _>(
                "guard",
                |_, enabled| {
                    assert!(enabled);
                    Ok(RouteEffect::Layer(Arc::new(Guard)))
                },
            ))
            .unwrap();
        state.registry_ref().insert(registry.clone()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        if axum {
            registry.register_router("example.test", move |_| {
                let calls = observed.clone();
                Router::new().fallback(
                    move |request: Request<::axum::body::Body>| {
                        calls.fetch_add(1, Ordering::Relaxed);
                        async move {
                            drop(request);
                            StatusCode::OK
                        }
                    },
                )
            });
        } else {
            registry.register_other("example.test", move |_, _, _| {
                Ok(OtherService::new(Reply(observed.clone())))
            });
        }
        let table = serde_yaml::from_str("example.test:\n  root: public\n  routes:\n    /private: {guard: true}\n").unwrap();
        let snapshot = registry.build_table(&state, &table, None).unwrap();
        let owner = snapshot.resolve(&"example.test".parse().unwrap()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        for (index, path, status) in [
            (0, "/private/data", StatusCode::FORBIDDEN),
            (1, "/public", StatusCode::OK),
            (2, "/privately", StatusCode::OK),
        ] {
            let request = Request::builder()
                .uri(path)
                .body(claviron_http::boxed_body(Unread(drops.clone())))
                .unwrap();
            let response = owner.handle(request).await.unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(calls.load(Ordering::Relaxed), index);
            assert_eq!(drops.load(Ordering::Relaxed), index + 1);
        }
    }
}

#[test]
fn listener_plans_keep_full_policy_and_never_fall_back_to_unbound_execution() {
    use crate::service::{
        HostService, RequestArrival, resolve_sni_for_listener,
    };
    use claviron_http::config::HttpYaml;
    use claviron_http::ext::ConnCtx;
    use claviron_http::listener::HttpProtocol;
    use claviron_http::{OtherHandler, OtherService, RouteBody, RouteFuture};
    use futures_util::FutureExt;
    use http_body_util::BodyExt;

    struct Echo;
    impl OtherHandler for Echo {
        fn call(
            &self,
            request: http::Request<RouteBody>,
        ) -> RouteFuture {
            let buffered = request
                .extensions()
                .get::<claviron_http::policy::HttpPolicy>()
                .unwrap()
                .requests
                .buffering
                == claviron_http::policy::Buffering::BufferDefault;
            let mut response = http::Response::new(request.into_body());
            if buffered {
                response.headers_mut().insert(
                    "x-buffered",
                    http::HeaderValue::from_static("yes"),
                );
            }
            Box::pin(std::future::ready(Ok(response)))
        }
    }
    let state = AppState::standalone("body-plans", ".");
    let http = serde_yaml::from_str::<HttpYaml>(
        r#"
defaults:
  listener:
    requests: {max_body_size: off, body_read_timeout: off}
listeners:
  open:
    transport: tcp
    listen: 127.0.0.1:8080
    protocols: h1
  limited:
    transport: tcp
    listen: 127.0.0.1:8081
    protocols: h1
    requests: {max_body_size: 4, body_read_timeout: off}
  timed:
    transport: tcp
    listen: 127.0.0.1:8082
    protocols: h1
    requests: {max_body_size: off, body_read_timeout: 2s}
  buffered:
    transport: tcp
    listen: 127.0.0.1:8083
    protocols: h1
    requests: {buffering: buffer_default, buffer_size: 8}
  excluded:
    transport: tcp
    listen: 127.0.0.1:8084
    protocols: h1
"#,
    )
    .unwrap()
    .resolve()
    .unwrap();
    let host = "example.test".parse().unwrap();
    let mut config = HostConfig::new("public");
    config.transport.non_ssl = crate::config::NonSslPolicy::Serve;
    config.listeners = Some(
        ["open", "limited", "timed", "buffered"]
            .map(|id| id.parse().unwrap())
            .into(),
    );
    let config = Arc::new(config);
    let (shared, plans) = build_host(
        &crate::pipeline::host::compile::HostCompiler {
            state: &state,
            http: Some(&http),
            protocol: None,
        },
        &host,
        config,
        Some(RustRouter::Other(OtherService::new(Echo))),
        HostContributions::default(),
        &HashMap::new(),
        #[cfg(feature = "rate-limit")]
        None,
    )
    .unwrap();
    assert_eq!(shared.kind(), "other");
    let plans = plans.unwrap();
    assert_eq!(plans.len(), 4);
    let mut snapshot = HostAppSnapshot::default();
    snapshot.insert(host.clone(), shared).unwrap();
    snapshot.listener_plans.insert(host, plans);
    let mut table: crate::loader::table::VhostTable =
        serde_yaml::from_str("example.test: {aliases: [alias.test]}").unwrap();
    table.set_prepared(
        std::any::TypeId::of::<HostAppSnapshot>(),
        Arc::new(snapshot),
    );
    for listener in ["excluded", "unknown"] {
        assert!(
            resolve_sni_for_listener(
                &table,
                Some("alias.test"),
                &listener.parse().unwrap()
            )
            .is_err()
        );
    }
    {
        let protocol = HttpProtocol::H1;
        for (listener, status) in [
            ("open", StatusCode::OK),
            ("limited", StatusCode::PAYLOAD_TOO_LARGE),
            ("timed", StatusCode::OK),
            ("buffered", StatusCode::OK),
        ] {
            let id = listener.parse().unwrap();
            let mut selected =
                resolve_sni_for_listener(&table, Some("alias.test"), &id)
                    .unwrap();
            let app = selected.application.take().unwrap();
            if listener == "open" {
                assert_eq!(app.kind(), "other");
            }
            let connection = Arc::new(ConnCtx {
                state: Default::default(),
                listener: id.clone(),
                proto: Some(protocol),
                remote: "192.0.2.1:1234".parse().unwrap(),
                local_ip: "192.0.2.2".parse().unwrap(),
                local_port: 8080,
                real_ip: None,
                country_code: None,
                tls: None,
            });
            let service = HostService::new(
                app,
                connection,
                selected,
                http.listeners[&id].policy(),
            );
            let request = http::Request::builder()
                .header("content-length", "5")
                .body(claviron_http::boxed_body(http_body_util::Full::new(
                    bytes::Bytes::from_static(b"12345"),
                )))
                .unwrap();
            let reply = service
                .request(request, RequestArrival::Established)
                .dispatch()
                .now_or_never()
                .unwrap();
            assert_eq!(reply.status(), status, "{listener}: {protocol:?}");
            if status == StatusCode::OK {
                assert_eq!(
                    reply.headers().contains_key("x-buffered"),
                    listener == "buffered"
                );
            }
            let bytes = reply
                .into_body()
                .collect()
                .now_or_never()
                .unwrap()
                .unwrap()
                .to_bytes();
            if status == StatusCode::OK {
                assert_eq!(bytes, "12345");
            }
        }
    }
}
