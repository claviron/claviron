use super::*;
#[cfg(feature = "axum")]
use ::axum::Router;
use claviron_http::OtherService;

use claviron_http::ServiceResult;
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pipeline::host::{HostHandlerFuture, HostLayerBuilder, HostRequest};

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
        &state,
        &"example.test".parse().unwrap(),
        Arc::new(HostConfig::new("public")),
        Some(RustRouter::Axum(Router::new())),
        HostContributions {
            layers: vec![Arc::new(Inactive)],
            ..Default::default()
        },
        #[cfg(feature = "rate-limit")]
        None,
    )
    .unwrap();
    assert!(owner.into_axum().is_ok());
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
        registry.routable_binding(&state, &host, config.clone()).unwrap();
    let native = build_host(
        &state,
        &host,
        config.clone(),
        None,
        prepared.clone(),
        #[cfg(feature = "rate-limit")]
        None,
    )
    .unwrap();
    let axum = build_host(
        &state,
        &host,
        config,
        Some(RustRouter::Axum(
            Router::new().fallback(|| async { StatusCode::NOT_FOUND }),
        )),
        prepared.clone(),
        #[cfg(feature = "rate-limit")]
        None,
    )
    .unwrap();
    for (owner, status, body) in [
        (native, StatusCode::OK, "native-file"),
        (axum, StatusCode::NOT_FOUND, ""),
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
