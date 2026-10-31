use super::*;

use claviron_http::ServiceResult;
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pipeline::host::{HostHandlerFuture, HostLayerBuilder, HostRequest};

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
fn inactive_provider_layers_keep_the_original_axum_call_boundary() {
    let state = AppState::standalone("inactive-composition", ".");
    let host = "example.test".parse().unwrap();
    let binding = Binding::Axum(Arc::new(|_| Router::new()));
    let app = binding
        .build_app(
            &state,
            &host,
            Arc::new(HostConfig::new("public")),
            &[],
            &[Arc::new(Inactive)],
            #[cfg(feature = "websocket")]
            Vec::new(),
        )
        .unwrap();
    assert!(matches!(app, RustRouter::Axum(_)));
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
    let native = build_default_app(
        &state,
        &host,
        config.clone(),
        &prepared.applications,
        &prepared.layers,
        #[cfg(feature = "websocket")]
        Vec::new(),
    )
    .unwrap();
    let axum = Binding::Axum(Arc::new(|_| {
        Router::new().fallback(|| async { StatusCode::NOT_FOUND })
    }))
    .build_app(
        &state,
        &host,
        config,
        &prepared.applications,
        &prepared.layers,
        #[cfg(feature = "websocket")]
        Vec::new(),
    )
    .unwrap();
    for (owner, status, body) in [
        (native, StatusCode::OK, "native-file"),
        (axum, StatusCode::NOT_FOUND, ""),
    ] {
        let RustRouter::Other(owner) = owner else {
            panic!("native layers own the outer call");
        };
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
