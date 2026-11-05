use super::*;

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use claviron_http::{
    OtherHandler, OtherService, RouteBody, RouteFuture, empty_body,
};
use futures_util::FutureExt;
use http::{Request, Response, StatusCode};

use crate::pipeline::apps::HostContributions;
use crate::pipeline::extension::{RouteEffect, RouteLayer, directive};
use crate::pipeline::host::{
    HostExecution,
    compile::{HostCompileTarget, HostCompiler},
};
use crate::pipeline::{HostLayerBuilder, RustRouter};

#[derive(Clone)]
struct Marker(&'static str);

struct Tag(&'static str);

impl RouteLayer for Tag {
    fn prepare(
        &self,
        layer: &mut HostLayerBuilder,
    ) -> Result<()> {
        let name = self.0;
        layer.before(move |head, _| {
            head.extensions.insert(Marker(name));
            ControlFlow::Continue(())
        });
        layer.after(move |head, _| {
            head.headers.append(
                "x-example-order",
                http::HeaderValue::from_static(name),
            );
        });
        Ok(())
    }
}

struct Reply;

impl OtherHandler for Reply {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let value = request.extensions().get::<Marker>().map(|marker| marker.0);
        let mut response = Response::new(empty_body());
        if let Some(value) = value {
            response.headers_mut().insert(
                "x-example-marker",
                http::HeaderValue::from_static(value),
            );
        }
        Box::pin(std::future::ready(Ok(response)))
    }
}

fn state(registry: Arc<HostAppRegistry>) -> AppState {
    let state = AppState::standalone("code-layers", ".");
    state.registry_ref().insert(registry).unwrap();
    state
}

fn compile(
    state: &AppState,
    config: &str,
    listener: Option<&ListenerConfig>,
) -> claviron_runtime::registry::Result<HostExecution> {
    HostCompiler { state, http: None, protocol: None }.prepare_target(
        HostCompileTarget { host: &"example.test".parse().unwrap(), listener },
        Arc::new(serde_yaml::from_str(config).unwrap()),
        Some(RustRouter::Other(OtherService::new(Reply))),
        &HostContributions::default(),
        #[cfg(feature = "rate-limit")]
        None,
        #[cfg(feature = "websocket")]
        None,
    )
}

fn response(owner: &HostExecution) -> claviron_http::RouteResponse {
    owner.handle(Request::new(empty_body())).now_or_never().unwrap().unwrap()
}

#[test]
fn registered_layer_selects_once_and_precedes_the_rust_router() {
    let registry = Arc::new(HostAppRegistry::default());
    let selected = Arc::new(AtomicUsize::new(0));
    let observed = selected.clone();
    let selected_host: Host = "example.test".parse().unwrap();
    registry.register_layer(
        move |host, _, _| {
            observed.fetch_add(1, Ordering::Relaxed);
            host == &selected_host
        },
        Tag("direct"),
    );
    let state = state(registry);
    let owner = compile(&state, "root: public", None).unwrap();
    let prepared = selected.load(Ordering::Relaxed);
    assert!(prepared > 0);
    for _ in 0..2 {
        let response = response(&owner);
        assert_eq!(response.headers()["x-example-marker"], "direct");
        assert_eq!(response.headers()["x-example-order"], "direct");
    }
    assert_eq!(selected.load(Ordering::Relaxed), prepared);
}

#[test]
fn unmatched_and_empty_code_layers_keep_the_direct_owner() {
    struct Empty;
    impl RouteLayer for Empty {
        fn prepare(
            &self,
            _: &mut HostLayerBuilder,
        ) -> Result<()> {
            Ok(())
        }
    }
    let registry = Arc::new(HostAppRegistry::default());
    registry.register_pipeline(
        |_, _, _| false,
        |_| -> Result<HostPipelineBuilder> {
            panic!("unselected factories must not prepare");
        },
    );
    registry.register_layer(|_, _, _| true, Empty);
    let owner = compile(&state(registry), "root: public", None).unwrap();
    assert_eq!(owner.kind(), "other");
    assert!(!response(&owner).headers().contains_key("x-example-marker"));
}

#[test]
fn configuration_and_code_use_the_same_route_layer_contract() {
    let registry = Arc::new(HostAppRegistry::default());
    registry
        .register_route_extension(directive::<bool, _>(
            "example_tag",
            |_, enabled| {
                assert!(enabled);
                Ok(RouteEffect::Layer(Arc::new(Tag("configured"))))
            },
        ))
        .unwrap();
    registry.register_layer(|_, _, _| true, Tag("direct"));
    registry.register_pipeline(
        |_, _, _| true,
        |_| {
            let mut builder = HostPipelineBuilder::new();
            builder.layer(&Tag("pipeline"))?;
            Ok(builder.build())
        },
    );
    let owner =
        compile(&state(registry), "root: public\nexample_tag: true", None)
            .unwrap();
    let response = response(&owner);
    assert_eq!(response.headers()["x-example-marker"], "pipeline");
    let order = response
        .headers()
        .get_all("x-example-order")
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(order, ["pipeline", "direct", "configured"]);
}

#[cfg(any(feature = "tcp", feature = "quic"))]
#[test]
fn protocol_selection_and_effective_scope_are_prepared_for_each_listener() {
    let registry = Arc::new(HostAppRegistry::default());
    let prepared = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = prepared.clone();
    registry.register_pipeline(
        |_, _, protocol| protocol == RouteProtocol::Https,
        move |context| {
            assert_eq!(context.protocol, Some(RouteProtocol::Https));
            assert_eq!(context.listener.unwrap().id.as_str(), "secure");
            observed.lock().unwrap().push((
                context.prefix.as_str().to_owned(),
                context.scope.policy.requests.max_body_size,
            ));
            let mut builder = HostPipelineBuilder::new();
            builder.layer(&Tag("tls"))?;
            Ok(builder)
        },
    );
    let state = state(registry);
    let http: claviron_http::config::HttpSnapshot = serde_yaml::from_str(
        r#"
defaults:
  listener:
    requests: {max_body_size: off, body_read_timeout: off}
listeners:
  plain: {transport: tcp, listen: '127.0.0.1:8080', protocols: h1}
  secure: {transport: tcp, tls: true, listen: '127.0.0.1:8443', protocols: h1}
  quic: {transport: quic, listen: '127.0.0.1:8443', protocols: h3}
"#,
    )
    .unwrap();
    let config = "root: public\nrequests: {max_body_size: 1024}\nroutes:\n  /private:\n    requests: {max_body_size: 512}\n";
    let unbound = compile(&state, config, None).unwrap();
    assert!(!response(&unbound).headers().contains_key("x-example-marker"));
    for name in ["plain", "secure", "quic"] {
        let owner = compile(
            &state,
            config,
            Some(&http.listeners[&name.parse().unwrap()]),
        )
        .unwrap();
        let response = response(&owner);
        assert_eq!(
            response.headers().contains_key("x-example-marker"),
            name == "secure"
        );
    }
    let prepared = prepared.lock().unwrap();
    assert!(!prepared.is_empty());
    assert!(
        prepared
            .iter()
            .any(|(prefix, size)| prefix == "/private" && *size == Some(512))
    );
    assert!(
        prepared
            .iter()
            .filter(|(prefix, _)| prefix == "/")
            .all(|(_, size)| *size == Some(1024))
    );
}

#[test]
fn failed_pipeline_preparation_leaves_the_previous_owner_usable() {
    let fail = Arc::new(AtomicBool::new(false));
    let observed = fail.clone();
    let registry = Arc::new(HostAppRegistry::default());
    registry.register_pipeline(
        |_, _, _| true,
        move |_| {
            if observed.load(Ordering::Relaxed) {
                return Err(crate::Error::Invalid(
                    "example preparation failure".into(),
                ));
            }
            let mut builder = HostPipelineBuilder::new();
            builder.layer(&Tag("retained"))?;
            Ok(builder)
        },
    );
    let state = state(registry);
    let previous = compile(&state, "root: public", None).unwrap();
    fail.store(true, Ordering::Relaxed);
    assert!(compile(&state, "root: public", None).is_err());
    assert_eq!(response(&previous).status(), StatusCode::OK);
    assert_eq!(response(&previous).headers()["x-example-marker"], "retained");
}
