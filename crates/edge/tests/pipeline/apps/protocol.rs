use super::*;

use futures_util::FutureExt;

const LISTENERS: &str = r#"
defaults:
  listener:
    requests: {max_body_size: off, body_read_timeout: off}
listeners:
  plain:
    transport: tcp
    listen: 127.0.0.1:8080
    protocols: h1
  plain-alt:
    transport: tcp
    listen: 127.0.0.1:8081
    protocols: h1
  tls:
    transport: tcp
    tls: true
    listen: 127.0.0.1:8443
    protocols: [h1, h2]
  quic:
    transport: quic
    listen: 127.0.0.1:8443
    protocols: h3
"#;

#[test]
#[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
fn listener_host_selection_keeps_names_aliases_and_prepared_plans_together() {
    let state = AppState::standalone("listener-host-selection", ".");
    let http =
        serde_yaml::from_str::<claviron_http::config::HttpSnapshot>(LISTENERS)
            .unwrap();
    let registry = HostAppRegistry::default();
    let table: crate::loader::table::VhostTable = serde_yaml::from_str(
        "default: {listeners: tls}\nselected.test: {listeners: [plain, tls, quic], aliases: [alias.test]}\nexcluded.test: {listeners: plain-alt, aliases: [excluded-alias.test]}\nh3.test: {transport: {protocols: h3}}\n",
    )
    .unwrap();
    for name in ["plain", "tls", "quic"] {
        let id = name.parse().unwrap();
        let input =
            crate::transport::publication::select_listener(&http, &id).unwrap();
        let prepared =
            registry.prepare_listener(&state, &table, &input, None).unwrap();
        let mut names = prepared
            .listener_entries(&id)
            .map(|(host, _)| host.to_string())
            .collect::<Vec<_>>();
        names.sort();
        let expected = match name {
            "tls" => vec!["default", "selected.test"],
            "quic" => vec!["h3.test", "selected.test"],
            _ => vec!["selected.test"],
        };
        assert_eq!(names, expected);
        let (canonical, plan) = prepared
            .resolve_application_on(
                &"selected.test".parse().unwrap(),
                Some(&id),
            )
            .unwrap();
        let (alias, alias_plan) = prepared
            .resolve_application_on(&"alias.test".parse().unwrap(), Some(&id))
            .unwrap();
        assert!(Arc::ptr_eq(&canonical.config, &alias.config));
        assert!(std::ptr::eq(plan.unwrap(), alias_plan.unwrap()));
        for excluded in ["excluded.test", "excluded-alias.test"] {
            assert!(
                prepared
                    .resolve_application_on(
                        &excluded.parse().unwrap(),
                        Some(&id)
                    )
                    .is_none(),
                "an excluded exact name cannot enter the default host"
            );
        }
        assert_eq!(
            prepared
                .resolve_application_on(
                    &"unknown.test".parse().unwrap(),
                    Some(&id)
                )
                .is_some(),
            name == "tls"
        );
        assert!(
            table
                .resolve_application_on(
                    &"selected.test".parse().unwrap(),
                    Some(&id)
                )
                .is_none(),
            "a declaration alone cannot admit a connection without its prepared plan"
        );
    }
}

struct HttpOnly {
    matches: Arc<AtomicUsize>,
    prepares: Arc<AtomicUsize>,
}

impl Component<AppState> for HttpOnly {
    type Deps = ();

    fn name(&self) -> &'static str {
        "http-only"
    }
}

impl Routable for HttpOnly {
    fn match_protocol(
        &self,
        protocol: RouteProtocol,
    ) -> bool {
        self.matches.fetch_add(1, Ordering::Relaxed);
        protocol == RouteProtocol::Http
    }

    fn match_host(
        &self,
        _key: &Host,
        _host: &HostConfig,
    ) -> bool {
        true
    }

    fn route_extensions(&self) -> Vec<Arc<dyn RouteExtension>> {
        vec![crate::pipeline::extension::directive::<bool, _>(
            "plain_only",
            |_, enabled| {
                assert!(enabled);
                Ok(RouteEffect::Layer(Arc::new(Deny)))
            },
        )]
    }

    fn layers(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> Result<Vec<Arc<dyn RouteLayer>>> {
        Ok(vec![self])
    }
}

impl RouteLayer for HttpOnly {
    fn prepare(
        &self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        self.prepares.fetch_add(1, Ordering::Relaxed);
        layer.after(|response, _| {
            response.headers.insert(
                "x-http-only",
                http::HeaderValue::from_static("selected"),
            );
        });
        Ok(())
    }
}

#[test]
fn protocol_selection_is_cold_and_http_layers_are_absent_from_tls_and_quic() {
    let state = AppState::standalone("protocol-selection", ".");
    let http = Arc::new(crate::HttpConfigProvider::empty("http.yaml".into()));

    http.replace_from_yaml(LISTENERS).unwrap();
    state.registry_ref().insert(http.clone()).unwrap();
    let registry = HostAppRegistry::default();
    registry.register_router("example.test", |_| {
        Router::new().fallback(|| async { StatusCode::OK })
    });
    let matches = Arc::new(AtomicUsize::new(0));
    let prepares = Arc::new(AtomicUsize::new(0));
    registry.register_routable(Arc::new(HttpOnly {
        matches: matches.clone(),
        prepares: prepares.clone(),
    }));
    let mut table: crate::loader::table::VhostTable =
        serde_yaml::from_str("example.test: {}\n").unwrap();
    let snapshot = registry.build_table(&state, &table, None).unwrap();
    assert_eq!(prepares.load(Ordering::Relaxed), 0);
    // An unbound lookup must not accidentally expose a protocol-specific layer.
    assert!(
        snapshot
            .resolve(&"example.test".parse().unwrap())
            .unwrap()
            .into_axum()
            .is_ok()
    );
    table.set_prepared(
        std::any::TypeId::of::<HostAppSnapshot>(),
        Arc::new(snapshot),
    );
    let host = "example.test".parse().unwrap();
    for (listener, selected) in
        [("plain", true), ("plain-alt", true), ("tls", false), ("quic", false)]
    {
        let id = listener.parse().unwrap();
        let input = crate::transport::publication::select_listener(
            &http.snapshot(),
            &id,
        )
        .unwrap();
        let prepared =
            registry.prepare_listener(&state, &table, &input, None).unwrap();
        let selections = matches.load(Ordering::Relaxed);
        let (_, owner) =
            prepared.resolve_application_on(&host, Some(&id)).unwrap();
        let owner = owner.unwrap();
        assert_eq!(owner.clone().into_axum().is_err(), selected);
        let reply = owner
            .handle(Request::new(claviron_http::empty_body()))
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(reply.status(), StatusCode::OK);
        assert_eq!(reply.headers().contains_key("x-http-only"), selected);
        assert_eq!(matches.load(Ordering::Relaxed), selections);
    }
    assert_eq!(prepares.load(Ordering::Relaxed), 2);

    // Removing HTTP listeners does not prepare a layer or resolve its resources.
    http.replace_from_yaml(
        "listeners:\n  tls:\n    transport: tcp\n    tls: true\n    listen: 127.0.0.1:8443\n    protocols: [h1, h2]\n    requests: {max_body_size: off, body_read_timeout: off}\n",
    ).unwrap();
    let secure_only = registry.build_table(&state, &table, None).unwrap();
    assert_eq!(prepares.load(Ordering::Relaxed), 2);
    assert!(secure_only.resolve(&host).unwrap().into_axum().is_ok());
}

struct Deny;

impl RouteLayer for Deny {
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

#[test]
fn protocol_selection_omits_inactive_directives_without_losing_their_owner() {
    let state = AppState::standalone("protocol-directive", ".");
    let http = Arc::new(crate::HttpConfigProvider::empty("http.yaml".into()));
    http.replace_from_yaml(LISTENERS).unwrap();
    state.registry_ref().insert(http.clone()).unwrap();
    let registry = Arc::new(HostAppRegistry::default());
    state.registry_ref().insert(registry.clone()).unwrap();
    registry.register_router("example.test", |_| {
        Router::new().fallback(|| async { StatusCode::OK })
    });
    registry.register_routable(Arc::new(HttpOnly {
        matches: Arc::new(AtomicUsize::new(0)),
        prepares: Arc::new(AtomicUsize::new(0)),
    }));
    let mut table: crate::loader::table::VhostTable = serde_yaml::from_str(
        "example.test:\n  routes:\n    /private: {plain_only: true}\n",
    )
    .unwrap();
    let snapshot = registry.build_table(&state, &table, None).unwrap();
    table.set_prepared(
        std::any::TypeId::of::<HostAppSnapshot>(),
        Arc::new(snapshot),
    );
    let host = "example.test".parse().unwrap();
    for (listener, status) in [
        ("plain", StatusCode::FORBIDDEN),
        ("tls", StatusCode::OK),
        ("quic", StatusCode::OK),
    ] {
        let id = listener.parse().unwrap();
        let input = crate::transport::publication::select_listener(
            &http.snapshot(),
            &id,
        )
        .unwrap();
        let prepared =
            registry.prepare_listener(&state, &table, &input, None).unwrap();
        let (_, owner) =
            prepared.resolve_application_on(&host, Some(&id)).unwrap();
        let response = owner
            .unwrap()
            .handle(
                Request::builder()
                    .uri("/private/item")
                    .body(claviron_http::empty_body())
                    .unwrap(),
            )
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), status, "{listener}");
    }
}
