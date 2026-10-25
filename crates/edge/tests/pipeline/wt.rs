use super::*;

struct Handler(&'static str);

#[async_trait]
impl WebTransportHandler for Handler {
    fn name(&self) -> &'static str {
        self.0
    }

    async fn handle(
        &self,
        _: AppState,
        _: AuthContext,
        _: Option<AnyAuthenticated>,
        _: H3WebTransportSession,
    ) -> Result<()> {
        Ok(())
    }
}

#[test]
fn orbit_webtransport_fallback_preserves_exact_native_binding_priority() {
    let registry = WebTransportRegistry::default();
    let host: Host = "app.test".parse().unwrap();
    registry.bind_fallback(host.clone(), Arc::new(Handler("orbit")));
    registry
        .register_handler(host.clone(), "/local", Arc::new(Handler("local")))
        .unwrap();
    assert!(registry.has_host(&host));
    for (path, expected) in [("/local", "local"), ("/remote", "orbit")] {
        let WebTransportBinding::Handler(handler) =
            registry.resolve(&host, path).unwrap();
        assert_eq!(handler.name(), expected);
    }
    registry.bind_fallback(host.clone(), Arc::new(Handler("replacement")));
    let WebTransportBinding::Handler(handler) =
        registry.resolve(&host, "/local").unwrap();
    assert_eq!(handler.name(), "local");
    assert!(
        registry.resolve(&"other.test".parse().unwrap(), "/remote").is_none()
    );
}
