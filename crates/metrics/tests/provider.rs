use super::*;

struct TestFamily(&'static str, &'static str);

impl MetricsFamily for TestFamily {
    fn name(&self) -> &'static str {
        self.0
    }

    fn write_prometheus(
        &self,
        _state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        output.push_str(self.1);
        Ok(())
    }
}

#[test]
fn repeated_family_registration_replaces_in_place() {
    let provider = MetricsProvider::new();
    provider.register(Arc::new(TestFamily("test", "old\n")));
    provider.register(Arc::new(TestFamily("test", "new\n")));

    let state = AppState::standalone("metrics-test", ".");
    assert_eq!(provider.family_count(), 1);
    assert_eq!(provider.render_prometheus(&state).unwrap(), "new\n");
}
