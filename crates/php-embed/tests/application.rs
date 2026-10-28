use std::time::Duration;

use super::*;
use crate::PhpEmbedRuntime;
use crate::test_support::ScriptFixture;
use claviron_runtime::AppState;

#[test]
fn configuration_is_local_and_validated() {
    let script = ScriptFixture::new();
    let duplicate = ScriptFixture::new();
    let app =
        PhpApplication::new("web", script.path()).instances(8).max_waiting(64);
    assert!(app.validate().is_ok());
    assert!(app.clone().instances(0).validate().is_err());
    assert!(
        app.clone()
            .retry(claviron_app::ApplicationRetryPolicy::new(
                Duration::ZERO,
                Duration::from_secs(1),
                None
            ))
            .validate()
            .is_err()
    );
    let state = AppState::standalone("application-catalog-test", ".");
    PhpEmbedRuntime::builder()
        .application(PhpApplication::new("web", script.path()))
        .build()
        .register(&state)
        .unwrap();
    let runtime = state.registry_ref().resolve::<PhpEmbedRuntime>().unwrap();
    assert!(
        runtime
            .register_application(PhpApplication::new("web", duplicate.path()))
            .is_err()
    );
}
