use claviron_runtime::registry::Component;

use super::*;

#[test]
fn lifecycle_owner_is_finalizable_not_runnable() {
    let state = AppState::standalone("tls-session-lifecycle-test", ".");
    register(&state).unwrap();
    let provider =
        state.registry_ref().resolve::<FleetServerSessionsProvider>().unwrap();

    assert!(provider.clone().as_runnable().is_none());
    assert!(provider.as_finalizable().is_some());
}
