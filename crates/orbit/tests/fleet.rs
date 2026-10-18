use super::*;

#[test]
fn fleet_provider_is_a_capability_not_a_runnable() {
    let provider = Arc::new(FleetProvider::new());

    assert!(provider.as_runnable().is_none());
}
