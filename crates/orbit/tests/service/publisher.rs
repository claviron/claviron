use std::time::Duration;

use super::*;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/service/orbit.yaml")
}

#[tokio::test]
async fn prepared_file_overrides_registration_policy_on_manual_reload() {
    let state = AppState::standalone("snapshot-config-test", ".");
    let initial = ServiceSnapshotPolicy::new(
        Duration::from_secs(3),
        Duration::from_secs(12),
    );
    let publisher = ServiceSnapshotPublisher::new(fixture(), initial);
    let mut changes = publisher.policy.subscribe();

    // Exercise the config reader and reload hook directly; no provider boot,
    // runtime generation, event loop or shared-memory segment is started.
    state.config_reader().initialize_owners(&[&publisher]).await.unwrap();
    assert_eq!(publisher.policy(), initial);
    publisher.reload(&state).await.unwrap();
    assert!(changes.has_changed().unwrap());
    let configured = *changes.borrow_and_update();
    assert_eq!(configured.publish_interval(), Duration::from_secs(2));
    assert_eq!(configured.max_age(), Duration::from_secs(9));
    assert_eq!(publisher.policy(), configured);

    publisher.reload(&state).await.unwrap();
    assert!(!changes.has_changed().unwrap());
}

#[tokio::test]
async fn absent_file_preserves_the_explicit_registration_policy() {
    let state = AppState::standalone("snapshot-config-test", ".");
    let initial = ServiceSnapshotPolicy::new(
        Duration::from_secs(3),
        Duration::from_secs(12),
    );
    let publisher = ServiceSnapshotPublisher::new(
        fixture().with_file_name("absent.yaml"),
        initial,
    );
    state.config_reader().initialize_owners(&[&publisher]).await.unwrap();
    state.config_reader().configure_registered(&publisher).await.unwrap();
    assert_eq!(publisher.policy(), initial);
}

#[tokio::test]
async fn invalid_policy_leaves_active_policy_and_notification_unchanged() {
    let initial = ServiceSnapshotPolicy::default();
    let publisher = ServiceSnapshotPublisher::new(fixture(), initial);
    let changes = publisher.policy.subscribe();
    for policy in [
        ServiceSnapshotPolicy::new(Duration::ZERO, initial.max_age()),
        ServiceSnapshotPolicy::new(initial.publish_interval(), Duration::ZERO),
        ServiceSnapshotPolicy::new(Duration::MAX, initial.max_age()),
    ] {
        let invalid =
            OrbitConfig { service_snapshots: policy, ..OrbitConfig::default() };
        assert!(publisher.validate_config(&invalid).is_err());
        assert!(publisher.configure(&invalid).await.is_err());
        assert_eq!(publisher.policy(), initial);
        assert!(!changes.has_changed().unwrap());
    }
}

#[tokio::test]
async fn startup_configuration_notifies_a_subsequent_generation() {
    let publisher = ServiceSnapshotPublisher::new(
        fixture(),
        ServiceSnapshotPolicy::default(),
    );
    let config: OrbitConfig =
        serde_yaml::from_str(include_str!("../fixtures/service/orbit.yaml"))
            .unwrap();
    publisher.configure(&config).await.unwrap();

    // Configuring with no runnable receiver must retain the policy for run().
    let changes = publisher.policy.subscribe();
    assert_eq!(*changes.borrow(), config.service_snapshots);
}

#[test]
fn snapshot_configuration_rejects_misspelled_policy_fields() {
    for yaml in
        ["service_snapshot: {}", "service_snapshots: { publish_intervall: 2s }"]
    {
        assert!(serde_yaml::from_str::<OrbitConfig>(yaml).is_err());
    }
}
