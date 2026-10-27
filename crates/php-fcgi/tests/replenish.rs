use super::*;

#[test]
fn audit_replenish_policy_preserves_defaults_and_validates_bounds() {
    let policy: ReplenishConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(policy.initial_backoff(), Duration::from_millis(100));
    assert_eq!(
        policy.next_backoff(Duration::from_secs(4)),
        Duration::from_secs(5)
    );
    let policy: ReplenishConfig =
        serde_yaml::from_str("initial_backoff: 300ms\nmax_backoff: 900ms")
            .unwrap();
    assert_eq!(policy.initial_backoff(), Duration::from_millis(300));
    assert_eq!(
        policy.next_backoff(Duration::from_millis(300)),
        Duration::from_millis(600)
    );
    assert_eq!(
        policy.next_backoff(Duration::from_millis(600)),
        Duration::from_millis(900)
    );
    for yaml in [
        "initial_backoff: 0s",
        "initial_backoff: 2s\nmax_backoff: 1s",
        "max_backoff: 0s",
    ] {
        assert!(serde_yaml::from_str::<ReplenishConfig>(yaml).is_err());
    }
    let base = "min_live: 1\nmax_idle: 2\nmax_live: 2\n";
    let enabled: PoolConfig = serde_yaml::from_str(base).unwrap();
    let disabled: PoolConfig =
        serde_yaml::from_str(&format!("{base}replenish: null")).unwrap();
    assert!(enabled.replenish.is_some());
    assert!(disabled.replenish.is_none());
}
