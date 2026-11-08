use super::*;

fn capacity(
    yaml: &str,
    protocol: UpstreamProtocol,
) -> Result<Capacity, ConfigError> {
    serde_yaml::from_str::<CapacityConfig>(yaml)
        .unwrap()
        .compile(protocol, None)
}

#[test]
fn audit_upstream_warm_capacity_separates_connections_and_streams() {
    let h1 =
        capacity("max_live: 4\nmin_idle: 2", UpstreamProtocol::H1).unwrap();
    assert!(!h1.warmup());
    assert_eq!(h1.min_idle(), 2);
    assert_eq!(h1.max_live(), 4);
    let h2 = capacity("max_live: 4", UpstreamProtocol::H2).unwrap();
    assert!(!h2.warmup());
    assert_eq!(h2.max_streams(), None);
    assert_eq!(h2.max_live(), 4);
    let h2 = capacity(
        "max_live: 4\nmax_streams: 16\nwarmup: true",
        UpstreamProtocol::H2,
    )
    .unwrap();
    assert!(h2.warmup());
    assert_eq!(h2.max_streams(), Some(16));
    assert_eq!(h2.max_live(), 4);
    assert!(matches!(
        capacity("max_live: 4\nmin_idle: 0", UpstreamProtocol::H2),
        Err(ConfigError::InvalidCapacity { field: "min_idle", .. })
    ));
    assert!(
        capacity("max_live: 4\nmax_idle: 5", UpstreamProtocol::H2).is_err()
    );
    assert!(
        capacity(
            "max_live: 4\nmax_idle: 0\nwarmup: true",
            UpstreamProtocol::H2
        )
        .is_err()
    );
    assert!(matches!(
        capacity("max_live: 4\nmax_streams: 16", UpstreamProtocol::H1),
        Err(ConfigError::InvalidCapacity { field: "max_streams", .. })
    ));
    assert!(
        capacity("max_live: 4\nmax_streams: 0", UpstreamProtocol::H2).is_err()
    );
    assert!(capacity("{}", UpstreamProtocol::H1).is_err());
}

#[test]
fn audit_upstream_warm_retry_policy_is_validated_when_used() {
    let policy = "replenish: {initial_backoff: 0s, max_backoff: 0s}";
    assert!(
        capacity(&format!("max_live: 4\n{policy}"), UpstreamProtocol::H2)
            .is_ok()
    );
    assert!(
        capacity(
            &format!("max_live: 4\nwarmup: true\n{policy}"),
            UpstreamProtocol::H2
        )
        .is_err()
    );
    assert!(
        capacity(
            &format!("max_live: 4\nmin_idle: 2\n{policy}"),
            UpstreamProtocol::H1
        )
        .is_err()
    );
}
