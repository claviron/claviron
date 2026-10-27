use super::*;

#[test]
fn audit_geoip_cache_configuration_controls_construction() {
    let default: GeoIpConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(default.cache.capacity, 100_000);
    assert_eq!(default.cache.ttl, Duration::from_secs(1800));
    let configured: GeoIpConfig =
        serde_yaml::from_str("cache: { capacity: 7, ttl: 42s }").unwrap();
    let cache = configured.cache.build();
    assert_eq!(cache.policy().max_capacity(), Some(7));
    assert_eq!(cache.policy().time_to_live(), Some(Duration::from_secs(42)));
    for yaml in [
        "cache: { capacity: -1 }",
        "cache: { ttl: invalid }",
        "cache: { ttl: 1001years }",
    ] {
        assert!(serde_yaml::from_str::<GeoIpConfig>(yaml).is_err());
    }
    let disabled: GeoIpConfig =
        serde_yaml::from_str("cache: { capacity: 0, ttl: 0s }").unwrap();
    let cache = disabled.cache.build();
    cache.insert("192.0.2.1".parse::<IpAddr>().unwrap(), "XY".into());
    assert!(cache.get(&"192.0.2.1".parse::<IpAddr>().unwrap()).is_none());
}
