use super::*;

#[test]
fn audit_geoip_disabled_configuration_installs_the_fallback() {
    let config =
        GeoIpConfig { non_routable: "XY".into(), ..GeoIpConfig::disabled() };
    boot_geoip(&config);
    assert!(GEO_DB.get().is_none());
    for address in ["127.0.0.1", "192.0.2.1", "::1"] {
        assert_eq!(geoip_lookup(address.parse().unwrap()), "XY");
    }
}
