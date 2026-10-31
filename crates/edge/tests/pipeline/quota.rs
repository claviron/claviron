//! Exercise the public Rust binding API without booting providers or a runtime.

use super::*;
use claviron_http::ext::{PathPrefix, RealIpExt};
use claviron_http::rate_limit::{IpKey, RateLimiter};
use claviron_orbit::quota::{Quota, QuotaBacking, QuotaConfig, QuotaTable};
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

#[test]
fn public_register_quota_binds_the_same_admission_to_canonical_and_alias() {
    let store = Quota::new(QuotaConfig {
        units: NonZeroU64::new(1).unwrap(),
        period: Duration::from_secs(3600),
        burst: NonZeroU64::new(1).unwrap(),
        table: QuotaTable {
            slots: NonZeroUsize::new(8).unwrap(),
            shards: NonZeroUsize::new(1).unwrap(),
            key_bytes: NonZeroUsize::new(64).unwrap(),
            probes: NonZeroUsize::new(8).unwrap(),
        },
        cas_attempts: NonZeroUsize::new(64).unwrap(),
        store: QuotaBacking::LocalGuard {},
    })
    .unwrap();
    let limiter = RateLimiter::new(
        store,
        IpKey,
        b"test".as_slice(),
        NonZeroU64::new(1).unwrap(),
    )
    .unwrap();
    let registry = HostAppRegistry::default();
    let host = "example.test".parse().unwrap();
    registry.register_quota(host, PathPrefix::root(), limiter.clone()).unwrap();
    assert!(
        registry
            .register_quota(
                "example.test".parse().unwrap(),
                PathPrefix::root(),
                limiter
            )
            .is_err()
    );
    let table: crate::loader::table::VhostTable = serde_yaml::from_str(
        "example.test:\n  root: public\n  aliases: [alias.example.test]\n",
    )
    .unwrap();
    let host = "example.test".parse().unwrap();
    let quotas = {
        let mut bindings = registry.quotas.write().unwrap();
        bindings.seal("runtime", &table).unwrap();
        bindings
            .prepare(
                false,
                &host,
                &table.canonical_config(&host).unwrap(),
                None,
            )
            .unwrap()
    };
    let mut app: HostApp = AppKind::Bypass(BypassService::Noop).into();
    app.quotas = quotas;
    let mut snapshot = HostAppSnapshot::default();
    for key in table.host_keys_for_canonical(&host) {
        snapshot.insert(key, app.clone()).unwrap();
    }
    let canonical = snapshot.resolve(&host).unwrap().quotas.unwrap();
    let alias = snapshot
        .resolve(&"alias.example.test".parse().unwrap())
        .unwrap()
        .quotas
        .unwrap();
    assert!(Arc::ptr_eq(&canonical, &alias));
    let mut request = http::Request::new(());
    request.extensions_mut().insert(RealIpExt("192.0.2.1".parse().unwrap()));
    let (mut head, ()) = request.into_parts();
    assert!(canonical.check(&mut head).is_ok());
    assert_eq!(
        alias.check(&mut head).unwrap_err().into_response().status(),
        http::StatusCode::TOO_MANY_REQUESTS
    );
}
