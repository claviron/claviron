use super::*;
use claviron_http::ext::RealIpExt;
use claviron_orbit::quota::{QuotaBacking, QuotaTable};
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

fn definition() -> RateLimitConfig {
    RateLimitConfig {
        namespace: "test".into(),
        cost: NonZeroU64::new(1).unwrap(),
        headers: false,
        quota: QuotaConfig {
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
        },
    }
}

fn config() -> HostConfig {
    let mut host = HostConfig::new("public");
    host.rate_limit = Some(definition());
    host
}

fn table(host: Arc<HostConfig>) -> VhostTable {
    VhostTable::from_host_map([("example.test".into(), host)]).unwrap()
}

fn request(
    path: &str,
    ip: &str,
) -> Request<()> {
    let mut request = Request::builder().uri(path).body(()).unwrap();
    request.extensions_mut().insert(RealIpExt(ip.parse().unwrap()));
    request
}

fn status(
    result: std::result::Result<Option<Snapshot>, HostResponse>
) -> http::StatusCode {
    match result {
        Ok(_) => http::StatusCode::OK,
        Err(reply) => reply.into_response().status(),
    }
}

#[test]
fn aliases_share_one_prepared_budget_and_other_definitions_are_independent() {
    let config = config();
    let registry = QuotaRegistrations::default();
    let host = "example.test".parse().unwrap();
    let first = registry.prepare(false, &host, &config, None).unwrap().unwrap();
    let alias = first.clone();
    let other = registry.prepare(false, &host, &config, None).unwrap().unwrap();
    let request = request("/", "192.0.2.1");
    assert!(first.check(&request).is_ok());
    assert_eq!(
        status(alias.check(&request)),
        http::StatusCode::TOO_MANY_REQUESTS
    );
    assert!(other.check(&request).is_ok());
}

#[test]
fn specific_prefix_selects_one_store_without_charging_its_parent() {
    let mut config = config();
    let mut child = HostConfig::new("public");
    child.route_rate_limit = Some(definition());
    config.routes = claviron_http::config::routes::RouteTable::new([(
        serde_yaml::from_str("/api").unwrap(),
        Arc::new(child),
    )]);
    let quotas = QuotaRegistrations::default()
        .prepare(false, &"example.test".parse().unwrap(), &config, None)
        .unwrap()
        .unwrap();
    assert!(quotas.check(&request("/api/x", "192.0.2.1")).is_ok());
    assert!(quotas.check(&request("/apiary", "192.0.2.1")).is_ok());
    assert_eq!(
        status(quotas.check(&request("/%61pi/x", "192.0.2.1"))),
        http::StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        status(quotas.check(&request("/api%2fx", "192.0.2.2"))),
        http::StatusCode::BAD_REQUEST
    );
    assert!(
        quotas.check(&request("/api/x", "192.0.2.2")).is_ok(),
        "ambiguous path did not charge either rule"
    );
}

#[test]
fn config_checks_do_not_open_fleet_stores_and_resource_collisions_are_rejected()
{
    let resource = QuotaResource { fleet: "example".parse().unwrap(), kind: 0 };
    let mut config = config();
    config.rate_limit.as_mut().unwrap().quota.store =
        QuotaBacking::FleetQuota {
            resource,
            owners: NonZeroUsize::new(4).unwrap(),
            recovery_attempts: 0,
        };
    let config = Arc::new(config);
    let mut registry = QuotaRegistrations::default();
    registry.seal("runtime", &table(config.clone())).unwrap();
    assert!(
        registry
            .prepare(true, &"example.test".parse().unwrap(), &config, None)
            .unwrap()
            .is_none()
    );
    let two = VhostTable::from_host_map([
        ("example.test".into(), config.clone()),
        ("other.test".into(), config.clone()),
    ])
    .unwrap();
    assert!(QuotaRegistrations::default().seal("runtime", &two).is_err());
    assert!(
        QuotaRegistrations::default().seal("example", &table(config)).is_err()
    );
}

#[test]
fn rust_registration_and_config_use_the_same_boundary_and_conflicts_fail() {
    let definition = definition();
    let limiter = RateLimiter::new(
        Quota::new(definition.quota).unwrap(),
        IpKey,
        definition.namespace.into_bytes(),
        definition.cost,
    )
    .unwrap();
    let mut registry = QuotaRegistrations::default();
    registry
        .register(
            "example.test".parse().unwrap(),
            PathPrefix::root(),
            limiter.clone(),
        )
        .unwrap();
    assert!(
        registry
            .register(
                "example.test".parse().unwrap(),
                PathPrefix::root(),
                limiter.clone()
            )
            .is_err()
    );
    assert!(registry.seal("runtime", &table(Arc::new(config()))).is_err());
    let host = Arc::new(HostConfig::new("public"));
    registry.seal("runtime", &table(host.clone())).unwrap();
    assert!(
        registry
            .register(
                "other.test".parse().unwrap(),
                PathPrefix::root(),
                limiter
            )
            .is_err()
    );
    let quotas = registry
        .prepare(false, &"example.test".parse().unwrap(), &host, None)
        .unwrap()
        .unwrap();
    assert!(quotas.check(&request("/", "192.0.2.1")).is_ok());
    assert_eq!(
        status(quotas.check(&request("/", "192.0.2.1"))),
        http::StatusCode::TOO_MANY_REQUESTS
    );
}

#[test]
fn unchanged_preparation_keeps_debt_and_changed_limits_require_restart() {
    let registry = QuotaRegistrations::default();
    let host = "example.test".parse().unwrap();
    let mut config = config();
    let first = registry.prepare(false, &host, &config, None).unwrap();
    let quotas = first.as_ref().unwrap();
    assert!(quotas.check(&request("/", "192.0.2.1")).is_ok());
    let next =
        registry.prepare(false, &host, &config, Some(&first)).unwrap().unwrap();
    assert!(Arc::ptr_eq(quotas, &next));
    assert_eq!(
        status(next.check(&request("/", "192.0.2.1"))),
        http::StatusCode::TOO_MANY_REQUESTS
    );
    config.rate_limit.as_mut().unwrap().quota.burst =
        NonZeroU64::new(2).unwrap();
    assert!(registry.prepare(false, &host, &config, Some(&first)).is_err());
    config.rate_limit = None;
    assert!(registry.prepare(false, &host, &config, Some(&first)).is_err());
}

#[test]
fn route_normalization_keeps_builtin_limits_outside_extension_resolution() {
    let config: HostConfig = serde_yaml::from_str("root: public\nrate_limit:\n  namespace: test\n  cost: 1\n  quota:\n    units: 1\n    period: 1h\n    burst: 1\n    table: {slots: 8, shards: 1, key_bytes: 64, probes: 8}\n    cas_attempts: 64\n    store: {mode: local_guard}\nroutes:\n  /api:\n    rate_limit:\n      namespace: api\n      cost: 1\n      quota:\n        units: 1\n        period: 1h\n        burst: 1\n        table: {slots: 8, shards: 1, key_bytes: 64, probes: 8}\n        cas_attempts: 64\n        store: {mode: local_guard}\n").unwrap();
    assert!(config.directive("rate_limit").is_none());
    assert!(!config.rate_limit.as_ref().unwrap().headers);
    assert_eq!(declarations(&config).len(), 2);
}

#[test]
fn header_opt_in_belongs_to_the_selected_definition_and_defaults_off() {
    let mut config = config();
    let mut child = HostConfig::new("public");
    let mut explicit = definition();
    explicit.headers = true;
    child.route_rate_limit = Some(explicit);
    config.routes = claviron_http::config::routes::RouteTable::new([(
        serde_yaml::from_str("/api").unwrap(),
        Arc::new(child),
    )]);
    let quotas = QuotaRegistrations::default()
        .prepare(false, &"example.test".parse().unwrap(), &config, None)
        .unwrap()
        .unwrap();
    assert_eq!(quotas.check(&request("/", "192.0.2.1")).unwrap(), None);
    assert_eq!(
        quotas.check(&request("/api/x", "192.0.2.1")).unwrap(),
        Some(Snapshot { limit: 1, remaining: 0 })
    );
    assert_eq!(
        status(quotas.check(&request("/api/x", "192.0.2.1"))),
        http::StatusCode::TOO_MANY_REQUESTS
    );
}
