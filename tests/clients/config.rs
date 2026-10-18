use super::{OptionsError, PoolConfig, PoolOptions};
use std::time::Duration;

fn declaration() -> serde_json::Value {
    serde_json::json!({
        "min_idle": 1,
        "max_idle": 2,
        "max_live": 3,
        "max_concurrency": 4,
        "max_waiting": 0
    })
}

#[test]
fn declared_bounds_do_not_invent_timeouts() {
    let config: PoolConfig = serde_json::from_value(declaration()).unwrap();
    config.validate().unwrap();
    assert_eq!(config.acquire_timeout, None);
    assert_eq!(config.idle_timeout, None);
    assert_eq!(config.max_lifetime, None);
    let options: PoolOptions = config.into();
    assert_eq!(options, PoolOptions::new(1, 2, 3, 4, 0));
}

#[test]
fn owned_and_borrowed_config_preserve_every_option() {
    let mut value = declaration();
    value["acquire_timeout"] = "750ms".into();
    value["idle_timeout"] = "2s".into();
    value["max_lifetime"] = "3m".into();
    let config: PoolConfig = serde_json::from_value(value).unwrap();
    config.validate().unwrap();
    let expected = PoolOptions::new(1, 2, 3, 4, 0)
        .acquire_timeout(Some(Duration::from_millis(750)))
        .idle_timeout(Some(Duration::from_secs(2)))
        .max_lifetime(Some(Duration::from_secs(180)));
    let borrowed: PoolOptions = (&config).into();
    let owned: PoolOptions = config.into();
    assert_eq!(borrowed, expected);
    assert_eq!(owned, expected);
}

#[test]
fn yaml_config_round_trips_with_explicit_null_durations() {
    let config: PoolConfig = serde_yaml::from_str(
        "min_idle: 0\nmax_idle: 2\nmax_live: 3\nmax_concurrency: 4\nmax_waiting: 0\nacquire_timeout: null\nidle_timeout: 2s\nmax_lifetime: null\n",
    )
    .unwrap();
    let encoded = serde_yaml::to_string(&config).unwrap();
    assert_eq!(serde_yaml::from_str::<PoolConfig>(&encoded).unwrap(), config);
}

#[test]
fn zero_live_capacity_and_concurrency_are_rejected() {
    for (field, expected) in [
        ("max_live", OptionsError::ZeroMaxLive),
        ("max_concurrency", OptionsError::ZeroMaxConcurrency),
    ] {
        let mut value = declaration();
        value[field] = 0.into();
        let config: PoolConfig = serde_json::from_value(value).unwrap();
        assert_eq!(config.validate(), Err(expected));
    }
}

#[test]
fn capacities_must_be_ordered() {
    for (min_idle, max_idle, max_live) in [(3, 2, 4), (1, 3, 2)] {
        let mut value = declaration();
        value["min_idle"] = min_idle.into();
        value["max_idle"] = max_idle.into();
        value["max_live"] = max_live.into();
        let config: PoolConfig = serde_json::from_value(value).unwrap();
        assert!(matches!(
            config.validate(),
            Err(OptionsError::CapacityOrder { .. })
        ));
    }
}

#[test]
fn present_durations_must_be_positive() {
    for field in ["acquire_timeout", "idle_timeout", "max_lifetime"] {
        let mut value = declaration();
        value[field] = "0s".into();
        let config: PoolConfig = serde_json::from_value(value).unwrap();
        assert_eq!(config.validate(), Err(OptionsError::ZeroDuration(field)));
    }
}

#[test]
fn unknown_pool_fields_and_missing_capacities_are_rejected() {
    let mut value = declaration();
    value["unexpected_setting"] = true.into();
    assert!(serde_json::from_value::<PoolConfig>(value).is_err());
    let mut value = declaration();
    value.as_object_mut().unwrap().remove("min_idle");
    assert!(serde_json::from_value::<PoolConfig>(value).is_err());
}
