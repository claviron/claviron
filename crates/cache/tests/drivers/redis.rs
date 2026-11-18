use super::{RedisConfig, redis_prefix_pattern};

#[test]
fn clear_pattern_escapes_redis_glob_characters() {
    assert_eq!(redis_prefix_pattern(r"cache[*]?:\\"), r"cache\[\*\]\?:\\\\*");
}

#[test]
fn redis_config_uses_the_document_root() {
    let config: RedisConfig = serde_yaml::from_str(
        r#"
connections:
  default:
    url: redis://127.0.0.1:6379/0
    min_delay: 2s
    max_delay: 5m
"#,
    )
    .expect("root Redis config");

    assert_eq!(config.connections.len(), 1);
}

#[test]
fn legacy_combined_redis_wrapper_is_rejected() {
    let result = serde_yaml::from_str::<RedisConfig>(
        r#"
redis:
  connections: {}
"#,
    );
    let Err(error) = result else {
        panic!("combined Redis wrapper must not be accepted");
    };

    assert!(error.to_string().contains("unknown field `redis`"));
}

#[test]
fn supplied_connections_reach_the_cache_without_a_file() {
    use std::sync::Arc;

    use claviron_runtime::AppState;
    use claviron_runtime::registry::Component;

    use super::{RedisCacheDriver, register, register_with_config};
    use crate::CacheDriver;
    use crate::config::CacheStoreConfig;

    let config: RedisConfig = serde_yaml::from_str(
        r#"
connections:
  default:
    url: redis://127.0.0.1:6379/0
    min_delay: 2s
    max_delay: 5m
"#,
    )
    .expect("supplied Redis config");
    let state = AppState::standalone("redis-supplied-test", "config");
    register_with_config(&state, config).expect("supply Redis connections");

    // The cache's own registration takes the supplied provider, not redis.yaml.
    let provider = register(&state).expect("resolve Redis provider");
    assert!(provider.has_connection("default"));
    assert!(provider.as_configurable().is_none());
    assert!(provider.as_reloadable().is_none());

    let store: CacheStoreConfig = serde_yaml::from_str(
        "driver: redis\nconnection: default\nprefix: \"app:\"\n",
    )
    .expect("store entry");
    Arc::new(RedisCacheDriver::new(provider))
        .create_store("redis", &store)
        .expect("store over the supplied connection");

    assert!(register_with_config(&state, RedisConfig::default()).is_err());
}
