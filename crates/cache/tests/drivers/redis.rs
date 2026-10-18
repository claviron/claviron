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
