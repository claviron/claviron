mod support {
    pub mod scripted;
}
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use claviron_resolver::{
    Answer, Query, Record, RecordType, ResolveError, ResolveErrorKind,
    Resolver, ResolverConfig,
};
use support::scripted::ScriptedResolver;

#[tokio::test]
async fn custom_resolver_preserves_deadline_and_typed_failure_through_shared_trait()
 {
    let expiry = Instant::now() + Duration::from_secs(3);
    let resolver = Arc::new(ScriptedResolver {
        steps: Mutex::new(
            [
                (
                    "api.internal",
                    RecordType::A,
                    Ok(Answer {
                        records: vec![Record::A("127.0.0.1".parse().unwrap())],
                        valid_until: expiry,
                    }),
                ),
                (
                    "api.internal",
                    RecordType::A,
                    Err(ResolveError::new(
                        ResolveErrorKind::Timeout,
                        "scripted timeout",
                    )),
                ),
            ]
            .into(),
        ),
    });
    let shared: Arc<dyn Resolver> = resolver.clone();
    let answer =
        shared.lookup(Query::new("api.internal", RecordType::A)).await.unwrap();
    assert_eq!(answer.valid_until, expiry);
    assert_eq!(answer.records, [Record::A("127.0.0.1".parse().unwrap())]);
    assert_eq!(
        shared
            .lookup(Query::new("api.internal", RecordType::A))
            .await
            .unwrap_err()
            .kind,
        ResolveErrorKind::Timeout
    );
    assert!(resolver.steps.lock().unwrap().is_empty());
}

#[test]
fn operator_config_validates_before_driver_creation() {
    let valid: ResolverConfig = serde_yaml::from_str("nameservers: ['127.0.0.1:5353']\ntimeout: 250ms\nattempts: 1\ncache_size: 0\nmin_ttl: 0s\nmax_ttl: 30s").unwrap();
    valid.validate().unwrap();
    assert_eq!(valid.timeout, Duration::from_millis(250));
    assert_eq!(valid.cache_size, 0);
    for yaml in [
        "timeout: 0s",
        "attempts: 0",
        "min_ttl: 10s\nmax_ttl: 1s",
        "max_ttl: 0s",
        "nameservers: ['127.0.0.1:0']",
    ] {
        let cfg: ResolverConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            cfg.validate().unwrap_err().kind,
            ResolveErrorKind::Config,
            "{yaml}"
        );
    }
}
