use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use claviron_http::ServiceResult;
use claviron_http::ext::RealIpExt;
use claviron_orbit::quota::{QuotaBacking, QuotaConfig, QuotaTable};

use super::*;
use crate::config::HostConfig;
use crate::config::quota::RateLimitConfig;
use crate::pipeline::host::QuotaRegistrations;

fn quotas() -> Arc<HostQuotas> {
    let mut host = HostConfig::new("public");
    host.rate_limit = Some(RateLimitConfig {
        namespace: "test".into(),
        cost: NonZeroU64::new(1).unwrap(),
        headers: true,
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
    });
    QuotaRegistrations::default()
        .prepare(false, &"example.test".parse().unwrap(), &host, None)
        .unwrap()
        .unwrap()
}

struct BodyOwner {
    drops: Arc<AtomicUsize>,
}

impl Drop for BodyOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn quota_layer_shares_debt_and_cancellation_does_not_refund_admission() {
    let quotas = quotas();
    let mut builder = HostPipelineBuilder::<QuotaState, HostResponse>::new();
    builder.layer(QuotaPolicy::new(&quotas)).unwrap();
    let pipeline = builder.build();
    assert_eq!(pipeline.before.len(), 1);
    let drops = Arc::new(AtomicUsize::new(0));
    let request = || {
        let mut request = Request::builder()
            .uri("/public?raw=%2F")
            .body(BodyOwner { drops: drops.clone() })
            .unwrap();
        request.extensions_mut()
            .insert(RealIpExt("192.0.2.1".parse().unwrap()));
        request
    };
    let mut head = request().into_parts().0;
    // The custom layer and native admission share the existing authority.
    assert_eq!(quotas.check(&mut head).unwrap(), Some(Snapshot { limit: 1, remaining: 0 }));
    let ServiceResult::Handled(reply) = pipeline.prepare_with_state(
        request(), QuotaState::default(),
    ) else {
        panic!("a second plan must not create a fresh budget");
    };
    assert_eq!(reply.response.into_response().status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(drops.load(Ordering::Relaxed), 2);

    let quotas = self::quotas();
    let pipeline = QuotaPolicy::new(&quotas).into_pipeline();
    drop(quotas);
    let ServiceResult::Unhandled(prepared) = pipeline.prepare_with_state(
        request(), QuotaState::default(),
    ) else {
        panic!("the prepared plan must own its store independently");
    };
    assert_eq!(prepared.state.snapshot(), Some(Snapshot { limit: 1, remaining: 0 }));
    assert_eq!(prepared.head().uri, "/public?raw=%2F");
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(prepared);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    let ServiceResult::Handled(reply) = pipeline.prepare_with_state(
        request(), QuotaState::default(),
    ) else {
        panic!("cancelling an admitted request must not refund its charge");
    };
    assert_eq!(reply.response.into_response().status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(drops.load(Ordering::Relaxed), 4);
}
