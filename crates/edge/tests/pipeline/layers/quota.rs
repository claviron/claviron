use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use claviron_http::ServiceResult;
use claviron_http::ext::ConnCtx;
use crate::pipeline::host::HostContext;
use crate::service::RequestArrival;
use http::Request;
use claviron_orbit::quota::{QuotaBacking, QuotaConfig, QuotaTable};

use super::*;
use crate::config::HostConfig;
use crate::config::quota::RateLimitConfig;

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

struct BodyOwner { drops: Arc<AtomicUsize> }
impl Drop for BodyOwner {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
}

fn connection() -> ConnCtx {
    ConnCtx {
        state: Default::default(), listener: "public".parse().unwrap(),
        proto: Some(claviron_http::listener::HttpProtocol::H1),
        remote: "192.0.2.1:1234".parse().unwrap(), local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 8080, real_ip: None, country_code: None, tls: None,
    }
}

#[test]
fn quota_layer_borrows_accepted_identity_and_cancellation_does_not_refund_admission() {
    let quotas = quotas();
    let first = QuotaPolicy::attach(HostPipelineBuilder::new(), &quotas).unwrap().build();
    let second = QuotaPolicy::attach(HostPipelineBuilder::new(), &quotas).unwrap().build();
    drop(quotas);
    let connection = connection();
    let context = Some(HostContext { connection: &connection, arrival: RequestArrival::Established });
    let drops = Arc::new(AtomicUsize::new(0));
    let request = || Request::builder().uri("/public?raw=%2F")
        .header("x-real-ip", "203.0.113.1")
        .body(BodyOwner { drops: drops.clone() }).unwrap();
    let ServiceResult::Unhandled(prepared) = first.prepare_in(request(), QuotaState::default(), context) else {
        panic!("the accepted peer must supply quota identity without extensions");
    };
    assert!(prepared.head().extensions.is_empty());
    assert_eq!(prepared.head().uri, "/public?raw=%2F");
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(prepared);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    let ServiceResult::Handled(reply) = second.prepare_in(request(), QuotaState::default(), context) else {
        panic!("another plan must share the charged budget after cancellation");
    };
    assert_eq!(reply.finish().status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn quota_snapshot_headers_come_from_the_admission_that_produced_the_reply() {
    use futures_util::FutureExt;
    let quotas = quotas();
    let pipeline = QuotaPolicy::attach(HostPipelineBuilder::new(), &quotas).unwrap().build();
    let connection = connection();
    let context = Some(HostContext { connection: &connection, arrival: RequestArrival::Established });
    let ServiceResult::Unhandled(prepared) = pipeline.prepare_in(Request::new(claviron_http::empty_body()), QuotaState::default(), context) else {
        panic!("first admission must succeed");
    };
    let response = prepared.dispatch(|request| std::future::ready(http::Response::new(request.into_body())))
        .now_or_never().expect("ready fixture must not require a runtime");
    assert_eq!(response.headers()["x-ratelimit-limit"], "1");
    assert_eq!(response.headers()["x-ratelimit-remaining"], "0");
    assert_eq!(response.headers()[http::header::CACHE_CONTROL], "private, no-store");
}
