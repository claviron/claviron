use super::*;
use claviron_orbit::quota::{QuotaBacking, QuotaConfig, QuotaTable};
use std::num::NonZeroUsize;
use std::time::Duration;

fn store() -> Quota {
    Quota::new(QuotaConfig {
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
    .unwrap()
}

fn request(ip: &str) -> Request<()> {
    let mut request = Request::new(());
    request.extensions_mut().insert(crate::ext::RealIpExt(ip.parse().unwrap()));
    request
}

#[test]
fn ip_keys_use_trusted_context_and_normalize_mapped_addresses() {
    let ipv4 = request("192.0.2.1");
    let mapped = request("::ffff:192.0.2.1");
    assert_eq!(
        IpKey.key(&RequestHead::new(&ipv4)),
        IpKey.key(&RequestHead::new(&mapped))
    );
    let mut missing = Request::new(());
    missing
        .headers_mut()
        .insert("x-forwarded-for", "192.0.2.1".parse().unwrap());
    assert_eq!(IpKey.key(&RequestHead::new(&missing)), Err(KeyError::Missing));
    assert_ne!(
        IpKey.key(&RequestHead::new(&ipv4)),
        IpKey.key(&RequestHead::new(&request("2001:db8::1")))
    );
}

struct Custom(u8);
impl KeyProvider for Custom {
    type Key = [u8; 1];
    fn max_key_bytes(&self) -> usize {
        1
    }
    fn key(
        &self,
        _: &RequestHead<'_>,
    ) -> Result<Self::Key, KeyError> {
        Ok([self.0])
    }
}

#[test]
fn each_limiter_retains_its_own_key_provider_instance() {
    let store = store();
    let cost = NonZeroU64::new(1).unwrap();
    let first =
        RateLimiter::new(store.clone(), Custom(1), b"test".as_slice(), cost)
            .unwrap();
    let second =
        RateLimiter::new(store, Custom(2), b"test".as_slice(), cost).unwrap();
    let request = Request::new(());
    let head = RequestHead::new(&request);
    assert_eq!(first.check(&head).unwrap(), Decision::Allowed);
    assert_eq!(second.check(&head).unwrap(), Decision::Allowed);
    assert!(matches!(first.check(&head).unwrap(), Decision::Limited { .. }));
    assert!(matches!(second.check(&head).unwrap(), Decision::Limited { .. }));
}

#[test]
fn missing_identity_does_not_charge_or_fall_back_to_a_header() {
    let limiter = RateLimiter::new(
        store(),
        IpKey,
        b"test".as_slice(),
        NonZeroU64::new(1).unwrap(),
    )
    .unwrap();
    let request = Request::new(());
    assert!(matches!(
        limiter.check(&RequestHead::new(&request)),
        Err(Error::Key(KeyError::Missing))
    ));
    assert_eq!(
        limiter.check(&RequestHead::new(&self::request("192.0.2.1"))).unwrap(),
        Decision::Allowed
    );
}

#[test]
fn key_bound_and_cost_are_validated_before_serving() {
    assert!(matches!(
        RateLimiter::new(
            store(),
            IpKey,
            vec![0; 64],
            NonZeroU64::new(1).unwrap()
        ),
        Err(Error::InvalidDefinition)
    ));
    assert!(matches!(
        RateLimiter::new(
            store(),
            IpKey,
            b"test".as_slice(),
            NonZeroU64::new(2).unwrap()
        ),
        Err(Error::InvalidDefinition)
    ));
}

#[test]
fn overload_is_not_quota_exhaustion_and_retry_after_rounds_up() {
    use claviron_orbit::quota::Error as StoreError;
    for error in [StoreError::Busy, StoreError::CapacityFull] {
        let reply = Rejection::from_result(Err(Error::Store(error)))
            .unwrap_err()
            .into_response();
        assert_eq!(reply.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(!reply.headers().contains_key(http::header::RETRY_AFTER));
    }
    let reply = Rejection::from_result(Ok(Decision::Limited {
        retry_after: Duration::from_nanos(1),
    }))
    .unwrap_err()
    .into_response();
    assert_eq!(reply.status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(reply.headers()[http::header::RETRY_AFTER], "1");
    assert!(Rejection::from_result(Ok(Decision::Allowed)).is_ok());
    assert_eq!(
        Rejection::from_result(Err(Error::Store(StoreError::ClockUnavailable)))
            .unwrap_err()
            .status(),
        http::StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn headers_are_opt_in_per_limiter_and_refusal_keeps_only_retry_after() {
    let head_request = request("192.0.2.1");
    let head = RequestHead::new(&head_request);
    let cost = NonZeroU64::new(1).unwrap();
    let plain =
        RateLimiter::new(store(), IpKey, b"test".as_slice(), cost).unwrap();
    let reported = RateLimiter::new(store(), IpKey, b"test".as_slice(), cost)
        .unwrap()
        .with_headers(true);
    let mut response = http::Response::new(());
    apply_headers(plain.admit(&head).unwrap(), &mut response);
    assert!(!response.headers().contains_key("x-ratelimit-limit"));
    assert!(!response.headers().contains_key(http::header::CACHE_CONTROL));
    let snapshot = reported.admit(&head).unwrap();
    apply_headers(snapshot, &mut response);
    assert_eq!(response.headers()["x-ratelimit-limit"], "1");
    assert_eq!(response.headers()["x-ratelimit-remaining"], "0");
    assert_eq!(
        response.headers()[http::header::CACHE_CONTROL],
        "private, no-store"
    );
    // Header creation is not another admission attempt.
    let mut later = http::Response::new(());
    apply_headers(snapshot, &mut later);
    assert_eq!(later.headers()["x-ratelimit-remaining"], "0");
    let denied = reported.admit(&head).unwrap_err().into_response();
    assert_eq!(denied.status(), http::StatusCode::TOO_MANY_REQUESTS);
    assert!(denied.headers().contains_key(http::header::RETRY_AFTER));
    assert!(!denied.headers().contains_key("x-ratelimit-limit"));
    assert!(!denied.headers().contains_key("x-ratelimit-remaining"));
}

#[test]
fn headers_describe_weighted_quota_and_only_admitted_successful_responses() {
    let mut config = store().config().clone();
    config.burst = NonZeroU64::new(40).unwrap();
    config.units = NonZeroU64::new(20).unwrap();
    config.period = Duration::from_secs(1);
    let limiter = RateLimiter::new(
        Quota::new(config).unwrap(),
        IpKey,
        b"test".as_slice(),
        NonZeroU64::new(2).unwrap(),
    )
    .unwrap()
    .with_headers(true);
    let snapshot =
        limiter.admit(&RequestHead::new(&request("192.0.2.1"))).unwrap();
    assert_eq!(snapshot, Some(Snapshot { limit: 40, remaining: 38 }));
    for status in [200, 204, 101, 400, 429, 503] {
        let mut reply = http::Response::builder()
            .status(status)
            .header(http::header::CACHE_CONTROL, "public, max-age=3600")
            .body(())
            .unwrap();
        apply_headers(snapshot, &mut reply);
        let allowed = status < 300;
        assert_eq!(
            reply.headers().contains_key("x-ratelimit-remaining"),
            allowed
        );
        if allowed {
            assert_eq!(reply.headers()["x-ratelimit-remaining"], "38");
            assert_eq!(
                reply.headers()[http::header::CACHE_CONTROL],
                "private, no-store"
            );
        } else {
            assert_eq!(
                reply.headers()[http::header::CACHE_CONTROL],
                "public, max-age=3600"
            );
        }
    }
    // Changing reporting on a clone must not create/reset the store budget.
    let plain_clone = limiter.clone().with_headers(false);
    assert!(
        plain_clone
            .admit(&RequestHead::new(&request("192.0.2.1")))
            .unwrap()
            .is_none()
    );
}
