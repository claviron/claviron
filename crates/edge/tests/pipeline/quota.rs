//! Exercise the public Rust binding API without booting providers or an executor.

use super::*;
use claviron_http::OtherService;
use claviron_http::ext::PathPrefix;
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
    let state = AppState::standalone("quota-composition", ".");
    struct Reply;
    impl crate::pipeline::apps::other::OtherHandler for Reply {
        fn call(
            &self,
            request: http::Request<claviron_http::RouteBody>,
        ) -> claviron_http::RouteFuture {
            drop(request);
            Box::pin(std::future::ready(Ok(http::Response::new(
                claviron_http::empty_body(),
            ))))
        }
    }
    registry
        .register_other("example.test", |_, _, _| Ok(OtherService::new(Reply)));
    let snapshot = registry.build_table(&state, &table, None).unwrap();
    let connection = claviron_http::ext::ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(claviron_http::listener::HttpProtocol::H1),
        remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 8080,
        real_ip: None,
        country_code: None,
        tls: None,
    };
    let context = Some(crate::pipeline::host::HostContext {
        connection: &connection,
        arrival: crate::service::RequestArrival::Established,
    });
    let run = |snapshot: &HostAppSnapshot, key: &str| {
        use futures_util::FutureExt;
        let owner = snapshot.resolve(&key.parse().unwrap()).unwrap();
        owner
            .handle_in(http::Request::new(claviron_http::empty_body()), context)
            .now_or_never()
            .expect("ready fixture")
            .unwrap()
            .status()
    };
    assert_eq!(run(&snapshot, "example.test"), http::StatusCode::OK);
    assert_eq!(
        run(&snapshot, "alias.example.test"),
        http::StatusCode::TOO_MANY_REQUESTS
    );
    let reloaded =
        registry.build_table(&state, &table, Some(&snapshot)).unwrap();
    assert_eq!(
        run(&reloaded, "example.test"),
        http::StatusCode::TOO_MANY_REQUESTS
    );
}
