use super::*;

#[cfg(feature = "rate-limit")]
use crate::pipeline::extension::{RouteEffect, RouteLayer};
use crate::pipeline::host::HostContext;
#[cfg(feature = "rate-limit")]
use crate::pipeline::host::HostLayerBuilder;
use crate::service::RequestArrival;
use claviron_http::config::{HttpSnapshot, HttpYaml};
use claviron_http::ext::ConnCtx;
#[cfg(feature = "rate-limit")]
use claviron_http::ext::RealIpExt;
use claviron_http::{OtherHandler, OtherService, RouteBody, RouteFuture};
use http::StatusCode;
#[cfg(feature = "rate-limit")]
use std::ops::ControlFlow;

struct Reply(Arc<AtomicUsize>);

impl OtherHandler for Reply {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(std::future::ready(Ok(http::Response::new(
            request.into_body(),
        ))))
    }
}

#[cfg(feature = "rate-limit")]
struct Observe(Arc<AtomicUsize>);

#[cfg(feature = "rate-limit")]
impl RouteLayer for Observe {
    fn prepare(
        &self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()> {
        let calls = self.0.clone();
        layer.before(move |head, _| {
            assert_eq!(
                head.extensions.get::<RealIpExt>().unwrap().0,
                "203.0.113.1".parse::<std::net::IpAddr>().unwrap()
            );
            calls.fetch_add(1, Ordering::Relaxed);
            ControlFlow::Continue(())
        });
        Ok(())
    }
}

fn http() -> HttpSnapshot {
    serde_yaml::from_str::<HttpYaml>(
        r#"
defaults:
  deny: ['^/blocked$']
  listener:
    requests: {max_body_size: off, body_read_timeout: off}
listeners:
  public:
    transport: tcp
    listen: 127.0.0.1:8080
    protocols: h1
    proxy:
      headers: [x-real-ip]
      trusted: [192.0.2.0/24]
  direct:
    transport: tcp
    listen: 127.0.0.1:8081
    protocols: h1
"#,
    )
    .unwrap()
    .resolve()
    .unwrap()
}

fn connection() -> Arc<ConnCtx> {
    Arc::new(ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(claviron_http::listener::HttpProtocol::H1),
        remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 8080,
        real_ip: None,
        country_code: None,
        tls: None,
    })
}

#[test]
fn absent_or_ineffective_defenses_keep_the_direct_router_path() {
    let state = AppState::standalone("empty-defenses", ".");
    let mut http = http();
    http.deny = regex::RegexSet::empty();
    let calls = Arc::new(AtomicUsize::new(0));
    for (listener, headers, trusted) in [
        ("direct", true, true),
        ("public", false, true),
        ("public", true, false),
    ] {
        let listener_config = Arc::make_mut(
            http.listeners.get_mut(&"public".parse().unwrap()).unwrap(),
        );
        let proxy = listener_config.proxy.as_mut().unwrap();
        proxy.headers = if headers {
            vec![http::HeaderName::from_static("x-real-ip")]
        } else {
            Vec::new()
        };
        proxy.trusted = if trusted {
            vec!["192.0.2.0/24".parse().unwrap()]
        } else {
            Vec::new()
        };
        let config: HostConfig = serde_yaml::from_str(&format!(
            "listeners: [{listener}]\nacl: {{policy: allow}}\n"
        ))
        .unwrap();
        let owner =
            HostCompiler { state: &state, http: Some(&http), protocol: None }
                .prepare_target(
                    HostCompileTarget {
                        host: &"example.test".parse().unwrap(),
                        listener: Some(
                            &http.listeners[&listener.parse().unwrap()],
                        ),
                    },
                    Arc::new(config),
                    Some(RustRouter::Other(OtherService::new(Reply(
                        calls.clone(),
                    )))),
                    &HostContributions::default(),
                    #[cfg(feature = "rate-limit")]
                    None,
                    #[cfg(feature = "websocket")]
                    None,
                )
                .unwrap();
        assert_eq!(owner.kind(), "other");
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[cfg(feature = "rate-limit")]
#[test]
fn forwarding_and_acl_precede_quota_which_protects_all_extension_layers() {
    use crate::config::quota::RateLimitConfig;
    use crate::pipeline::layers::QuotaRegistrations;
    use claviron_orbit::quota::{QuotaBacking, QuotaConfig, QuotaTable};
    use futures_util::FutureExt;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::time::Duration;

    let state = AppState::standalone("defense-order", ".");
    let registered_calls = Arc::new(AtomicUsize::new(0));
    let directive_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let registry = Arc::new(HostAppRegistry::default());
    let observed = directive_calls.clone();
    registry
        .register_route_extension(crate::pipeline::extension::directive::<
            bool,
            _,
        >("observe", move |_, _| {
            Ok(RouteEffect::Layer(Arc::new(Observe(observed.clone()))))
        }))
        .unwrap();
    state.registry_ref().insert(registry).unwrap();
    let mut config: HostConfig = serde_yaml::from_str(
        r#"
acl: {policy: deny, allowed: [203.0.113.0/24]}
routes:
  /selected:
    observe: true
"#,
    )
    .unwrap();
    config.rate_limit = Some(RateLimitConfig {
        namespace: "defense-order".into(),
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
    });
    let host = "example.test".parse().unwrap();
    let quotas = QuotaRegistrations::default()
        .prepare(false, &host, &config, None)
        .unwrap()
        .unwrap();
    let mut contributions = HostContributions::default();
    contributions.layers.push(Arc::new(Observe(registered_calls.clone())));
    let http = http();
    let config = Arc::new(config);
    let build = |listener: &str| {
        HostCompiler { state: &state, http: Some(&http), protocol: None }
            .prepare_target(
                HostCompileTarget {
                    host: &host,
                    listener: Some(&http.listeners[&listener.parse().unwrap()]),
                },
                config.clone(),
                Some(RustRouter::Other(OtherService::new(Reply(
                    handler_calls.clone(),
                )))),
                &contributions,
                Some(&quotas),
                #[cfg(feature = "websocket")]
                None,
            )
            .unwrap()
    };
    let owner = build("public");
    let accepted = connection();
    let drops = Arc::new(AtomicUsize::new(0));
    for (listener, peer) in
        [("direct", "192.0.2.1:1234"), ("public", "198.51.100.1:1234")]
    {
        let mut untrusted = accepted.clone();
        Arc::make_mut(&mut untrusted).listener = listener.parse().unwrap();
        Arc::make_mut(&mut untrusted).remote = peer.parse().unwrap();
        let response = build(listener)
            .handle_in(
                Request::builder()
                    .uri("/selected")
                    .header("x-real-ip", "203.0.113.1")
                    .body(claviron_http::empty_body())
                    .unwrap(),
                Some(HostContext {
                    host: None,
                    connection: &untrusted,
                    arrival: RequestArrival::Established,
                }),
            )
            .now_or_never()
            .expect("head-only fixture is ready")
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    for (path, forwarded, status) in [
        ("/blocked", "203.0.113.1", StatusCode::FORBIDDEN),
        ("/selected", "198.51.100.1", StatusCode::FORBIDDEN),
        ("/selected", "203.0.113.1", StatusCode::OK),
        ("/selected", "203.0.113.1", StatusCode::TOO_MANY_REQUESTS),
    ] {
        let response = owner
            .handle_in(
                Request::builder()
                    .uri(path)
                    .header("x-real-ip", forwarded)
                    .body(claviron_http::boxed_body(Unread(drops.clone())))
                    .unwrap(),
                Some(HostContext {
                    host: None,
                    connection: &accepted,
                    arrival: RequestArrival::Established,
                }),
            )
            .now_or_never()
            .expect("head-only fixture is ready")
            .unwrap();
        assert_eq!(response.status(), status, "{path}: {forwarded}");
        drop(response);
    }
    assert_eq!(registered_calls.load(Ordering::Relaxed), 1);
    assert_eq!(directive_calls.load(Ordering::Relaxed), 1);
    assert_eq!(handler_calls.load(Ordering::Relaxed), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 4);
    assert!(accepted.real_ip.is_none());
}

#[test]
fn an_allow_all_child_still_overrides_a_deny_all_parent() {
    use futures_util::FutureExt;
    let state = AppState::standalone("acl-override", ".");
    let config = serde_yaml::from_str(
        "acl: {policy: deny}\nroutes:\n  /public:\n    acl: {policy: allow}\n",
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let owner = HostCompiler { state: &state, http: None, protocol: None }
        .prepare(
            &"example.test".parse().unwrap(),
            Arc::new(config),
            Some(RustRouter::Other(OtherService::new(Reply(calls.clone())))),
            &HostContributions::default(),
            #[cfg(feature = "rate-limit")]
            None,
            #[cfg(feature = "websocket")]
            None,
        )
        .unwrap();
    let accepted = connection();
    for (path, status) in [
        ("/public/item", StatusCode::OK),
        ("/private", StatusCode::FORBIDDEN),
        ("/public%2F..%2Fprivate", StatusCode::BAD_REQUEST),
    ] {
        let response = owner
            .handle_in(
                Request::builder()
                    .uri(path)
                    .body(claviron_http::empty_body())
                    .unwrap(),
                Some(HostContext {
                    host: None,
                    connection: &accepted,
                    arrival: RequestArrival::Established,
                }),
            )
            .now_or_never()
            .expect("head-only fixture is ready")
            .unwrap();
        assert_eq!(response.status(), status, "{path}");
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn cors_absence_keeps_the_direct_router_and_scoped_policies_inherit_at_boot() {
    use futures_util::FutureExt;
    let state = AppState::standalone("cors-scopes", ".");
    let calls = Arc::new(AtomicUsize::new(0));
    let host = "example.test".parse().unwrap();
    for (config, expected_kind) in [
        ("{}", "other"),
        (
            "cors: permissive\nroutes:\n  /private:\n    cors: disabled\n  /private/child: {}\n  /restricted:\n    cors: {policy: allowlist, origins: ['https://ui.example.test']}\n",
            "pipeline",
        ),
    ] {
        let owner = HostCompiler { state: &state, http: None, protocol: None }
            .prepare(
                &host,
                Arc::new(serde_yaml::from_str(config).unwrap()),
                Some(RustRouter::Other(OtherService::new(Reply(
                    calls.clone(),
                )))),
                &HostContributions::default(),
                #[cfg(feature = "rate-limit")]
                None,
                #[cfg(feature = "websocket")]
                None,
            )
            .unwrap();
        assert_eq!(owner.kind(), expected_kind);
        for (path, origin, expected, header) in [
            (
                "/public",
                "https://other.example.test",
                StatusCode::OK,
                Some("*"),
            ),
            (
                "/private/child",
                "https://ui.example.test",
                StatusCode::FORBIDDEN,
                None,
            ),
            (
                "/restricted",
                "https://ui.example.test",
                StatusCode::OK,
                Some("https://ui.example.test"),
            ),
            (
                "/restricted",
                "https://other.example.test",
                StatusCode::FORBIDDEN,
                None,
            ),
        ] {
            let drops = Arc::new(AtomicUsize::new(0));
            let reply = owner
                .handle(
                    Request::builder()
                        .uri(path)
                        .header("origin", origin)
                        .body(claviron_http::boxed_body(Unread(drops.clone())))
                        .unwrap(),
                )
                .now_or_never()
                .expect("head fixture must be ready")
                .unwrap();
            if expected_kind == "other" {
                assert_eq!(reply.status(), StatusCode::OK);
                assert!(
                    !reply
                        .headers()
                        .contains_key("access-control-allow-origin")
                );
            } else {
                assert_eq!(reply.status(), expected, "{path}: {origin}");
                assert_eq!(
                    reply
                        .headers()
                        .get("access-control-allow-origin")
                        .map(|value| value.to_str().unwrap()),
                    header
                );
            }
            drop(reply);
            assert_eq!(drops.load(Ordering::Relaxed), 1);
        }
    }
}

#[cfg(feature = "rate-limit")]
#[test]
fn cors_preflight_spends_quota_and_quota_refusal_keeps_cors_headers() {
    use crate::config::quota::RateLimitConfig;
    use crate::pipeline::layers::QuotaRegistrations;
    use claviron_orbit::quota::{QuotaBacking, QuotaConfig, QuotaTable};
    use futures_util::FutureExt;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::time::Duration;

    let state = AppState::standalone("cors-quota", ".");
    let mut config: HostConfig = serde_yaml::from_str("cors: {policy: allowlist, credentials: true, origins: ['https://ui.example.test']}").unwrap();
    config.rate_limit = Some(RateLimitConfig {
        namespace: "cors-quota".into(),
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
    let host = "example.test".parse().unwrap();
    let quotas = QuotaRegistrations::default()
        .prepare(false, &host, &config, None)
        .unwrap()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let owner = HostCompiler { state: &state, http: None, protocol: None }
        .prepare(
            &host,
            Arc::new(config),
            Some(RustRouter::Other(OtherService::new(Reply(calls.clone())))),
            &HostContributions::default(),
            Some(&quotas),
            #[cfg(feature = "websocket")]
            None,
        )
        .unwrap();
    let accepted = connection();
    let drops = Arc::new(AtomicUsize::new(0));
    for (method, expected) in [
        (http::Method::OPTIONS, StatusCode::NO_CONTENT),
        (http::Method::GET, StatusCode::TOO_MANY_REQUESTS),
    ] {
        let mut request = Request::builder()
            .method(method.clone())
            .header("origin", "https://ui.example.test");
        if method == http::Method::OPTIONS {
            request = request.header("access-control-request-method", "GET");
        }
        let reply = owner
            .handle_in(
                request
                    .body(claviron_http::boxed_body(Unread(drops.clone())))
                    .unwrap(),
                Some(HostContext {
                    host: None,
                    connection: &accepted,
                    arrival: RequestArrival::Established,
                }),
            )
            .now_or_never()
            .expect("head fixture must be ready")
            .unwrap();
        assert_eq!(reply.status(), expected);
        assert_eq!(
            reply.headers()["access-control-allow-origin"],
            "https://ui.example.test"
        );
        assert_eq!(reply.headers()["access-control-allow-credentials"], "true");
        if expected == StatusCode::TOO_MANY_REQUESTS {
            assert!(reply.headers().contains_key("retry-after"));
        } else {
            assert_eq!(reply.headers()["x-ratelimit-remaining"], "0");
        }
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn body_policy_is_narrowed_through_scope_ancestors_at_boot() {
    use claviron_http::policy::HttpPolicy;
    let config: HostConfig = serde_yaml::from_str("requests: {max_body_size: 10, body_read_timeout: 5s}\nroutes:\n  /tight:\n    requests: {max_body_size: 2}\n  /tight/child:\n    requests: {body_read_timeout: 1s}\n  /wide:\n    requests: {max_body_size: 100}\n").unwrap();
    let mut listener = HttpPolicy::unlimited();
    listener.requests.max_body_size = Some(8);
    for (path, size, seconds) in
        [("/", 8, 5), ("/tight/child", 2, 1), ("/wide", 8, 5)]
    {
        let prefix = serde_yaml::from_str(path).unwrap();
        let policy = config.policy_at(&prefix, listener).requests;
        assert_eq!(policy.max_body_size, Some(size));
        assert_eq!(
            policy.body_read_timeout,
            Some(std::time::Duration::from_secs(seconds))
        );
    }
}
