// Two tests here build a router; without `axum` they are gated out and take
// their imports with them.
#![cfg_attr(not(feature = "axum"), allow(unused_imports, dead_code))]

use bytes::Bytes;
use claviron_http::config::{
    HttpPolicy, RequestPolicy, ResponsePolicy, RouterConfig,
};
use claviron_http::ext::{
    ClientAuthStatus, HostKey, HostName, RealIpExt, TlsInfo,
};
use claviron_http::{RouteFuture, boxed_body};
#[cfg(feature = "http3")]
use http::Method;
use http::{Response, header};
use http_body_util::{BodyExt, Full};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::pipeline::bypass::{BypassHandler, BypassService};
use crate::service::resolve_sni_from_table;

fn service(
    protocol: HttpProtocol,
    config: &str,
    app: AppKind,
) -> HostService {
    service_with_limits(protocol, config, app, RequestPolicy::unlimited())
}

pub(super) fn service_with_limits(
    protocol: HttpProtocol,
    config: &str,
    app: impl Into<crate::pipeline::HostApp>,
    listener: RequestPolicy,
) -> HostService {
    let table =
        serde_yaml::from_str(&format!("app.test: {{root: public, {config}}}"))
            .unwrap();
    let host = resolve_sni_from_table(&table, Some("app.test")).unwrap();
    let access = Arc::new(VhostResolver::default());
    access.upsert_host(
        host.vhost_key.0.as_ref().clone(),
        host.vhost_config.clone(),
    );
    let connection = Arc::new(ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(protocol),
        remote: "10.0.0.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 443,
        real_ip: None,
        country_code: None,
        tls: Some(TlsInfo {
            protocol_version: None,
            cipher_suite: None,
            client_auth: ClientAuthStatus::NotProvided,
            client_cert: None,
        }),
    });
    let router: RouterConfig =
        serde_yaml::from_str("deny: ['^/blocked']").unwrap();
    HostService::new(
        app,
        connection,
        host,
        HostIngress {
            proxy: Some(ProxyConfig {
                trusted: vec!["10.0.0.0/8".parse().unwrap()],
                headers: vec![header::HeaderName::from_static("x-real-ip")],
            }),
            deny: Arc::new(DenyService::from_set(router.deny_set())),
        },
        access,
        HttpPolicy {
            requests: listener,
            responses: ResponsePolicy::unlimited(),
        },
    )
}

fn noop(
    protocol: HttpProtocol,
    config: &str,
) -> HostService {
    service(protocol, config, AppKind::Bypass(BypassService::Noop))
}

pub(super) fn request(path: &str) -> Request<()> {
    Request::builder()
        .uri(path)
        .header(header::HOST, "app.test")
        .body(())
        .unwrap()
}

#[cfg(feature = "rate-limit")]
#[test]
fn quota_admission_is_shared_by_native_protocols_and_precedes_body_dispatch() {
    use crate::HostConfig;
    use crate::service::quota::QuotaRegistrations;
    use claviron_http::rate_limit::{IpKey, RateLimiter};
    use claviron_orbit::quota::{Quota, QuotaBacking, QuotaConfig, QuotaTable};
    use std::num::{NonZeroU64, NonZeroUsize};

    let store = Quota::new(QuotaConfig {
        units: NonZeroU64::new(1).unwrap(),
        period: std::time::Duration::from_secs(3600),
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
    let host = HostConfig::new("public");
    let mut registrations = QuotaRegistrations::default();
    registrations
        .register(
            "app.test".parse().unwrap(),
            claviron_http::ext::PathPrefix::root(),
            limiter,
        )
        .unwrap();
    let quotas = registrations
        .prepare(false, &"app.test".parse().unwrap(), &host, None)
        .unwrap();
    let mut app: HostApp = AppKind::Bypass(BypassService::Noop).into();
    app.quotas = quotas;
    let protocols = [HttpProtocol::H1, HttpProtocol::H2];
    for (index, protocol) in protocols.into_iter().enumerate() {
        let service = service_with_limits(
            protocol,
            "",
            app.clone(),
            RequestPolicy::unlimited(),
        );
        let mut request = request("/");
        // Native admission replaces an arbitrary preexisting extension using
        // its accepted listener's trusted proxy policy.
        request
            .extensions_mut()
            .insert(RealIpExt("192.0.2.99".parse().unwrap()));
        request.headers_mut().insert("x-real-ip", "192.0.2.1".parse().unwrap());
        let reply = service.prepare(request, RequestArrival::Established);
        if index == 0 {
            assert!(reply.is_ok());
        } else {
            assert_eq!(
                reply.err().unwrap().into_response().status(),
                StatusCode::TOO_MANY_REQUESTS
            );
        }
    }
    #[cfg(feature = "http3")]
    {
        let service = service_with_limits(
            HttpProtocol::H3,
            "",
            app,
            RequestPolicy::unlimited(),
        );
        let mut request = request("/");
        request.headers_mut().insert("x-real-ip", "192.0.2.1".parse().unwrap());
        assert_eq!(
            service
                .prepare(request, RequestArrival::Established)
                .err()
                .unwrap()
                .into_response()
                .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }
}

/// A host narrows the listener's body ceiling and can never widen it.
///
/// Resolved once, where the host becomes known, so the four places that
/// enforce it — the declared-length refusal and the running count, on two
/// transports each — read one number instead of choosing between two. A host
/// that could widen would make the listener's value a suggestion; the whole
/// point of it is that it is not.
#[test]
fn a_host_narrows_the_listener_body_ceiling_and_cannot_widen_it() {
    let listener = RequestPolicy {
        max_body_size: Some(64 * 1024 * 1024),
        ..RequestPolicy::unlimited()
    };
    let resolved = |config: &str| {
        service_with_limits(
            HttpProtocol::H1,
            config,
            AppKind::Bypass(BypassService::Noop),
            listener,
        )
        .prepare(request("/"), RequestArrival::Established)
        .map(|prepared| prepared.max_body_size())
        .unwrap_or_else(|_| panic!("fixture must prepare: {config}"))
    };

    assert_eq!(
        resolved(""),
        listener.max_body_size,
        "absent inherits the listener"
    );
    assert_eq!(
        resolved("requests: {max_body_size: 10485760}"),
        Some(10 * 1024 * 1024),
        "narrows"
    );
    assert_eq!(
        resolved("requests: {max_body_size: '10 MiB'}"),
        Some(10 * 1024 * 1024),
        "either spelling"
    );
    assert_eq!(
        resolved("requests: {max_body_size: 134217728}"),
        listener.max_body_size,
        "a host cannot widen what the listener allows"
    );
}

#[test]
fn head_guards_are_shared_and_do_not_require_a_pollable_body() {
    // This body deliberately does not implement http_body::Body.
    struct NativeBody;
    for protocol in [HttpProtocol::H1, HttpProtocol::H2, HttpProtocol::H3] {
        let service = noop(protocol, "transport: {protocols: [h1, h2, h3]}");
        let mismatched = Request::builder()
            .uri("https://other.test/blocked")
            .header(header::HOST, "other.test")
            .body(NativeBody)
            .unwrap();
        assert!(matches!(
            service.prepare(mismatched, RequestArrival::Established),
            Err(HostResponse::Rejected(RequestRejection::Misdirected))
        ));
        assert!(matches!(
            service.prepare(
                request("/blocked").map(|()| NativeBody),
                RequestArrival::Established
            ),
            Err(HostResponse::Rejected(RequestRejection::PathDenied))
        ));
        let denied = noop(protocol, "listeners: []");
        assert!(matches!(
            denied.prepare(request("/"), RequestArrival::Established),
            Err(HostResponse::Rejected(RequestRejection::Misdirected))
        ));
        let denied = noop(protocol, "transport: {protocols: []}");
        assert!(matches!(
            denied.prepare(request("/"), RequestArrival::Established),
            Err(HostResponse::Rejected(RequestRejection::Misdirected))
        ));
        assert!(
            service
                .prepare(
                    request("/ok").map(|()| NativeBody),
                    RequestArrival::Established
                )
                .is_ok()
        );
    }
}

#[test]
fn checked_deny_requests_keep_host_guards_and_cannot_bypass_another_matcher() {
    for cleartext in [false, true] {
        #[cfg(not(feature = "tcp"))]
        if cleartext {
            continue;
        }
        let service =
            noop(HttpProtocol::H1, "acl: {'/private': {policy: deny}}");
        let prepare = |checked| {
            #[cfg(feature = "tcp")]
            if cleartext {
                return service.clone().prepare_cleartext(
                    CleartextRequest::new(checked).unwrap(),
                );
            }
            service.prepare_checked(checked, RequestArrival::Established)
        };
        let matcher = service.policy.ingress.deny.clone();
        let empty = DenyService::empty();
        let checked = empty.check_request(request("/blocked")).ok().unwrap();
        assert!(matches!(
            prepare(checked),
            Err(HostResponse::Rejected(RequestRejection::PathDenied))
        ));
        let checked = matcher.check_request(request("/private")).ok().unwrap();
        assert!(matches!(
            prepare(checked),
            Err(HostResponse::Rejected(RequestRejection::AccessDenied))
        ));
        let mismatched = Request::builder()
            .uri("http://other.test/ok")
            .header(header::HOST, "other.test")
            .body(())
            .unwrap();
        let checked = matcher.check_request(mismatched).ok().unwrap();
        assert!(matches!(
            prepare(checked),
            Err(HostResponse::Rejected(RequestRejection::Misdirected))
        ));
        let checked =
            matcher.check_request(request("/ok?raw=%2F")).ok().unwrap();
        let prepared = prepare(checked).unwrap();
        assert_eq!(prepared.request().uri(), "/ok?raw=%2F");
        assert!(
            prepared.request().extensions().get::<Arc<ConnCtx>>().is_some()
        );
    }
}

#[cfg(feature = "tcp")]
#[test]
fn cleartext_authority_handoff_keeps_transport_and_upgrade_guards() {
    for (config, upgrade, expected) in [
        ("listeners: []", false, RequestRejection::Misdirected),
        ("transport: {protocols: [h2]}", false, RequestRejection::Misdirected),
        ("", true, RequestRejection::UpgradeForbidden),
    ] {
        let service = noop(HttpProtocol::H1, config);
        let mut request = request("/socket");
        if upgrade {
            request
                .headers_mut()
                .insert(header::UPGRADE, "websocket".parse().unwrap());
        }
        let deny = service.policy.ingress.deny.clone();
        let checked = deny.check_request(request).ok().unwrap();
        let validated = CleartextRequest::new(checked).unwrap();
        assert!(matches!(
            service.prepare_cleartext(validated),
            Err(HostResponse::Rejected(reason)) if reason == expected
        ));
    }
}

#[cfg(feature = "tcp")]
#[test]
fn cleartext_authority_handoff_keeps_live_acl_redirect_and_removal() {
    let service = noop(HttpProtocol::H1, "");
    let key = service.policy.host.vhost_key.0.as_ref().clone();
    let prepare = || {
        let checked = service
            .policy
            .ingress
            .deny
            .check_request(request("/original?q=%2F"))
            .ok()
            .unwrap();
        service
            .clone()
            .prepare_cleartext(CleartextRequest::new(checked).unwrap())
    };
    assert!(prepare().is_ok());
    for (config, expected) in [
        ("acl: {'/': {policy: deny}}", StatusCode::FORBIDDEN),
        (
            "redirect: {path: /original, to: /elsewhere}",
            StatusCode::TEMPORARY_REDIRECT,
        ),
    ] {
        service.policy.access.upsert_host(
            key.clone(),
            Arc::new(
                serde_yaml::from_str(&format!("root: public\n{config}"))
                    .unwrap(),
            ),
        );
        assert_eq!(prepare().err().unwrap().into_response().status(), expected);
    }
    service.policy.access.delete_host(&key);
    assert!(matches!(
        prepare(),
        Err(HostResponse::Rejected(RequestRejection::HostRemoved))
    ));
}

#[test]
fn missing_protocol_is_not_inferred_from_request_version_or_uri() {
    let mut service = noop(HttpProtocol::H1, "");
    Arc::get_mut(&mut Arc::get_mut(&mut service.policy).unwrap().connection)
        .unwrap()
        .proto = None;
    assert!(matches!(
        service.prepare(request("/"), RequestArrival::Established),
        Err(HostResponse::Rejected(RequestRejection::MissingProtocol))
    ));
}

#[test]
fn live_host_replies_precede_body_adaptation_and_native_dispatch() {
    struct NativeBody;
    for protocol in [HttpProtocol::H1, HttpProtocol::H2, HttpProtocol::H3] {
        let service = noop(protocol, "");
        let resolver = &service.policy.access;
        let key = service.policy.host.vhost_key.0.as_ref().clone();
        let captured = service.policy.host.vhost_config.clone();
        let admitted = service
            .prepare(request("/old"), RequestArrival::Established)
            .unwrap();
        for (config, status, location) in [
            ("acl: {'/': {policy: deny}}", StatusCode::FORBIDDEN, None),
            (
                "redirect: {path: /old, code: 308, to: 'https://new.test$uri?$args'}",
                StatusCode::PERMANENT_REDIRECT,
                Some("https://new.test/old?raw=%2F"),
            ),
        ] {
            resolver.upsert_host(
                key.clone(),
                Arc::new(
                    serde_yaml::from_str(&format!("root: other\n{config}"))
                        .unwrap(),
                ),
            );
            let result = service.prepare(
                request("/old?raw=%2F").map(|()| NativeBody),
                RequestArrival::Established,
            );
            let Err(reply) = result else {
                panic!("host reply must precede any body/dispatch API");
            };
            let response = reply.into_response();
            assert_eq!(response.status(), status);
            assert_eq!(
                response
                    .headers()
                    .get(header::LOCATION)
                    .map(|v| v.to_str().unwrap()),
                location
            );
        }
        assert!(Arc::ptr_eq(&admitted.auth_context().host_config, &captured));
        resolver.upsert_host(key, captured);
        assert!(
            service
                .prepare(request("/old"), RequestArrival::Established)
                .is_ok()
        );
    }
}

#[test]
fn upgrades_require_h1_permission_and_do_not_rewrite_the_request() {
    for (config, allowed) in
        [("", false), ("transport: {h1: {upgrade: true}}", true)]
    {
        let service = noop(HttpProtocol::H1, config);
        let req = Request::builder()
            .uri("/socket?raw=%2F")
            .header(header::HOST, "app.test:443")
            .header(header::UPGRADE, "websocket")
            .body(())
            .unwrap();
        let result = service.prepare(req, RequestArrival::Established);
        assert_eq!(result.is_ok(), allowed);
        if let Ok(prepared) = result {
            assert_eq!(prepared.request().uri(), "/socket?raw=%2F");
            assert_eq!(
                prepared.request().headers()[header::HOST],
                "app.test:443"
            );
        }
    }
}

#[test]
fn native_auth_and_http_share_fresh_request_identity() {
    for protocol in [HttpProtocol::H1, HttpProtocol::H2, HttpProtocol::H3] {
        let service = noop(protocol, "");
        for ip in ["203.0.113.1", "203.0.113.2"] {
            let mut req = request("/socket?raw=%2F");
            req.headers_mut().insert("x-real-ip", ip.parse().unwrap());
            req.headers_mut()
                .insert("x-forwarded-proto", "http".parse().unwrap());
            let prepared =
                service.prepare(req, RequestArrival::Established).unwrap();
            let auth = prepared.auth_context();
            let extensions = prepared.request().extensions();
            assert!(Arc::ptr_eq(
                &auth.conn,
                extensions.get::<Arc<ConnCtx>>().unwrap()
            ));
            assert_eq!(auth.conn.real_ip, Some(ip.parse().unwrap()));
            assert_eq!(
                extensions.get::<RealIpExt>().unwrap().0,
                ip.parse::<std::net::IpAddr>().unwrap()
            );
            assert_eq!(extensions.get::<HttpProtocol>(), Some(&protocol));
            assert_eq!(
                extensions.get::<http::uri::Scheme>(),
                Some(&http::uri::Scheme::HTTPS)
            );
            assert_eq!(
                extensions.get::<claviron_http::ListenerId>(),
                Some(&auth.conn.listener)
            );
            assert_eq!(
                extensions.get::<HostKey>().unwrap().0.to_string(),
                "app.test"
            );
            assert_eq!(
                extensions.get::<HostName>().unwrap().0.to_string(),
                "app.test"
            );
            assert_eq!(auth.path.as_ref(), "/socket");
            assert_eq!(prepared.request().uri(), "/socket?raw=%2F");
            assert_eq!(
                prepared.request().headers()["x-forwarded-proto"],
                "http"
            );
        }
        assert!(service.policy.connection.real_ip.is_none());
    }
}

#[cfg(feature = "http3")]
#[test]
fn early_data_is_transport_owned_and_authority_is_checked_first() {
    for protocol in [HttpProtocol::H1, HttpProtocol::H2] {
        let service = noop(protocol, "transport: {h3: {early_data: allow}}");
        assert!(matches!(
            service.prepare(request("/"), RequestArrival::EarlyData),
            Err(HostResponse::Rejected(RequestRejection::TooEarly))
        ));
    }
    for policy in ["deny", "idempotent", "allow"] {
        let service = noop(
            HttpProtocol::H3,
            &format!("transport: {{h3: {{early_data: {policy}}}}}"),
        );
        for method in [
            Method::GET,
            Method::HEAD,
            Method::OPTIONS,
            Method::PUT,
            Method::POST,
        ] {
            let safe =
                matches!(method, Method::GET | Method::HEAD | Method::OPTIONS);
            let expected =
                policy == "allow" || (policy == "idempotent" && safe);
            let mut req = request("/");
            *req.method_mut() = method;
            assert!(
                service
                    .prepare(req.clone(), RequestArrival::Established)
                    .is_ok()
            );
            assert_eq!(
                service.prepare(req, RequestArrival::EarlyData).is_ok(),
                expected
            );
        }
        let req = Request::builder()
            .uri("https://other.test/")
            .header(header::HOST, "other.test")
            .method(Method::POST)
            .body(())
            .unwrap();
        assert!(matches!(
            service.prepare(req, RequestArrival::EarlyData),
            Err(HostResponse::Rejected(RequestRejection::Misdirected))
        ));
    }
}

#[cfg(not(feature = "http3"))]
#[test]
fn early_data_cannot_bypass_missing_host_capability() {
    assert!(matches!(
        noop(HttpProtocol::H3, "")
            .prepare(request("/"), RequestArrival::EarlyData),
        Err(HostResponse::Rejected(RequestRejection::TooEarly))
    ));
}

struct Echo;
impl BypassHandler for Echo {
    fn call(
        &self,
        req: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(async move {
            assert!(req.extensions().get::<Arc<ConnCtx>>().is_some());
            Ok(Response::new(req.into_body()))
        })
    }
}

#[cfg(feature = "rate-limit")]
#[tokio::test]
async fn quota_response_headers_are_opt_in_and_survive_native_body_mapping() {
    use crate::HostConfig;
    use crate::service::quota::QuotaRegistrations;
    let protocols = [
        HttpProtocol::H1,
        HttpProtocol::H2,
        #[cfg(feature = "http3")]
        HttpProtocol::H3,
    ];
    for protocol in protocols {
        for headers in [false, true] {
            let config: HostConfig = serde_yaml::from_str(&format!(
                "root: public\nrate_limit:\n  namespace: test\n  cost: 1\n  headers: {headers}\n  quota:\n    units: 1\n    period: 1h\n    burst: 2\n    table: {{slots: 8, shards: 1, key_bytes: 64, probes: 8}}\n    cas_attempts: 64\n    store: {{mode: local_guard}}\n"
            )).unwrap();
            let mut app: HostApp =
                AppKind::Bypass(BypassService::Handler(Arc::new(Echo))).into();
            app.quotas = QuotaRegistrations::default()
                .prepare(false, &"app.test".parse().unwrap(), &config, None)
                .unwrap();
            let service = service_with_limits(
                protocol,
                "",
                app,
                RequestPolicy::unlimited(),
            );
            let prepare = || {
                service
                    .prepare(request("/"), RequestArrival::Established)
                    .unwrap_or_else(|_| panic!("quota should admit"))
            };
            let first = prepare()
                .map_body(|()| claviron_http::empty_body())
                .dispatch()
                .await;
            assert_eq!(first.status(), StatusCode::OK);
            assert_eq!(
                first.headers().contains_key("x-ratelimit-limit"),
                headers
            );
            assert_eq!(
                first.headers().contains_key("x-ratelimit-remaining"),
                headers
            );
            if headers {
                assert_eq!(first.headers()["x-ratelimit-limit"], "2");
                assert_eq!(first.headers()["x-ratelimit-remaining"], "1");
                assert_eq!(
                    first.headers()[header::CACHE_CONTROL],
                    "private, no-store"
                );
            }
            let second = prepare()
                .map_body(|()| claviron_http::empty_body())
                .dispatch()
                .await;
            if headers {
                assert_eq!(second.headers()["x-ratelimit-remaining"], "0");
            }
            let denied = service
                .prepare(request("/"), RequestArrival::Established)
                .err()
                .expect("only two units fit")
                .into_response();
            assert_eq!(denied.status(), StatusCode::TOO_MANY_REQUESTS);
            assert!(denied.headers().contains_key(header::RETRY_AFTER));
            assert!(!denied.headers().contains_key("x-ratelimit-remaining"));
        }
    }
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn both_apps_use_one_dispatch_and_keep_the_admitted_generation() {
    for protocol in [HttpProtocol::H1, HttpProtocol::H2, HttpProtocol::H3] {
        let router = axum::Router::new().fallback(
            |req: Request<axum::body::Body>| async move {
                assert!(req.extensions().get::<Arc<ConnCtx>>().is_some());
                Response::new(req.into_body())
            },
        );
        for app in [
            AppKind::Axum(router),
            AppKind::Bypass(BypassService::Handler(Arc::new(Echo))),
        ] {
            let service = service(protocol, "", app);
            let admitted = service
                .prepare(request("/"), RequestArrival::Established)
                .unwrap();
            drop(service);
            let replacement = noop(protocol, "transport: {protocols: []}");
            assert!(
                replacement
                    .prepare(request("/"), RequestArrival::Established)
                    .is_err()
            );
            let response = admitted
                .map_body(|()| {
                    boxed_body(Full::new(Bytes::from_static(b"payload")))
                })
                .dispatch()
                .await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                "payload"
            );
        }
    }
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn acl_reload_reaches_existing_bindings_before_axum_or_bypass_dispatch() {
    for protocol in [HttpProtocol::H1, HttpProtocol::H2, HttpProtocol::H3] {
        let router = axum::Router::new().fallback(|| async { "axum" });
        for app in [
            AppKind::Axum(router),
            AppKind::Bypass(BypassService::Handler(Arc::new(Echo))),
        ] {
            let service = service(protocol, "", app);
            let old = service
                .prepare(request("/admin"), RequestArrival::Established)
                .unwrap();
            let captured = service.policy.host.vhost_config.clone();
            let resolver = &service.policy.access;
            let key = service.policy.host.vhost_key.0.as_ref().clone();
            // Neither changing transport policy nor router construction is
            // required to publish a new request-admission ACL.
            resolver.upsert_host(key.clone(), Arc::new(serde_yaml::from_str(
                "root: new-root\ntransport: {protocols: []}\nacl:\n  '/': {policy: allow}\n  '/admin': {policy: deny}\n  '/admin/public': {policy: allow}"
            ).unwrap()));
            assert!(matches!(
                service.prepare(request("/admin"), RequestArrival::Established),
                Err(HostResponse::Rejected(RequestRejection::AccessDenied))
            ));
            assert_eq!(
                old.map_body(|()| claviron_http::empty_body())
                    .dispatch()
                    .await
                    .status(),
                StatusCode::OK
            );
            let allowed = service
                .prepare(
                    request("/admin/public?raw=%2f"),
                    RequestArrival::Established,
                )
                .unwrap();
            assert!(Arc::ptr_eq(
                &allowed.auth_context().host_config,
                &captured
            ));
            assert_eq!(allowed.request().uri(), "/admin/public?raw=%2f");
            assert_eq!(
                allowed
                    .map_body(|()| claviron_http::empty_body())
                    .dispatch()
                    .await
                    .status(),
                StatusCode::OK
            );
            // Opt-out also reaches this same live binding.
            resolver.upsert_host(
                key.clone(),
                Arc::new(serde_yaml::from_str("root: public").unwrap()),
            );
            assert!(
                service
                    .prepare(request("/admin"), RequestArrival::Established)
                    .is_ok()
            );
            resolver.delete_host(&key);
            resolver.upsert_host(
                "default".parse().unwrap(),
                Arc::new(serde_yaml::from_str("root: public").unwrap()),
            );
            assert!(
                matches!(
                    service.prepare(
                        request("/admin"),
                        RequestArrival::Established
                    ),
                    Err(HostResponse::Rejected(RequestRejection::HostRemoved))
                ),
                "a removed canonical must not inherit the default host's open ACL"
            );
        }
    }
}

#[test]
fn acl_uses_this_requests_trusted_ip_and_never_polls_a_body() {
    struct Unpollable;
    for protocol in [HttpProtocol::H1, HttpProtocol::H2, HttpProtocol::H3] {
        let mut service = noop(
            protocol,
            "aliases: [alias.test], acl: {'/admin': {policy: deny, allowed: ['203.0.113.1/32']}}",
        );
        // The connection arrived via the alias; authority must still match
        // that exact SNI. ACL lookup uses the canonical app.test key.
        Arc::get_mut(&mut service.policy).unwrap().host.hostname =
            HostName(Arc::new("alias.test".parse().unwrap()));
        for (ip, allowed) in [
            ("203.0.113.1", true),
            ("203.0.113.2", false),
            ("203.0.113.1", true),
        ] {
            let req = Request::builder()
                .uri("/admin?ignored=/public")
                .header(header::HOST, "alias.test")
                .header("x-real-ip", ip)
                .body(Unpollable)
                .unwrap();
            let result = service.prepare(req, RequestArrival::Established);
            assert_eq!(result.is_ok(), allowed);
            if !allowed {
                assert!(matches!(
                    result,
                    Err(HostResponse::Rejected(RequestRejection::AccessDenied))
                ));
            }
        }
        Arc::get_mut(&mut service.policy).unwrap().ingress.proxy = None;
        let forged = Request::builder()
            .uri("/admin")
            .header(header::HOST, "alias.test")
            .header("x-real-ip", "203.0.113.1")
            .body(Unpollable)
            .unwrap();
        assert!(matches!(
            service.prepare(forged, RequestArrival::Established),
            Err(HostResponse::Rejected(RequestRejection::AccessDenied))
        ));
        assert!(service.policy.connection.real_ip.is_none());
    }
}

#[tokio::test]
async fn invalid_reload_retains_the_enforced_acl() {
    let service = noop(HttpProtocol::H2, "acl: {'/admin': {policy: deny}}");
    let dir = tempfile::tempdir().unwrap();
    let state = claviron_runtime::AppState::standalone("acl-test", dir.path());
    std::fs::create_dir_all(state.config_dir()).unwrap();
    let path = state.config_dir().join("vhost.yaml");
    for yaml in [
        "app.test: {root: public, acl: {'/admin': {policy: alow}}}",
        "app.test:\n  root: public\n  acl:\n    '/admin': {policy: deny}\n    '/admin/': {policy: allow}",
        "app.test: {root: public, acl: {'/admin': {policy: deny, allowed: [invalid]}}}",
        "app.test: {root: public, error: 200}",
        "app.test: {root: public, redirect: {to: /new, code: 999}}",
        "app.test: {root: public, redirect: {to: '/not escaped'}}",
    ] {
        std::fs::write(&path, yaml).unwrap();
        assert!(service.policy.access.replace_all(&state).await.is_err());
        assert!(matches!(
            service.prepare(request("/admin"), RequestArrival::Established),
            Err(HostResponse::Rejected(RequestRejection::AccessDenied))
        ));
    }
}

/// A bypass handler that answers once it is let go.
struct Hold(Arc<tokio::sync::Notify>);
impl BypassHandler for Hold {
    fn call(
        &self,
        _: Request<RouteBody>,
    ) -> RouteFuture {
        let release = Arc::clone(&self.0);
        Box::pin(async move {
            release.notified().await;
            Ok(Response::new(claviron_http::empty_body()))
        })
    }
}

struct HeadTimingProbe(Arc<AtomicUsize>);

impl BypassHandler for HeadTimingProbe {
    fn times_its_head(&self) -> bool {
        self.0.fetch_add(1, Ordering::SeqCst);
        false
    }

    fn call(
        &self,
        _: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(async { Ok(Response::new(claviron_http::empty_body())) })
    }
}

#[tokio::test]
async fn bypass_without_head_timeout_does_not_query_backend_timing() {
    for (timeout, expected) in
        [(None, 0), (Some(std::time::Duration::from_secs(1)), 1)]
    {
        let queried = Arc::new(AtomicUsize::new(0));
        let service = service_with_limits(
            HttpProtocol::H1,
            "",
            AppKind::Bypass(BypassService::new(HeadTimingProbe(
                queried.clone(),
            ))),
            RequestPolicy {
                head_timeout: timeout,
                ..RequestPolicy::unlimited()
            },
        );
        let response = service
            .prepare(request("/"), RequestArrival::Established)
            .unwrap()
            .map_body(|()| claviron_http::empty_body())
            .dispatch()
            .await;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(queried.load(Ordering::SeqCst), expected);
    }
}

/// Admission holds on every path to a host, the bypass one too, which a
/// layer on the router never saw: a request past the host's limit waits its
/// `queue_timeout` and is refused with 503, and one whose head is late past
/// `head_timeout` is answered 504.
#[tokio::test]
async fn admission_holds_on_the_bypass_path() {
    use crate::pipeline::{Admission, HostApp};
    let release = Arc::new(tokio::sync::Notify::new());
    let app = HostApp {
        app: AppKind::Bypass(BypassService::Handler(Arc::new(Hold(
            Arc::clone(&release),
        )))),
        admission: Admission::limited(Some(1)),
        exchange_policy: None,
        server_header: None,
        #[cfg(feature = "rate-limit")]
        quotas: None,
    };
    let listener = RequestPolicy {
        // Short, and real: the test waits them out.
        queue_timeout: Some(std::time::Duration::from_millis(20)),
        head_timeout: Some(std::time::Duration::from_millis(200)),
        ..RequestPolicy::unlimited()
    };
    let service = service_with_limits(HttpProtocol::H1, "", app, listener);
    let send = |service: &HostService| {
        let prepared =
            service.prepare(request("/"), RequestArrival::Established).unwrap();
        tokio::spawn(
            prepared.map_body(|()| claviron_http::empty_body()).dispatch(),
        )
    };

    let first = send(&service);
    tokio::task::yield_now().await;
    let second = send(&service);
    assert_eq!(
        second.await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "no place"
    );
    assert_eq!(
        first.await.unwrap().status(),
        StatusCode::GATEWAY_TIMEOUT,
        "head late"
    );
}
