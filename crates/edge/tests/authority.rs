#![cfg(all(feature = "server", feature = "axum"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use claviron_edge::VhostResolver;
use claviron_edge::deny::DenyService;
use claviron_edge::transport::{
    HostIngress, HostResponse, HostService, OtherHandler, OtherService,
    RequestArrival, RequestAuthority, RequestRejection, RustRouter, VhostTable,
    resolve_request_authority, resolve_sni_from_table,
};
use claviron_http::ext::ConnCtx;
use claviron_http::listener::HttpProtocol;
use claviron_http::{RouteBody, RouteFuture, empty_body};
use http::{Method, Request, Response, StatusCode, Version, header};

struct Count(Arc<AtomicUsize>);
impl OtherHandler for Count {
    fn call(
        &self,
        _: Request<RouteBody>,
    ) -> RouteFuture {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(Response::new(empty_body())) })
    }
}

fn service(
    protocol: HttpProtocol,
    app: RustRouter,
) -> HostService {
    let table: VhostTable =
        serde_yaml::from_str("app.test: {root: public}").unwrap();
    let host = resolve_sni_from_table(&table, Some("app.test")).unwrap();
    let access = Arc::new(VhostResolver::default());
    access.upsert_host(
        host.vhost_key.0.as_ref().clone(),
        host.vhost_config.clone(),
    );
    HostService::new(
        app,
        Arc::new(ConnCtx {
            state: Default::default(),
            listener: "public".parse().unwrap(),
            proto: Some(protocol),
            remote: "127.0.0.1:1234".parse().unwrap(),
            local_ip: "127.0.0.1".parse().unwrap(),
            local_port: 443,
            real_ip: None,
            country_code: None,
            tls: None,
        }),
        host,
        HostIngress { proxy: None, deny: Arc::new(DenyService::empty()) },
        access,
        claviron_http::config::HttpPolicy::unlimited(),
    )
}

#[test]
fn configured_authority_resolution_keeps_aliases_and_ipv6() {
    let table: VhostTable = serde_yaml::from_str(
        "app.test: {root: public, aliases: [alias.test]}\n'[::1]': {root: public}"
    )
    .unwrap();
    for (raw, canonical) in
        [("ALIAS.test:8080", "app.test"), ("[::1]:8080", "[::1]")]
    {
        let request = Request::builder()
            .uri("/original?q=1")
            .header(header::HOST, raw)
            .body(())
            .unwrap();
        let authority = RequestAuthority::from_cleartext(&request).unwrap();
        let resolved = resolve_request_authority(&authority, &table).unwrap();
        assert_eq!(resolved.vhost_key.0.as_ref(), &canonical.parse().unwrap());
        assert_eq!(authority.authority().as_str(), raw);
        assert_eq!(request.uri(), "/original?q=1");
    }
}

#[tokio::test]
async fn authority_rejection_precedes_body_auth_and_both_dispatch_modes() {
    // The native head can be checked without even having a pollable body.
    struct UnreadBody;
    for (protocol, version) in [
        (HttpProtocol::H1, Version::HTTP_11),
        (HttpProtocol::H2, Version::HTTP_2),
        (HttpProtocol::H3, Version::HTTP_3),
    ] {
        let entries = Arc::new(AtomicUsize::new(0));
        let counter = entries.clone();
        let router = axum::Router::new().fallback(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            async { "ok" }
        });
        for app in [
            RustRouter::Axum(router),
            RustRouter::Other(OtherService::Handler(Arc::new(Count(
                entries.clone(),
            )))),
        ] {
            let service = service(protocol, app);
            entries.store(0, Ordering::Relaxed);
            for method in [Method::GET, Method::CONNECT] {
                for headers in [
                    vec![],
                    vec!["app.test", "app.test"],
                    vec!["bad@host"],
                    vec!["app.test:+443"],
                ] {
                    let mut builder = Request::builder()
                        .version(version)
                        .method(method.clone());
                    for value in headers {
                        builder = builder.header(header::HOST, value);
                    }
                    let request = builder.body(UnreadBody).unwrap();
                    let Err(reply) =
                        service.prepare(request, RequestArrival::Established)
                    else {
                        panic!(
                            "invalid head reached body/dispatch/auth context creation"
                        );
                    };
                    assert!(matches!(
                        reply,
                        HostResponse::Rejected(
                            RequestRejection::InvalidAuthority(_)
                        )
                    ));
                    let response = reply.into_response();
                    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
                    assert_eq!(response.into_body(), ());
                    assert_eq!(entries.load(Ordering::Relaxed), 0);
                }
            }
            let wrong = Request::builder()
                .version(version)
                .header(header::HOST, "other.test")
                .body(UnreadBody)
                .unwrap();
            assert!(matches!(
                service.prepare(wrong, RequestArrival::Established),
                Err(HostResponse::Rejected(RequestRejection::Misdirected))
            ));
            let good = Request::builder()
                .version(version)
                .header(header::HOST, "APP.test:443")
                .body(())
                .unwrap();
            assert_eq!(
                service
                    .prepare(good, RequestArrival::Established)
                    .unwrap()
                    .map_body(|()| empty_body())
                    .dispatch()
                    .await
                    .status(),
                StatusCode::OK
            );
            assert_eq!(entries.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn legacy_head_and_h2_authority_only_preserve_wire_metadata() {
    let h2_service =
        service(HttpProtocol::H2, RustRouter::Other(OtherService::Noop));
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri("https://APP.test:443/original?raw=%2f")
        .body(())
        .unwrap();
    let prepared =
        h2_service.prepare(request, RequestArrival::Established).unwrap();
    assert_eq!(
        prepared.request().uri(),
        "https://APP.test:443/original?raw=%2f"
    );
    assert!(!prepared.request().headers().contains_key(header::HOST));
    let service =
        service(HttpProtocol::H1, RustRouter::Other(OtherService::Noop));
    assert!(
        service
            .prepare(
                Request::builder().version(Version::HTTP_10).body(()).unwrap(),
                RequestArrival::Established
            )
            .is_ok()
    );
    assert!(matches!(
        service.prepare(Request::new(()), RequestArrival::Established),
        Err(HostResponse::Rejected(RequestRejection::InvalidAuthority(_)))
    ));
}
