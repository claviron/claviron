use super::*;
use crate::pipeline::apps::HostContributions;
use crate::pipeline::host::compile::HostCompiler;
use crate::pipeline::{
    HostHandler, HostHandlerFuture, HostRequest, RustRouter,
};
use claviron_http::ext::{
    ClientAuthStatus, HostKey, HostName, RealIpExt, TlsInfo,
};
use futures_util::FutureExt;

fn check<B>(request: &Request<B>) {
    let connection = request.extensions().get::<Arc<ConnCtx>>().unwrap();
    let host = request.extensions().get::<HostName>().unwrap();
    assert_eq!(host.0.to_string(), "alias.test");
    assert!(request.extensions().get::<HostKey>().is_none());
    assert_eq!(connection.country_code.as_deref(), Some("GB"));
    assert_eq!(
        request.extensions().get::<HttpPolicy>(),
        Some(&HttpPolicy::unlimited())
    );
    assert!(request.extensions().get::<RealIpExt>().is_none());
    assert_eq!(
        claviron_http::ext::request_real_ip(request.extensions()),
        Some(connection.remote.ip())
    );
    assert_eq!(
        claviron_http::ext::request_scheme(request.extensions()),
        Some(connection.scheme())
    );
    assert!(request.extensions().get::<http::uri::Scheme>().is_none());
    assert!(request.extensions().get::<HttpProtocol>().is_none());
    let version = match connection.proto.unwrap() {
        HttpProtocol::H1 => http::Version::HTTP_11,
        HttpProtocol::H2 => http::Version::HTTP_2,
        HttpProtocol::H3 => http::Version::HTTP_3,
    };
    assert_eq!(request.version(), version);
}

struct RustOwner;
impl OtherHandler for RustOwner {
    fn reads_identity(&self) -> bool {
        true
    }

    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        check(&request);
        Box::pin(std::future::ready(Ok(http::Response::new(
            request.into_body(),
        ))))
    }
}

struct EmbeddedOwner;
impl HostHandler for EmbeddedOwner {
    fn reads_identity(&self) -> bool {
        true
    }

    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        let request = request.into_request();
        check(&request);
        Box::pin(std::future::ready(claviron_http::ServiceResult::Handled(
            http::Response::new(request.into_body()),
        )))
    }
}

#[test]
fn native_handoff_exports_identity_to_rust_and_registered_application_owners() {
    for (protocol, tls, version) in [
        (HttpProtocol::H1, false, http::Version::HTTP_11),
        (HttpProtocol::H1, true, http::Version::HTTP_11),
        (HttpProtocol::H2, true, http::Version::HTTP_2),
        (HttpProtocol::H3, true, http::Version::HTTP_3),
    ] {
        let connection = Arc::new(ConnCtx {
            state: Default::default(),
            listener: "public".parse().unwrap(),
            proto: Some(protocol),
            remote: "192.0.2.1:1234".parse().unwrap(),
            local_ip: "127.0.0.1".parse().unwrap(),
            local_port: 8080,
            real_ip: None,
            country_code: Some("GB".into()),
            tls: tls.then_some(TlsInfo {
                protocol_version: Some(rustls::ProtocolVersion::TLSv1_3),
                cipher_suite: None,
                client_auth: ClientAuthStatus::NotProvided,
                client_cert: None,
            }),
        });
        let table = serde_yaml::from_str(
            "app.test: {root: public, aliases: [alias.test]}",
        )
        .unwrap();
        let host = resolve_sni_from_table(&table, Some("alias.test")).unwrap();
        let state =
            claviron_runtime::AppState::standalone("backend-identity", ".");
        let compiler =
            HostCompiler { state: &state, http: None, protocol: None };
        let config = host.vhost_config.clone();
        let canonical = "app.test".parse().unwrap();
        let mut contributions = HostContributions::default();
        contributions.applications.push(Arc::new(EmbeddedOwner));
        let mut owners = vec![
            (
                Some(RustRouter::Other(OtherService::new(RustOwner))),
                HostContributions::default(),
            ),
            (None, contributions),
        ];
        #[cfg(feature = "axum")]
        owners.push((
            Some(RustRouter::Axum(axum::Router::new().fallback(
                |ip: claviron_http::ext::RealIp,
                 request: Request<axum::body::Body>| async move {
                    check(&request);
                    assert_eq!(
                        ip.0,
                        "192.0.2.1".parse::<std::net::IpAddr>().unwrap()
                    );
                    http::Response::new(request.into_body())
                },
            ))),
            HostContributions::default(),
        ));
        for (router, contributions) in owners {
            let owner = compiler
                .prepare(
                    &canonical,
                    config.clone(),
                    router,
                    &contributions,
                    #[cfg(feature = "rate-limit")]
                    None,
                    #[cfg(feature = "websocket")]
                    None,
                )
                .unwrap();
            let service = HostService::new(
                owner,
                connection.clone(),
                host.clone(),
                HttpPolicy::unlimited(),
            );
            let answering = service
                .request(
                    Request::builder()
                        .version(version)
                        .body(empty_body())
                        .unwrap(),
                    RequestArrival::Established,
                )
                .dispatch();
            let response = answering
                .now_or_never()
                .expect("metadata projection needs no executor");
            assert_eq!(response.status(), StatusCode::OK);
        }
    }
}
