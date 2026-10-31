use std::sync::atomic::{AtomicUsize, Ordering};

use crate::deny::DenyService;
use crate::pipeline::host::{HostPipeline, HostRequestState};
use crate::service::{HostIngress, RequestArrival};
use claviron_http::ServiceResult;
use claviron_http::ext::{HostKey, HostName, RealIpExt};

use super::*;

fn host() -> VhostCtx {
    VhostCtx {
        hostname: HostName(Arc::new("alias.test".parse().unwrap())),
        vhost_key: HostKey(Arc::new("app.test".parse().unwrap())),
        vhost_config: Arc::new(
            serde_yaml::from_str("root: public\naliases: [alias.test]")
                .unwrap(),
        ),
        application: None,
    }
}

fn native_pipeline(connection: &Arc<ConnCtx>, host: &VhostCtx, ingress: &HostIngress) -> HostPipeline<HostRequestState, crate::service::HostResponse> {
    let access = Arc::new(crate::VhostResolver::default());
    access.upsert_host(host.vhost_key.0.as_ref().clone(), host.vhost_config.clone());
    HostPipeline::for_host(connection, host, ingress, access)
}

fn connection(ip: Option<IpAddr>) -> Arc<ConnCtx> {
    Arc::new(ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(claviron_http::listener::HttpProtocol::H1),
        remote: "192.0.2.1:4321".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 8080,
        real_ip: ip,
        country_code: None,
        tls: None,
    })
}

fn proxy(trusted: &str) -> ProxyConfig {
    ProxyConfig {
        trusted: vec![trusted.parse().unwrap()],
        headers: vec![
            "x-real-ip".parse().unwrap(),
            "x-forwarded-for".parse().unwrap(),
        ],
    }
}

#[test]
fn host_identity_uses_only_captured_trust_and_does_not_cache_proxy_clients() {
    let accepted = connection(None);
    let host = host();
    for (policy, expected) in [
        (None, "192.0.2.1"),
        (Some(proxy("198.51.100.0/24")), "192.0.2.1"),
        (Some(proxy("192.0.2.0/24")), "203.0.113.1"),
    ] {
        let pipeline = native_pipeline(
            &accepted,
            &host,
            &HostIngress {
                proxy: policy,
                deny: Arc::new(DenyService::empty()),
            },
        );
        let request = Request::builder()
            .header("host", "alias.test")
            .header("x-real-ip", "203.0.113.1")
            .body(())
            .unwrap();
        let admitted = pipeline
            .admit(request, HostRequestState::new(RequestArrival::Established))
            .unwrap();
        assert_eq!(
            admitted.connection.real_ip,
            Some(expected.parse().unwrap())
        );
        assert!(Arc::ptr_eq(
            &admitted.connection,
            admitted.request.extensions().get::<Arc<ConnCtx>>().unwrap()
        ));
    }

    let pipeline = native_pipeline(
        &accepted,
        &host,
        &HostIngress {
            proxy: Some(proxy("192.0.2.0/24")),
            deny: Arc::new(DenyService::empty()),
        },
    );
    let mut identities = Vec::new();
    for (primary, fallback, expected) in [
        ("203.0.113.1", "203.0.113.99", "203.0.113.1"),
        ("invalid", "203.0.113.99", "192.0.2.1"),
        ("203.0.113.2", "203.0.113.99", "203.0.113.2"),
    ] {
        let request = Request::builder()
            .header("host", "alias.test")
            .header("x-real-ip", primary)
            .header("x-forwarded-for", fallback)
            .body(())
            .unwrap();
        let admitted = pipeline
            .admit(request, HostRequestState::new(RequestArrival::Established))
            .unwrap();
        assert_eq!(
            admitted.connection.real_ip,
            Some(expected.parse().unwrap())
        );
        assert_eq!(
            admitted.request.extensions().get::<RealIpExt>().unwrap().0,
            expected.parse::<IpAddr>().unwrap()
        );
        identities.push(admitted.connection);
    }
    assert!(!Arc::ptr_eq(&identities[0], &identities[1]));
    assert!(!Arc::ptr_eq(&identities[1], &identities[2]));
    assert_eq!(identities[0].real_ip, Some("203.0.113.1".parse().unwrap()));
    assert!(accepted.real_ip.is_none());
}

struct BodyOwner {
    bytes: Box<[u8]>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for BodyOwner {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn host_identity_follows_ingress_and_keeps_single_body_ownership() {
    let accepted = connection(Some("192.0.2.1".parse().unwrap()));
    let pipeline = native_pipeline(
        &accepted,
        &host(),
        &HostIngress {
            proxy: Some(proxy("192.0.2.0/24")),
            deny: Arc::new(DenyService::from_set(
                &regex::RegexSet::new(["^/blocked"]).unwrap(),
            )),
        },
    );
    let dropped = Arc::new(AtomicUsize::new(0));
    let body =
        BodyOwner { bytes: Box::from(*b"untouched"), dropped: dropped.clone() };
    let pointer = body.bytes.as_ptr();
    let request = Request::builder()
        .uri("/ok?raw=%2F")
        .header("host", "alias.test")
        .header("x-real-ip", "203.0.113.1")
        .body(body)
        .unwrap();
    let admitted = pipeline
        .admit(request, HostRequestState::new(RequestArrival::Established))
        .unwrap();
    assert!(Arc::ptr_eq(&admitted.connection, &accepted));
    assert_eq!(admitted.request.body().bytes.as_ptr(), pointer);
    assert_eq!(admitted.request.uri(), "/ok?raw=%2F");
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(admitted);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);

    let request = Request::builder()
        .uri("/blocked")
        .header("host", "alias.test")
        .body(BodyOwner {
            bytes: Box::from(*b"untouched"),
            dropped: dropped.clone(),
        })
        .unwrap();
    let ServiceResult::Handled(reply) = pipeline.prepare_with_state(
        request,
        HostRequestState::new(RequestArrival::Established),
    ) else {
        panic!("deny must reject before identity")
    };
    assert_eq!(reply.response, crate::service::HostResponse::Rejected(RequestRejection::PathDenied));
    assert!(reply.state.identity().connection().is_none());
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
    drop(reply);
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
}
