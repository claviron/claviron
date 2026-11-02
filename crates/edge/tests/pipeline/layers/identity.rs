use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pipeline::host::{HostPipeline, HostRequestState};
use crate::service::RequestArrival;
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

fn native_pipeline(
    connection: &Arc<ConnCtx>,
    host: &VhostCtx,
) -> HostPipeline<HostRequestState, crate::service::HostResponse> {
    let access = Arc::new(crate::VhostResolver::default());
    access.upsert_host(
        host.vhost_key.0.as_ref().clone(),
        host.vhost_config.clone(),
    );
    HostPipeline::for_host(connection, host, access)
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

struct BodyOwner {
    bytes: Box<[u8]>,
    dropped: Arc<AtomicUsize>,
}

#[test]
fn direct_identity_without_a_transport_real_ip_is_shared_between_requests() {
    let accepted = connection(None);
    let pipeline = native_pipeline(&accepted, &host());
    let mut identities = Vec::new();
    for client in ["203.0.113.1", "203.0.113.2"] {
        let request = Request::builder()
            .header("host", "alias.test")
            .header("x-real-ip", client)
            .body(())
            .unwrap();
        let admitted = pipeline
            .admit(request, HostRequestState::new(RequestArrival::Established))
            .unwrap();
        assert_eq!(admitted.connection.real_ip, Some(accepted.remote.ip()));
        assert!(Arc::ptr_eq(
            &admitted.connection,
            admitted.head.extensions.get::<Arc<ConnCtx>>().unwrap(),
        ));
        assert_eq!(
            admitted.head.extensions.get::<RealIpExt>().unwrap().0,
            accepted.remote.ip()
        );
        identities.push(admitted.connection);
    }
    assert!(Arc::ptr_eq(&identities[0], &identities[1]));
    assert!(accepted.real_ip.is_none());
}

impl Drop for BodyOwner {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn host_identity_follows_ingress_and_keeps_single_body_ownership() {
    let accepted = connection(Some("192.0.2.1".parse().unwrap()));
    let pipeline = native_pipeline(&accepted, &host());
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
    assert_eq!(admitted.body.bytes.as_ptr(), pointer);
    assert_eq!(admitted.head.uri, "/ok?raw=%2F");
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(admitted);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);

    let request = Request::builder()
        .uri("/ok")
        .header("host", "other.test")
        .body(BodyOwner {
            bytes: Box::from(*b"untouched"),
            dropped: dropped.clone(),
        })
        .unwrap();
    let ServiceResult::Handled(reply) = pipeline.prepare_with_state(
        request,
        HostRequestState::new(RequestArrival::Established),
    ) else {
        panic!("invalid authority must reject before identity")
    };
    assert_eq!(
        reply.response,
        crate::service::HostResponse::Rejected(RequestRejection::Misdirected)
    );
    assert!(reply.state.identity().connection().is_none());
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
    drop(reply);
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
}
