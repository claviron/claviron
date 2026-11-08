use crate::plan::HostContext;
use claviron_http::ext::ConnCtx;
use claviron_http::policy::HttpPolicy;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::plan::{HostPipeline, HostPipelineBuilder};
use crate::response::RequestRejection;
use claviron_http::ServiceResult;
use claviron_http::ext::{HostKey, HostName, RealIpExt};

use super::*;
use http::Request;

struct VhostCtx {
    hostname: HostName,
    vhost_key: HostKey,
}

fn host() -> VhostCtx {
    VhostCtx {
        hostname: HostName(Arc::new("alias.test".parse().unwrap())),
        vhost_key: HostKey(Arc::new("app.test".parse().unwrap())),
    }
}

fn native_pipeline() -> HostPipeline<(), RequestRejection> {
    let mut builder = HostPipelineBuilder::new();
    builder.layer(IdentityPolicy::new(HttpPolicy::unlimited())).unwrap();
    builder.build()
}

fn connection(ip: Option<IpAddr>) -> Arc<ConnCtx> {
    Arc::new(ConnCtx {
        transport: claviron_http::listener::ListenerTransport::Tcp,
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
    let host = host();
    let pipeline = native_pipeline();
    let context = Some(
        HostContext::new(
            &accepted,
            claviron_http::ext::RequestArrival::Established,
        )
        .with_host(&host.hostname, &host.vhost_key),
    );
    let mut identities = Vec::new();
    for client in ["203.0.113.1", "203.0.113.2"] {
        let request = Request::builder()
            .header("host", "alias.test")
            .header("x-real-ip", client)
            .body(())
            .unwrap();
        let ServiceResult::Unhandled(admitted) =
            pipeline.prepare_in(request, (), context)
        else {
            panic!("identity must preserve the request");
        };
        let identified =
            admitted.head().extensions.get::<Arc<ConnCtx>>().unwrap();
        assert!(Arc::ptr_eq(identified, &accepted));
        assert!(admitted.head().extensions.get::<RealIpExt>().is_none());
        assert_eq!(
            claviron_http::ext::request_real_ip(&admitted.head().extensions),
            Some(accepted.remote.ip())
        );
        identities.push(identified.clone());
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
fn explicit_identity_keeps_single_body_ownership() {
    let accepted = connection(Some("192.0.2.1".parse().unwrap()));
    let host = host();
    let pipeline = native_pipeline();
    let context = Some(
        HostContext::new(
            &accepted,
            claviron_http::ext::RequestArrival::Established,
        )
        .with_host(&host.hostname, &host.vhost_key),
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
    let ServiceResult::Unhandled(admitted) =
        pipeline.prepare_in(request, (), context)
    else {
        panic!("identity must preserve the request");
    };
    assert!(Arc::ptr_eq(
        admitted.head().extensions.get::<Arc<ConnCtx>>().unwrap(),
        &accepted
    ));
    assert_eq!(admitted.head().uri, "/ok?raw=%2F");
    let admitted = admitted.map_body(|body| {
        assert_eq!(body.bytes.as_ptr(), pointer);
        body
    });
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(admitted);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}
