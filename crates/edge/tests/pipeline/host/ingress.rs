use std::sync::atomic::{AtomicUsize, Ordering};

use claviron_http::ext::{HostKey, HostName};

use super::*;
use claviron_http::ServiceResult;

impl IngressPolicy {
    fn into_pipeline(self) -> HostPipeline<IngressState, RequestRejection> {
        let mut builder = HostPipelineBuilder::new();
        builder.layer(self).unwrap();
        builder.build()
    }
}

fn policy(
    protocol: HttpProtocol,
    config: &str,
    deny: Arc<DenyService>,
) -> IngressPolicy {
    let host = VhostCtx {
        hostname: HostName(Arc::new("app.test".parse().unwrap())),
        vhost_key: HostKey(Arc::new("app.test".parse().unwrap())),
        vhost_config: Arc::new(serde_yaml::from_str(config).unwrap()),
        application: None,
    };
    let connection = ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(protocol),
        remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 443,
        real_ip: None,
        country_code: None,
        tls: None,
    };
    IngressPolicy::new(&connection, &host, deny)
}

#[cfg(feature = "http3")]
#[test]
fn only_active_ingress_gates_are_registered() {
    let deny = Arc::new(DenyService::from_set(
        &regex::RegexSet::new(["^/blocked"]).unwrap(),
    ));
    let allow = "root: public\ntransport: {h1: {upgrade: true}, h3: {early_data: allow}}";
    let pipeline =
        policy(HttpProtocol::H3, allow, Arc::new(DenyService::empty()))
            .into_pipeline();
    // Authority/binding only: no replay, upgrade or deny callback remains.
    assert_eq!(pipeline.before.len(), 1);
    let pipeline =
        policy(HttpProtocol::H3, allow, deny.clone()).into_pipeline();
    assert_eq!(pipeline.before.len(), 2);
    let pipeline =
        policy(HttpProtocol::H1, allow, deny.clone()).into_pipeline();
    // H1 rejects native early-data facts even if the host allows H3 early data.
    assert_eq!(pipeline.before.len(), 3);
    let pipeline = policy(
        HttpProtocol::H1,
        "root: public\ntransport: {h1: {upgrade: false}}",
        deny,
    )
    .into_pipeline();
    assert_eq!(pipeline.before.len(), 4);
    assert!(pipeline.after.is_empty());
    assert!(pipeline.body.is_empty());
}

struct OwnedBody {
    bytes: Box<[u8]>,
    drops: Arc<AtomicUsize>,
}

impl Drop for OwnedBody {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn ingress_keeps_the_original_body_and_typed_rejection_before_later_work() {
    let mut layer =
        HostLayerBuilder::<IngressState, RequestRejection>::default();
    policy(HttpProtocol::H1, "root: public", Arc::new(DenyService::empty()))
        .prepare(&mut layer)
        .unwrap();
    let entered = Arc::new(AtomicUsize::new(0));
    let observed = entered.clone();
    layer.before(move |_, _| {
        observed.fetch_add(1, Ordering::Relaxed);
        ControlFlow::Continue(())
    });
    let mut builder = HostPipelineBuilder::new();
    builder.layers.push(layer);
    let pipeline = builder.build();
    let drops = Arc::new(AtomicUsize::new(0));
    let body =
        OwnedBody { bytes: Box::from(*b"untouched"), drops: drops.clone() };
    let pointer = body.bytes.as_ptr();
    let request = Request::builder()
        .uri("/ok?raw=%2F")
        .header("host", "app.test")
        .body(body)
        .unwrap();
    let ServiceResult::Unhandled(prepared) = pipeline.prepare_with_state(
        request,
        IngressState::new(RequestArrival::Established),
    ) else {
        panic!("valid ingress must remain unclaimed")
    };
    let (request, _) = prepared.into_request_and_state();
    assert_eq!(request.body().bytes.as_ptr(), pointer);
    assert_eq!(request.uri(), "/ok?raw=%2F");
    assert_eq!(entered.load(Ordering::Relaxed), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(request);
    assert_eq!(drops.load(Ordering::Relaxed), 1);

    let request = Request::builder()
        .header("host", "other.test")
        .body(OwnedBody {
            bytes: Box::from(*b"untouched"),
            drops: drops.clone(),
        })
        .unwrap();
    let result = pipeline.prepare_with_state(
        request,
        IngressState::new(RequestArrival::EarlyData),
    );
    let ServiceResult::Handled(reply) = result else {
        panic!("wrong host must reject")
    };
    assert_eq!(reply.response, RequestRejection::Misdirected);
    assert_eq!(entered.load(Ordering::Relaxed), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(reply);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}
