use claviron_http::ext::{ConnCtx, HostName};
use claviron_http::listener::HttpProtocol;

use super::*;
use crate::deny::DenyService;
use crate::service::{HostIngress, RequestArrival, VhostCtx};

fn fixture() -> (
    HostPipeline<HostRequestState, HostResponse>,
    Arc<VhostResolver>,
    HostKey,
) {
    let config = Arc::new(serde_yaml::from_str("root: public").unwrap());
    let canonical = HostKey(Arc::new("app.test".parse().unwrap()));
    let access = Arc::new(VhostResolver::default());
    access.upsert_host(canonical.0.as_ref().clone(), config.clone());
    let connection = Arc::new(ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(HttpProtocol::H1),
        remote: "192.0.2.1:4321".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 8080,
        real_ip: Some("192.0.2.1".parse().unwrap()),
        country_code: None,
        tls: None,
    });
    let host = VhostCtx {
        hostname: HostName(canonical.0.clone()),
        vhost_key: canonical.clone(),
        vhost_config: config,
        application: None,
    };
    let ingress = HostIngress {
        proxy: None,
        deny: Arc::new(DenyService::empty()),
    };
    let pipeline = HostPipeline::for_host(
        &connection, &host, &ingress, access.clone(),
    );
    (pipeline, access, canonical)
}

fn request() -> Request<()> {
    Request::builder()
        .uri("/public?raw=%2F")
        .header("host", "app.test")
        .body(())
        .unwrap()
}

#[test]
fn access_retains_admitted_rules_until_the_request_is_dropped() {
    let (pipeline, access, canonical) = fixture();
    let state = || HostRequestState::new(RequestArrival::Established);
    let open = pipeline.admit(request(), state()).unwrap();
    assert!(matches!(open.access, PolicySnapshot::Open));
    access.upsert_host(
        canonical.0.as_ref().clone(),
        Arc::new(serde_yaml::from_str(
            "root: public\nacl: {'/private': {policy: deny}}",
        ).unwrap()),
    );
    let admitted = pipeline.admit(request(), state()).unwrap();
    let PolicySnapshot::Rules(policy) = &admitted.access else {
        panic!("the same plan must observe newly enabled rules");
    };
    let generation = Arc::downgrade(policy);
    access.upsert_host(
        canonical.0.as_ref().clone(),
        Arc::new(serde_yaml::from_str(
            "root: public\nacl: {'/': {policy: deny}}",
        ).unwrap()),
    );
    assert!(matches!(
        pipeline.admit(request(), state()),
        Err(HostResponse::Rejected(RequestRejection::AccessDenied))
    ));
    assert!(admitted.access.check(
        &admitted.head.uri, "192.0.2.1".parse().unwrap(),
    ).is_ok());
    assert_eq!(&admitted.head.uri, "/public?raw=%2F");
    assert!(generation.upgrade().is_some());
    drop(admitted);
    assert!(generation.upgrade().is_none());
    access.upsert_host(
        canonical.0.as_ref().clone(),
        Arc::new(serde_yaml::from_str("root: public").unwrap()),
    );
    assert!(matches!(
        pipeline.admit(request(), state()).unwrap().access,
        PolicySnapshot::Open
    ));
}

#[test]
fn removed_canonical_cannot_inherit_a_default_or_reassigned_alias() {
    let (pipeline, access, canonical) = fixture();
    assert!(access.delete_host(&canonical.0));
    access.upsert_host(
        "default".parse().unwrap(),
        Arc::new(serde_yaml::from_str("root: public").unwrap()),
    );
    let state = || HostRequestState::new(RequestArrival::Established);
    assert!(matches!(
        pipeline.admit(request(), state()),
        Err(HostResponse::Rejected(RequestRejection::HostRemoved))
    ));
    access.upsert_host(
        "other.test".parse().unwrap(),
        Arc::new(serde_yaml::from_str(
            "root: public\naliases: [app.test]",
        ).unwrap()),
    );
    let (resolved, _) = access.resolve_exact(&canonical.0).unwrap();
    assert_eq!(resolved, "other.test".parse().unwrap());
    assert!(matches!(
        pipeline.admit(request(), state()),
        Err(HostResponse::Rejected(RequestRejection::HostRemoved))
    ));
}

#[test]
fn access_requires_prepared_identity() {
    let (_, resolver, canonical) = fixture();
    let mut builder =
        HostPipelineBuilder::<HostRequestState, HostResponse>::new();
    builder.layer(AccessPolicy::new(resolver, canonical)).unwrap();
    let pipeline = builder.build();
    assert!(matches!(
        pipeline.admit(
            request(), HostRequestState::new(RequestArrival::Established),
        ),
        Err(HostResponse::Rejected(RequestRejection::MissingIdentity))
    ));
}
