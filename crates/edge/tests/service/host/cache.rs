use super::*;
use crate::HostConfig;
use crate::deny::DenyService;
use crate::pipeline::bypass::BypassHandler;
use crate::pipeline::bypass::BypassService;
use claviron_http::ext::HostKey;
use claviron_http::{RouteBody, RouteFuture};
use http::header;
use std::sync::atomic::{AtomicUsize, Ordering};

fn cache(access: Arc<VhostResolver>) -> CleartextHostCache {
    CleartextHostCache::new(
        Arc::new(ConnCtx {
            state: Default::default(),
            listener: "public".parse().unwrap(),
            proto: Some(HttpProtocol::H1),
            remote: "192.0.2.1:1234".parse().unwrap(),
            local_ip: "127.0.0.1".parse().unwrap(),
            local_port: 8080,
            real_ip: Some("192.0.2.1".parse().unwrap()),
            country_code: None,
            tls: None,
        }),
        HostIngress { proxy: None, deny: Arc::new(DenyService::empty()) },
        access,
        HttpPolicy::unlimited(),
    )
}

fn request<B>(body: B) -> Request<B> {
    Request::builder()
        .uri("/")
        .header(header::HOST, "alias.test")
        .body(body)
        .unwrap()
}

fn bind(
    cache: &mut CleartextHostCache,
    table: &VhostTable,
    app: HostApp,
) -> HostService {
    let authority = RequestAuthority::from_cleartext(&request(())).unwrap();
    let host = cache.resolve(&authority, table).unwrap();
    cache.bind(app, host)
}

fn app(header: &'static str) -> HostApp {
    let mut app: HostApp = AppKind::Bypass(BypassService::Noop).into();
    app.server_header = Some(http::HeaderValue::from_static(header));
    app
}

#[tokio::test]
async fn cached_host_policy_is_reused_without_caching_the_application() {
    let access = Arc::new(VhostResolver::default());
    access.upsert_host(
        "app.test".parse().unwrap(),
        Arc::new(
            serde_yaml::from_str("root: public\naliases: [alias.test]")
                .unwrap(),
        ),
    );
    let table = access.snapshot();
    let mut cache = cache(access);
    let first = bind(&mut cache, &table, app("first"));
    let second = bind(&mut cache, &table, app("second"));
    assert!(Arc::ptr_eq(&first.policy, &second.policy));
    for (service, expected) in [(first, "first"), (second, "second")] {
        let response = service
            .prepare(request(()), RequestArrival::Established)
            .unwrap()
            .map_body(|()| claviron_http::empty_body())
            .dispatch()
            .await;
        assert_eq!(response.headers()[header::SERVER], expected);
    }
}

struct Dropped(Arc<AtomicUsize>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn config_reload_replaces_cached_metadata_but_retains_owned_requests() {
    let access = Arc::new(VhostResolver::default());
    let first_config: Arc<HostConfig> = Arc::new(
        serde_yaml::from_str("root: first\naliases: [alias.test]\nrequests: {max_body_size: 16}\nresponses: {max_body_size: 32}").unwrap(),
    );
    access.upsert_host("app.test".parse().unwrap(), first_config.clone());
    let mut cache = cache(access.clone());
    cache.listener.requests.max_body_size = Some(12);
    cache.listener.responses.max_body_size = Some(48);
    let first = bind(&mut cache, &access.snapshot(), app("first"));
    let old_policy = Arc::downgrade(&first.policy);
    let dropped = Arc::new(AtomicUsize::new(0));
    let owned = first
        .prepare(request(Dropped(dropped.clone())), RequestArrival::Established)
        .unwrap();
    assert_eq!(owned.max_body_size(), Some(12));
    assert_eq!(owned.policy.responses.max_body_size, Some(32));
    drop(first);

    let next_config: Arc<HostConfig> = Arc::new(
        serde_yaml::from_str("root: second\naliases: [alias.test]\nrequests: {max_body_size: 8}\nresponses: {max_body_size: 64}").unwrap(),
    );
    access.upsert_host("app.test".parse().unwrap(), next_config.clone());
    let second = bind(&mut cache, &access.snapshot(), app("second"));
    assert!(!Arc::ptr_eq(&old_policy.upgrade().unwrap(), &second.policy));
    let next =
        second.prepare(request(()), RequestArrival::Established).unwrap();
    assert_eq!(next.max_body_size(), Some(8));
    assert_eq!(next.policy.responses.max_body_size, Some(48));
    assert_eq!(owned.max_body_size(), Some(12));
    assert_eq!(owned.policy.responses.max_body_size, Some(32));
    assert!(Arc::ptr_eq(
        owned.request().extensions().get::<Arc<crate::HostConfig>>().unwrap(),
        &first_config
    ));
    assert!(Arc::ptr_eq(
        next.request().extensions().get::<Arc<crate::HostConfig>>().unwrap(),
        &next_config
    ));
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(owned);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
    assert!(old_policy.upgrade().is_none());
}

#[test]
fn ingress_plan_reloads_for_a_new_binding_without_changing_the_pinned_one() {
    let access = Arc::new(VhostResolver::default());
    let mut cache = cache(access.clone());
    let mut bindings = Vec::new();
    for upgrade in [false, true] {
        let config = format!(
            "root: public\naliases: [alias.test]\ntransport: {{h1: {{upgrade: {upgrade}}}}}"
        );
        access.upsert_host(
            "app.test".parse().unwrap(),
            Arc::new(serde_yaml::from_str(&config).unwrap()),
        );
        bindings.push(bind(&mut cache, &access.snapshot(), app("test")));
    }
    assert!(!Arc::ptr_eq(&bindings[0].policy, &bindings[1].policy));
    for (binding, allowed) in bindings.iter().zip([false, true]) {
        let mut upgrade = request(());
        upgrade
            .headers_mut()
            .insert(header::UPGRADE, "websocket".parse().unwrap());
        match binding.prepare(upgrade, RequestArrival::Established) {
            Ok(_) => assert!(allowed),
            Err(crate::service::HostResponse::Rejected(
                RequestRejection::UpgradeForbidden,
            )) => assert!(!allowed),
            Err(_) => panic!("only the pinned upgrade policy may reject"),
        }
    }
}

#[test]
fn alias_remap_and_host_removal_cannot_reuse_an_old_policy() {
    let config: Arc<HostConfig> = Arc::new(
        serde_yaml::from_str("root: public\naliases: [alias.test]").unwrap(),
    );
    let old =
        VhostTable::from_host_map([("app.test".to_owned(), config.clone())])
            .unwrap();
    let new =
        VhostTable::from_host_map([("next.test".to_owned(), config)]).unwrap();
    let mut cache = cache(Arc::new(VhostResolver::default()));
    let first = bind(&mut cache, &old, app("first"));
    let next = bind(&mut cache, &new, app("second"));
    assert!(!Arc::ptr_eq(&first.policy, &next.policy));
    assert!(Arc::ptr_eq(
        &first.policy.host.vhost_config,
        &next.policy.host.vhost_config
    ));
    assert_ne!(first.policy.host.vhost_key, next.policy.host.vhost_key);
    let mut metadata = request(());
    next.policy.extensions.inject(
        &mut metadata,
        &next.policy.connection,
        &next.policy.host,
        None,
    );
    assert_eq!(
        metadata.extensions().get::<HostKey>().unwrap(),
        &next.policy.host.vhost_key
    );
    let old_policy = Arc::downgrade(&next.policy);
    drop(next);
    let authority = RequestAuthority::from_cleartext(&request(())).unwrap();
    assert!(cache.resolve(&authority, &VhostTable::default()).is_none());
    assert!(old_policy.upgrade().is_none());
}

#[test]
fn cached_policy_still_checks_current_acl_for_each_request() {
    let access = Arc::new(VhostResolver::default());
    access.upsert_host(
        "app.test".parse().unwrap(),
        Arc::new(
            serde_yaml::from_str("root: public\naliases: [alias.test]")
                .unwrap(),
        ),
    );
    let captured = access.snapshot();
    let mut cache = cache(access.clone());
    let first = bind(&mut cache, &captured, app("first"));
    assert!(first.prepare(request(()), RequestArrival::Established).is_ok());
    access.upsert_host("app.test".parse().unwrap(), Arc::new(serde_yaml::from_str("root: public\naliases: [alias.test]\nacl: {'/': {policy: deny}}").unwrap()));
    // Keep the same captured metadata to exercise a cache hit, independently
    // of a normal cleartext config publication invalidating that metadata.
    let next = bind(&mut cache, &captured, app("next"));
    assert!(Arc::ptr_eq(&first.policy, &next.policy));
    let rejected =
        next.prepare(request(()), RequestArrival::Established).err().unwrap();
    assert_eq!(rejected.into_response().status(), StatusCode::FORBIDDEN);
}

struct Backend;

impl BypassHandler for Backend {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(async move {
            drop(request);
            Ok(http::Response::new(claviron_http::empty_body()))
        })
    }
}

#[test]
fn cached_policy_does_not_retain_a_retired_backend() {
    let table =
        serde_yaml::from_str("app.test: {root: public, aliases: [alias.test]}")
            .unwrap();
    let mut cache = cache(Arc::new(VhostResolver::default()));
    let authority = RequestAuthority::from_cleartext(&request(())).unwrap();
    let mut host = cache.resolve(&authority, &table).unwrap();
    let backend = Arc::new(Backend);
    let weak = Arc::downgrade(&backend);
    let app: HostApp = AppKind::Bypass(BypassService::Handler(backend)).into();
    host.application = Some(app.clone());
    let service = cache.bind(app, host);
    assert!(cache.current.is_some());
    drop(service);
    assert!(weak.upgrade().is_none());
}
