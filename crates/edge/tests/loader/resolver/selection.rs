use super::*;
use crate::pipeline::other::{OtherHandler, OtherService};
use crate::pipeline::{Admission, RustRouter, HostApp, HostAppSnapshot};
use crate::service::{
    VhostBindings, VhostCtx, resolve_request_authority, resolve_sni_from_table,
};
use claviron_http::authority::RequestAuthority;
use claviron_http::{RouteBody, RouteFuture};
use http::{HeaderValue, Request};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

fn host(name: &str) -> Host {
    name.parse().unwrap()
}

fn resolver() -> VhostResolver {
    let resolver = VhostResolver::default();
    resolver.publish(serde_yaml::from_str(
        "a.test: {root: first, aliases: [alias.test]}\nb.test: {root: second}\ndefault: {root: fallback}",
    ).unwrap()).unwrap();
    resolver
}

fn applications(
    table: &VhostTable,
    marker: Option<&str>,
) -> crate::Result<HostAppSnapshot> {
    assert!(table.prepared::<HostAppSnapshot>().is_none());
    let mut apps = HostAppSnapshot::default();
    for (canonical, record) in table.canonical_entries() {
        assert!(table.resolve_with_application(canonical).unwrap().1.is_none());
        let root = record.config.root();
        let name = marker
            .unwrap_or_else(|| root.file_name().unwrap().to_str().unwrap());
        let app = HostApp {
            admission: Admission::limited(Some(1)),
            server_header: Some(HeaderValue::from_str(name).unwrap()),
            ..RustRouter::Other(OtherService::Noop).into()
        };
        apps.insert(canonical.clone(), app).unwrap();
    }
    Ok(apps)
}

fn install(resolver: &VhostResolver) {
    resolver
        .prepare_publications(|table, _: Option<&HostAppSnapshot>| {
            applications(table, None)
        })
        .unwrap();
}

fn resolve(
    table: &VhostTable,
    name: &str,
) -> Option<VhostCtx> {
    let request =
        Request::builder().header(http::header::HOST, name).body(()).unwrap();
    let authority = RequestAuthority::from_cleartext(&request).unwrap();
    resolve_request_authority(&authority, table)
}

fn assert_selected(
    ctx: &VhostCtx,
    canonical: &str,
    marker: &str,
) {
    assert_eq!(ctx.vhost_key.0.as_ref(), &host(canonical));
    assert_eq!(
        ctx.application.as_ref().unwrap().server_header.as_ref().unwrap(),
        marker
    );
}

#[test]
fn host_selection_alias_default_and_sni_share_the_bound_application() {
    let resolver = resolver();
    install(&resolver);
    let table = resolver.snapshot();
    let bindings = VhostBindings::new(table.clone(), table.clone());
    for (name, canonical, marker) in [
        ("a.test", "a.test", "first"),
        ("alias.test", "a.test", "first"),
        ("unknown.test", "default", "fallback"),
    ] {
        let cleartext = resolve(&table, name).unwrap();
        let sni = resolve_sni_from_table(&table, Some(name)).unwrap();
        let tls = bindings.resolve_sni(Some(name)).unwrap();
        for ctx in [&cleartext, &sni, &tls] {
            assert_selected(ctx, canonical, marker);
            assert!(Arc::ptr_eq(
                &ctx.vhost_config,
                &table.canonical_config(&host(canonical)).unwrap()
            ));
        }
    }
    let canonical = resolve(&table, "a.test").unwrap();
    let alias = resolve(&table, "alias.test").unwrap();
    let first = canonical.application.unwrap();
    let second = alias.application.unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut entering = pin!(first.admission.enter(None));
    let Poll::Ready(Some(place)) = entering.as_mut().poll(&mut cx) else {
        panic!("the first request must be admitted");
    };
    let mut queued = pin!(second.admission.enter(None));
    assert!(queued.as_mut().poll(&mut cx).is_pending());
    drop(place);
    assert!(matches!(queued.as_mut().poll(&mut cx), Poll::Ready(Some(_))));
}

#[test]
fn host_selection_reload_keeps_old_contexts_and_applies_alias_removal_and_recreation()
 {
    let resolver = resolver();
    install(&resolver);
    let transport_policy = resolver.snapshot();
    let accepted = resolve(&resolver.snapshot(), "alias.test").unwrap();
    resolver.upsert_alias(host("alias.test"), host("b.test")).unwrap();
    let moved = resolve(&resolver.snapshot(), "alias.test").unwrap();
    assert_selected(&accepted, "a.test", "first");
    assert_selected(&moved, "b.test", "second");
    let alias_only =
        VhostBindings::new(transport_policy.clone(), resolver.snapshot());
    assert_selected(
        &alias_only.resolve_sni(Some("alias.test")).unwrap(),
        "b.test",
        "second",
    );

    let replacement: Arc<HostConfig> = Arc::new(
        serde_yaml::from_str("root: third\naliases: [alias.test]").unwrap(),
    );
    resolver.upsert_host(host("b.test"), replacement.clone());
    let latest = resolve(&resolver.snapshot(), "alias.test").unwrap();
    assert_selected(&latest, "b.test", "third");
    assert!(Arc::ptr_eq(&latest.vhost_config, &replacement));
    assert!(!Arc::ptr_eq(&moved.vhost_config, &replacement));
    assert_selected(&moved, "b.test", "second");
    let stale_transport =
        VhostBindings::new(transport_policy, resolver.snapshot());
    assert!(stale_transport.resolve_sni(Some("alias.test")).is_err());

    assert!(resolver.delete_host(&host("b.test")));
    assert_selected(
        &resolve(&resolver.snapshot(), "alias.test").unwrap(),
        "default",
        "fallback",
    );
    assert!(matches!(
        resolver.request_policy(&host("b.test")),
        crate::service::policy::PolicySnapshot::Removed
    ));
    assert!(resolver.delete_host(&host("default")));
    assert!(resolve(&resolver.snapshot(), "alias.test").is_none());
    resolver.upsert_host(
        host("b.test"),
        Arc::new(
            serde_yaml::from_str("root: recreated\naliases: [alias.test]")
                .unwrap(),
        ),
    );
    assert_selected(
        &resolve(&resolver.snapshot(), "alias.test").unwrap(),
        "b.test",
        "recreated",
    );
    assert_selected(&accepted, "a.test", "first");
}

#[test]
fn host_selection_application_reload_preserves_config_and_transport_binding() {
    let resolver = Arc::new(resolver());
    install(&resolver);
    let accepted = resolve(&resolver.snapshot(), "alias.test").unwrap();
    let mut binding = resolver.bind_config(
        accepted.hostname.0.clone(),
        accepted.vhost_key.0.clone(),
        accepted.vhost_config.clone(),
    );
    let mut retired = pin!(binding.retired());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(retired.as_mut().poll(&mut cx).is_pending());

    resolver
        .prepare_publications(|table, _: Option<&HostAppSnapshot>| {
            applications(table, Some("reloaded"))
        })
        .unwrap();
    let latest = resolve(&resolver.snapshot(), "alias.test").unwrap();
    assert_selected(&latest, "a.test", "reloaded");
    assert_selected(&accepted, "a.test", "first");
    assert!(Arc::ptr_eq(&accepted.vhost_config, &latest.vhost_config));
    assert!(retired.as_mut().poll(&mut cx).is_pending());
}

#[test]
fn host_selection_failed_preparation_retains_the_published_bindings() {
    let resolver = resolver();
    install(&resolver);
    struct LaterPreparation;
    let fail = Arc::new(AtomicBool::new(false));
    let reject = fail.clone();
    resolver
        .prepare_publications(move |_, _: Option<&LaterPreparation>| {
            if reject.load(Ordering::Relaxed) {
                return Err(crate::Error::other("candidate rejected"));
            }
            Ok(LaterPreparation)
        })
        .unwrap();
    let before = resolver.snapshot();
    fail.store(true, Ordering::Relaxed);
    assert!(resolver.upsert_alias(host("alias.test"), host("b.test")).is_err());
    assert!(Arc::ptr_eq(&before, &resolver.snapshot()));
    assert_selected(
        &resolve(&resolver.snapshot(), "alias.test").unwrap(),
        "a.test",
        "first",
    );
    assert!(
        resolver
            .prepare_publications::<HostAppSnapshot>(|_, _| Err(
                crate::Error::other("rebuild rejected")
            ))
            .is_err()
    );
    assert!(Arc::ptr_eq(&before, &resolver.snapshot()));
    fail.store(false, Ordering::Relaxed);
    resolver.upsert_alias(host("alias.test"), host("b.test")).unwrap();
    assert_selected(
        &resolve(&resolver.snapshot(), "alias.test").unwrap(),
        "b.test",
        "second",
    );
}

struct KeepsDeclarations {
    _declarations: VhostTable,
    _generation: Arc<()>,
}

impl OtherHandler for KeepsDeclarations {
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

fn install_owned(
    resolver: &VhostResolver,
    generation: Arc<()>,
) {
    resolver
        .prepare_publications(move |table, _: Option<&HostAppSnapshot>| {
            let mut apps = HostAppSnapshot::default();
            for (canonical, _) in table.canonical_entries() {
                assert!(
                    table
                        .resolve_with_application(canonical)
                        .unwrap()
                        .1
                        .is_none()
                );
                apps.insert(
                    canonical.clone(),
                    RustRouter::Other(OtherService::new(KeepsDeclarations {
                        _declarations: table.clone(),
                        _generation: generation.clone(),
                    })),
                )
                .unwrap();
            }
            Ok(apps)
        })
        .unwrap();
}

#[test]
fn host_selection_declarations_do_not_retain_old_application_generations() {
    let resolver = resolver();
    let first = Arc::new(());
    let old_generation = Arc::downgrade(&first);
    install_owned(&resolver, first);
    let accepted = resolve(&resolver.snapshot(), "alias.test").unwrap();
    let second = Arc::new(());
    let current_generation = Arc::downgrade(&second);
    install_owned(&resolver, second);
    assert!(old_generation.upgrade().is_some());
    drop(accepted);
    assert!(old_generation.upgrade().is_none());
    let declarations = resolver.snapshot().declarations_only();
    assert!(declarations.prepared::<HostAppSnapshot>().is_none());
    drop(resolver);
    assert!(current_generation.upgrade().is_none());
    assert!(declarations.resolve(&host("a.test")).is_some());
}
