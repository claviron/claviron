#![cfg(feature = "server")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use claviron_edge::transport::{
    HostRejection, VhostBindings, VhostTable, resolve_sni_from_table,
};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

struct WarnCount(Arc<AtomicUsize>);

impl Subscriber for WarnCount {
    fn enabled(
        &self,
        metadata: &Metadata<'_>,
    ) -> bool {
        *metadata.level() == Level::WARN
    }

    fn new_span(
        &self,
        _span: &Attributes<'_>,
    ) -> Id {
        Id::from_u64(1)
    }

    fn record(
        &self,
        _span: &Id,
        _values: &Record<'_>,
    ) {
    }

    fn record_follows_from(
        &self,
        _span: &Id,
        _follows: &Id,
    ) {
    }

    fn event(
        &self,
        event: &Event<'_>,
    ) {
        if *event.metadata().level() == Level::WARN {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn enter(
        &self,
        _span: &Id,
    ) {
    }

    fn exit(
        &self,
        _span: &Id,
    ) {
    }
}

#[test]
fn name_churn_keeps_configured_bindings_without_warning_history() {
    let table: Arc<VhostTable> = Arc::new(
        serde_yaml::from_str(
            "app.test: {root: public, aliases: [alias.test]}\ndefault: {root: fallback}"
        )
        .unwrap()
    );
    let bindings = VhostBindings::new(table.clone(), table.clone());
    let fallback = table.canonical_config(&"default".parse().unwrap()).unwrap();
    let warnings = Arc::new(AtomicUsize::new(0));

    tracing::subscriber::with_default(WarnCount(warnings.clone()), || {
        for name in ["app.test", "alias.test"] {
            let bound = bindings.resolve_sni(Some(name)).unwrap();
            assert_eq!(bound.vhost_key.0.to_string(), "app.test");
            assert_eq!(bound.hostname.0.to_string(), name);
        }
        for index in 0..2048 {
            let name = format!("unmapped-{index}.test");
            for bound in [
                bindings.resolve_sni(Some(&name)).unwrap(),
                resolve_sni_from_table(&table, Some(&name)).unwrap(),
            ] {
                assert_eq!(bound.hostname.0.to_string(), name);
                assert_eq!(bound.vhost_key.0.to_string(), "default");
                assert!(Arc::ptr_eq(&bound.vhost_config, &fallback));
            }
            assert!(table.resolve_exact(&name.parse().unwrap()).is_none());
        }
    });

    assert_eq!(warnings.load(Ordering::Relaxed), 0);
    assert_eq!(table.host_count(), 2);
    assert_eq!(table.alias_count(), 1);
    assert_eq!(
        Arc::strong_count(&fallback),
        2,
        "only the table and this test retain config"
    );
}

#[test]
fn unknown_names_without_a_catchall_do_not_become_bindings() {
    let table: Arc<VhostTable> =
        Arc::new(serde_yaml::from_str("app.test: {root: public}").unwrap());
    let bindings = VhostBindings::new(table.clone(), table.clone());
    for index in 0..2048 {
        let name = format!("unhosted-{index}.test");
        assert!(matches!(
            bindings.resolve_sni(Some(&name)),
            Err(HostRejection::NotHosted(rejected)) if rejected == name
        ));
        assert!(matches!(
            resolve_sni_from_table(&table, Some(&name)),
            Err(HostRejection::NotHosted(rejected)) if rejected == name
        ));
    }
    assert!(matches!(bindings.resolve_sni(None), Err(HostRejection::Missing)));
    assert!(matches!(
        bindings.resolve_sni(Some("bad name")),
        Err(HostRejection::Invalid(..))
    ));
    assert_eq!(table.host_count(), 1);
    assert_eq!(table.alias_count(), 0);
}
