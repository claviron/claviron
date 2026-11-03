use super::*;
use crate::transport::publication::{ListenerPublication, PreparedListener};
use std::sync::atomic::{AtomicBool, Ordering};

struct Listener {
    id: claviron_http::ListenerId,
    reject: AtomicBool,
    views: Arc<Mutex<Vec<Arc<VhostTable>>>>,
    factory_refreshes: Arc<Mutex<Vec<bool>>>,
}

impl Listener {
    fn new(id: &str) -> Arc<Self> {
        Arc::new(Self {
            id: id.parse().unwrap(),
            reject: AtomicBool::new(false),
            views: Arc::default(),
            factory_refreshes: Arc::default(),
        })
    }
    fn count(&self) -> usize {
        self.views.lock().unwrap().len()
    }
}

impl ListenerPublication for Listener {
    fn listener_id(&self) -> &claviron_http::ListenerId {
        &self.id
    }
    fn prepare(
        &self,
        hosts: &VhostTable,
        refresh_factories: bool,
    ) -> crate::Result<PreparedListener> {
        if self.reject.load(Ordering::Relaxed) {
            return Err(crate::Error::other("candidate rejected"));
        }
        let hosts = Arc::new(hosts.clone());
        let views = self.views.clone();
        let refreshes = self.factory_refreshes.clone();
        Ok(Box::new(move || {
            views.lock().unwrap().push(hosts);
            refreshes.lock().unwrap().push(refresh_factories);
        }))
    }
}

#[test]
fn listener_publications_stage_together_but_targeted_updates_keep_siblings() {
    let resolver = VhostResolver::default();
    let first = Listener::new("first");
    let second = Listener::new("second");
    let initial: VhostTable =
        serde_yaml::from_str("example.test: {aliases: [alias.test]}").unwrap();
    resolver.publish(initial).unwrap();
    resolver.register_listener(first.clone(), || Ok(())).unwrap();
    resolver.register_listener(second.clone(), || Ok(())).unwrap();
    let failed = Listener::new("failed-start");
    assert!(
        resolver
            .register_listener(failed.clone(), || {
                Err::<(), _>(crate::Error::other("start refused"))
            })
            .is_err()
    );
    assert_eq!(failed.count(), 0);
    let retained = first.views.lock().unwrap()[0].clone();
    resolver
        .update_listener(|hosts| {
            first.prepare(hosts, true)?();
            Ok(())
        })
        .unwrap();
    assert_eq!((first.count(), second.count()), (2, 1));

    second.reject.store(true, Ordering::Relaxed);
    let next: VhostTable =
        serde_yaml::from_str("example.test: {aliases: [new.test]}").unwrap();
    assert!(resolver.publish(next.clone()).is_err());
    assert_eq!((first.count(), second.count()), (2, 1));
    assert!(resolver.resolve_exact(&"alias.test".parse().unwrap()).is_some());
    assert!(resolver.resolve_exact(&"new.test".parse().unwrap()).is_none());

    second.reject.store(false, Ordering::Relaxed);
    resolver.publish(next).unwrap();
    assert_eq!((first.count(), second.count()), (3, 2));
    assert!(resolver.resolve_exact(&"new.test".parse().unwrap()).is_some());
    assert!(retained.resolve_exact(&"alias.test".parse().unwrap()).is_some());
    assert!(retained.resolve_exact(&"new.test".parse().unwrap()).is_none());
    assert!(
        resolver.register_listener(Listener::new("first"), || Ok(())).is_err()
    );

    resolver.prepare_publications(|_, _: Option<&()>| Ok(())).unwrap();
    assert_eq!((first.count(), second.count()), (4, 3));
    drop(second);
    resolver.publish(VhostTable::default()).unwrap();
    assert_eq!(first.count(), 5);
    assert_eq!(
        *first.factory_refreshes.lock().unwrap(),
        [true, true, false, true, false]
    );
    assert_eq!(resolver.listeners.lock().unwrap().len(), 1);
}

#[test]
fn listener_bindings_protect_retiring_tcp_owners_across_tls_driver_changes() {
    let config: claviron_http::config::HttpYaml = serde_yaml::from_str(
        "defaults:\n  listener:\n    requests: {max_body_size: off, body_read_timeout: off}\nlisteners:\n  plain:\n    transport: tcp\n    listen: 127.0.0.1:8080\n    protocols: h1\n  secure:\n    transport: tcp\n    tls: true\n    listen: 127.0.0.1:8443\n    protocols: [h1, h2]\n"
    ).unwrap();
    let config = config.resolve().unwrap();
    let plain = config.listeners[&"plain".parse().unwrap()].clone();
    let mut secure =
        config.listeners[&"secure".parse().unwrap()].as_ref().clone();
    secure.listen = plain.listen.clone();
    let secure = Arc::new(secure);
    let resolver = VhostResolver::default();
    let owned = resolver.start_listener_binding(plain).unwrap();
    assert!(resolver.validate_listener_binding(secure.clone()).is_err());
    assert!(resolver.start_listener_binding(secure.clone()).is_err());
    drop(owned);
    resolver.start_listener_binding(secure).unwrap();
}
