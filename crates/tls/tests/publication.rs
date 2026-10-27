use std::sync::Barrier;

use super::*;

fn key() -> Arc<CertifiedKey> {
    let key = claviron_types::ssl::generators::generate_private_key().unwrap();
    let signing = rustls::crypto::aws_lc_rs::sign::any_supported_type(
        &rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der())
            .into(),
    )
    .unwrap();
    Arc::new(CertifiedKey::new(Vec::new(), signing))
}

#[test]
fn resolver_publication_preserves_disjoint_concurrent_updates() {
    let certificate = key();
    let resolver = CertResolver::new(HashMap::new(), certificate.clone());
    for _ in 0..128 {
        let start = Barrier::new(8);
        std::thread::scope(|scope| {
            for index in 0..8 {
                let (resolver, start, certificate) =
                    (&resolver, &start, &certificate);
                scope.spawn(move || {
                    start.wait();
                    resolver.upsert_host(
                        format!("host{index}.test"),
                        certificate.clone(),
                    );
                });
            }
        });
        assert_eq!(resolver.count(), 8);
        std::thread::scope(|scope| {
            for index in 0..8 {
                let (resolver, start, certificate) =
                    (&resolver, &start, &certificate);
                scope.spawn(move || {
                    start.wait();
                    resolver.remove_host(&format!("host{index}.test"));
                    resolver.upsert_host(
                        format!("new{index}.test"),
                        certificate.clone(),
                    );
                });
            }
        });
        assert_eq!(resolver.count(), 8);
        for index in 0..8 {
            assert!(!resolver.contains(&format!("host{index}.test")));
            assert!(resolver.contains(&format!("new{index}.test")));
        }
        resolver.replace_all(HashMap::new(), certificate.clone());
    }
}

#[test]
fn resolver_publication_orders_same_host_and_authoritative_replacement() {
    let old = key();
    let new = key();
    let resolver = CertResolver::new(HashMap::new(), old.clone());
    let (sent, received) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            resolver.upsert_host("shared.test", old.clone());
            sent.send(()).unwrap();
        });
        let (resolver, new) = (&resolver, &new);
        scope.spawn(move || {
            received.recv().unwrap();
            resolver.upsert_host("shared.test", new.clone());
        });
    });
    assert!(Arc::ptr_eq(&resolver.get_cert("shared.test").unwrap(), &new));
    resolver.replace_all(
        HashMap::from([("replacement.test".into(), old.clone())]),
        old,
    );
    assert!(!resolver.contains("shared.test"));
    resolver.upsert_host("shared.test", new.clone());
    resolver.set_default(new.clone());
    let published = resolver.published.load();
    assert_eq!(published.certs.len(), 2);
    assert!(Arc::ptr_eq(&published.default_cert, &new));
}

#[test]
fn resolver_publication_full_replacement_race_has_only_serial_outcomes() {
    let old = key();
    let new = key();
    let resolver = CertResolver::new(HashMap::new(), old.clone());
    for _ in 0..128 {
        let start = Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                start.wait();
                resolver.replace_all(
                    HashMap::from([("base.test".into(), new.clone())]),
                    new.clone(),
                );
            });
            scope.spawn(|| {
                start.wait();
                resolver.upsert_host("added.test", old.clone());
            });
        });
        let published = resolver.published.load();
        assert!(Arc::ptr_eq(&published.default_cert, &new));
        assert!(Arc::ptr_eq(&published.certs["base.test"], &new));
        assert!(matches!(published.certs.len(), 1 | 2));
        drop(published);
        resolver.replace_all(HashMap::new(), old.clone());
    }
}
