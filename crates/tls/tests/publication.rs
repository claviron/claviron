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

#[test]
fn resolver_publication_unowned_pointer_groups_remain_compatible() {
    let certificate = key();
    let other = key();
    let resolver = CertResolver::new(
        HashMap::from([
            ("example.test".into(), certificate.clone()),
            ("alias.test".into(), certificate.clone()),
            ("other.test".into(), other.clone()),
        ]),
        certificate,
    );
    assert_eq!(resolver.remove_cert("EXAMPLE.TEST"), 2);
    assert!(Arc::ptr_eq(&resolver.get_cert("other.test").unwrap(), &other));
    assert_eq!(resolver.remove_cert("example.test"), 0);
}

#[cfg(feature = "db")]
#[test]
fn resolver_publication_rotates_complete_groups_and_preserves_new_owners() {
    let old = key();
    let new = key();
    let foreign = key();
    let resolver = CertResolver::new(HashMap::new(), old.clone());
    resolver.upsert_cert(
        "EXAMPLE.TEST",
        &["retired.test".into(), "moved.test".into()],
        old,
    );
    resolver.upsert_cert("other.test", &["moved.test".into()], foreign.clone());
    resolver.upsert_cert("example.test", &["current.test".into()], new.clone());
    assert!(!resolver.contains("retired.test"));
    assert!(Arc::ptr_eq(&resolver.get_cert("current.test").unwrap(), &new));
    assert!(Arc::ptr_eq(&resolver.get_cert("moved.test").unwrap(), &foreign));

    // Even reassigning the canonical hostname does not transfer ownership of
    // the rest of the original identity to its new occupant.
    resolver.upsert_cert(
        "other.test",
        &["example.test".into(), "moved.test".into()],
        foreign.clone(),
    );
    assert_eq!(resolver.remove_cert("EXAMPLE.TEST"), 1);
    assert!(!resolver.contains("current.test"));
    assert!(Arc::ptr_eq(&resolver.get_cert("example.test").unwrap(), &foreign));
    assert_eq!(resolver.remove_cert("example.test"), 0);
    assert_eq!(resolver.remove_cert("other.test"), 3);
    assert_eq!(resolver.count(), 0);
}

#[cfg(feature = "db")]
#[test]
fn resolver_publication_retains_ownership_across_authoritative_reload() {
    let old = key();
    let new = key();
    let certs = HashMap::from([
        ("example.test".into(), old.clone()),
        ("retired.test".into(), old.clone()),
        ("file.test".into(), old.clone()),
    ]);
    let owners = HashMap::from([
        ("example.test".into(), "example.test".into()),
        ("retired.test".into(), "example.test".into()),
    ]);
    let resolver =
        CertResolver::new_owned(certs.clone(), old.clone(), owners.clone());
    for _ in 0..2 {
        resolver.upsert_cert("example.test", &[], new.clone());
        assert!(!resolver.contains("retired.test"));
        assert!(resolver.contains("file.test"));
        assert_eq!(resolver.remove_cert("example.test"), 1);
        assert!(resolver.contains("file.test"));
        resolver.replace_owned(certs.clone(), old.clone(), owners.clone());
    }
}

#[cfg(feature = "db")]
#[test]
fn resolver_publication_managed_removal_preserves_raw_replacements() {
    let certificate = key();
    let resolver = CertResolver::new(HashMap::new(), certificate.clone());
    resolver.upsert_cert(
        "example.test",
        &["alias.test".into()],
        certificate.clone(),
    );
    resolver.upsert_host("example.test", certificate.clone());
    resolver.upsert_host("alias.test", certificate);
    assert_eq!(resolver.remove_managed_cert("example.test"), 0);
    assert_eq!(resolver.count(), 2);
}

#[cfg(feature = "db")]
#[test]
fn resolver_publication_concurrent_groups_are_complete_for_readers() {
    let old = key();
    let new = key();
    let resolver = CertResolver::new(HashMap::new(), old.clone());
    resolver.upsert_cert("example.test", &["old.test".into()], old.clone());
    let boundary = Barrier::new(3);
    let mut coherent = true;
    std::thread::scope(|scope| {
        scope.spawn(|| {
            for index in 0..256 {
                boundary.wait();
                let (alias, key) = if index % 2 == 0 {
                    ("new.test", &new)
                } else {
                    ("old.test", &old)
                };
                resolver.upsert_cert(
                    "example.test",
                    &[alias.into()],
                    key.clone(),
                );
                boundary.wait();
            }
        });
        scope.spawn(|| {
            for _ in 0..256 {
                boundary.wait();
                resolver.upsert_cert(
                    "independent.test",
                    &["alias.test".into()],
                    new.clone(),
                );
                boundary.wait();
            }
        });
        for _ in 0..256 {
            boundary.wait();
            let published = resolver.published.load();
            let identity = published.certs.get("example.test");
            let Some(identity) = identity else {
                coherent = false;
                boundary.wait();
                continue;
            };
            let alias = if Arc::ptr_eq(identity, &old) {
                "old.test"
            } else {
                "new.test"
            };
            let retired =
                if alias == "old.test" { "new.test" } else { "old.test" };
            coherent &= published
                .certs
                .get(alias)
                .is_some_and(|key| Arc::ptr_eq(key, identity));
            coherent &= !published.certs.contains_key(retired);
            drop(published);
            boundary.wait();
            coherent &= resolver.contains("independent.test");
            coherent &= resolver.count() == 4;
        }
    });
    assert!(
        coherent,
        "readers must see complete identities without losing independent writers"
    );
}

#[cfg(feature = "postgres")]
#[test]
fn resolver_publication_delayed_alias_removal_is_owner_conditional() {
    let certificate = key();
    let resolver = CertResolver::new(HashMap::new(), certificate.clone());
    resolver.upsert_cert(
        "first.test",
        &["alias.test".into()],
        certificate.clone(),
    );
    resolver.upsert_cert("second.test", &["alias.test".into()], certificate);
    resolver.remove_alias("first.test", "alias.test");
    assert!(resolver.contains("alias.test"));
    resolver.remove_alias("second.test", "second.test");
    assert!(resolver.contains("second.test"));
    resolver.remove_alias("SECOND.TEST", "ALIAS.TEST");
    assert!(!resolver.contains("alias.test"));
}
