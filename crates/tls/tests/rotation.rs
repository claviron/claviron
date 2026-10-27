use std::collections::HashMap;
use std::sync::Arc;

use claviron_hosting::{CertificateAlias, CertificateKind};
use claviron_types::ssl::{generators, parsers};
use time::OffsetDateTime;

use super::*;

fn certificate(aliases: &[&str]) -> HostedCertificate {
    let ca_key = generators::generate_ca_key().unwrap();
    let ca_pem = ca_key.serialize_pem();
    let ca_cert = generators::generate_ca_cert(
        "authority.test",
        ca_pem.as_bytes(),
        30,
        None,
        None,
    )
    .unwrap();
    let key = generators::generate_private_key().unwrap();
    let cert = generators::generate_leaf_cert(
        CertificateKind::Server,
        "example.test",
        key.serialize_pem().as_bytes(),
        &ca_cert,
        ca_pem.as_bytes(),
        1,
        aliases,
    )
    .unwrap();
    let now = OffsetDateTime::now_utc();
    HostedCertificate {
        certificate: Certificate {
            id: 1,
            kind: CertificateKind::Server,
            issuer_id: None,
            cn: "example.test".into(),
            certs_der: parsers::concat_chain_bytes_from_pem(&cert).unwrap(),
            key_der: key.serialize_der(),
            expires_at: now + time::Duration::days(1),
            issued_at: now,
            updated_at: None,
            revoked_at: None,
            enabled: true,
        },
        aliases: aliases
            .iter()
            .enumerate()
            .map(|(index, hostname)| CertificateAlias {
                id: index as i32,
                certificate_id: 1,
                hostname: (*hostname).into(),
                created_at: now,
            })
            .collect(),
    }
}

#[tokio::test]
async fn resolver_publication_manager_retires_aliases_and_preserves_failed_update()
 {
    // A lazy handle satisfies the manager's borrow; upsert receives rows and
    // neither opens a database connection nor boots any provider or server.
    let store = HostingStore::sqlite(
        sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
    );
    let original = certificate(&["retired.test"]);
    let default = Arc::new(
        crate::pem::db_cert_to_certified_key(&original.certificate).unwrap(),
    );
    let resolver = CertResolver::new(HashMap::new(), default);
    let manager = CertificateManager::new(&store, &resolver);
    manager.upsert(original).unwrap();
    let original_key = resolver.get_cert("retired.test").unwrap();

    let rotated = certificate(&["current.test"]);
    let mut broken = rotated.clone();
    broken.certificate.key_der.clear();
    assert!(manager.upsert(broken).is_err());
    assert!(Arc::ptr_eq(
        &resolver.get_cert("retired.test").unwrap(),
        &original_key
    ));
    assert!(!resolver.contains("current.test"));

    manager.upsert(rotated.clone()).unwrap();
    assert!(!resolver.contains("retired.test"));
    assert!(resolver.contains("current.test"));
    assert!(!Arc::ptr_eq(
        &resolver.get_cert("example.test").unwrap(),
        &original_key
    ));
    let mut disabled = rotated;
    disabled.certificate.enabled = false;
    manager.upsert(disabled).unwrap();
    assert_eq!(resolver.count(), 0);
}
