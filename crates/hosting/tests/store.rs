use super::*;

#[test]
fn certificate_rows_are_grouped_without_losing_aliases() {
    let now = OffsetDateTime::UNIX_EPOCH;
    let row = |alias_id: Option<i32>, hostname: Option<&str>| {
        CertificateWithAliasRow {
            revoked_at: None,
            id: 7,
            kind: CertificateKind::Server,
            issuer_id: Some(3),
            cn: "example.test".to_owned(),
            certs_der: vec![1, 2],
            key_der: vec![3, 4],
            expires_at: now,
            issued_at: now,
            updated_at: None,
            enabled: true,
            alias_id,
            alias_hostname: hostname.map(str::to_owned),
            alias_created_at: alias_id.map(|_| now),
        }
    };

    let grouped = group_certificates(vec![
        row(Some(10), Some("www.example.test")),
        row(Some(11), Some("api.example.test")),
    ]);

    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].certificate.cn, "example.test");
    assert_eq!(grouped[0].aliases.len(), 2);
    assert_eq!(grouped[0].aliases[0].certificate_id, 7);
}

#[cfg(feature = "sqlite")]
async fn alias_store() -> HostingStore {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("fixtures/aliases.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO hosting_servers (id, name) VALUES (1, 'edge'), (2, 'other')")
        .execute(&pool).await.unwrap();
    HostingStore::sqlite(pool)
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn alias_assignment_creates_identity_and_server_link_idempotently() {
    let store = alias_store().await;
    let alias =
        store.add_alias_for_server(1, "app.test", "www.test").await.unwrap();
    let host = store.vhost_by_name("app.test").await.unwrap().unwrap();
    assert_eq!(alias.vhost_id, host.id);
    assert_eq!(
        store.aliases_for_server(1).await.unwrap()["app.test"],
        ["www.test"]
    );
    let again =
        store.add_alias_for_server(2, "app.test", "www.test").await.unwrap();
    assert_eq!(alias.id, again.id);
    assert_eq!(store.servers_for_vhost("app.test").await.unwrap().len(), 2);

    let moved = store
        .replace_alias_for_server(1, "next.test", "www.test")
        .await
        .unwrap();
    assert_eq!(moved.id, alias.id);
    assert_eq!(
        store.aliases_for_server(1).await.unwrap()["next.test"],
        ["www.test"]
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn rejected_alias_assignment_rolls_back_identity_and_links() {
    let store = alias_store().await;
    store.add_alias_for_server(1, "app.test", "www.test").await.unwrap();
    assert!(matches!(
        store.add_alias_for_server(1, "conflict.test", "www.test").await,
        Err(HostingError::Conflict(_))
    ));
    assert!(store.vhost_by_name("conflict.test").await.unwrap().is_none());
    assert!(matches!(
        store.replace_alias_for_server(1, "missing.test", "absent.test").await,
        Err(HostingError::NotFound(_))
    ));
    assert!(store.vhost_by_name("missing.test").await.unwrap().is_none());
    assert!(
        store
            .add_alias_for_server(999, "invalid-server.test", "new.test")
            .await
            .is_err()
    );
    assert!(
        store.vhost_by_name("invalid-server.test").await.unwrap().is_none()
    );
    assert!(store.alias_by_hostname("new.test").await.unwrap().is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn issuer_relationships_round_trip_without_installing_client_identity() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("fixtures/certificates.sql"))
        .execute(&pool)
        .await
        .unwrap();
    let store = HostingStore::sqlite(pool);
    let material = |kind, issuer_id, cn: &str| NewCertificate {
        kind,
        issuer_id,
        cn: cn.into(),
        certs_der: vec![1, 2],
        key_der: vec![3, 4],
        issued_at: OffsetDateTime::UNIX_EPOCH,
        expires_at: OffsetDateTime::UNIX_EPOCH,
        enabled: true,
    };
    assert!(
        store
            .insert_unassigned_certificate(
                material(CertificateKind::Client, None, "client.test"),
                &[]
            )
            .await
            .is_err()
    );
    let ca = store
        .insert_unassigned_certificate(
            material(CertificateKind::Ca, None, "ca.test"),
            &["ca.test".into()],
        )
        .await
        .unwrap();
    let issuer = store.issuer_by_name("ca.test").await.unwrap().unwrap();
    assert!(ca.aliases.is_empty());
    assert_eq!(issuer.ca_certificate_id, Some(ca.certificate.id));
    assert_eq!(issuer.kind, IssuerKind::LocalCa);
    assert_eq!(issuer.acme_account_id, None);
    let saved = store
        .insert_unassigned_certificate(
            material(CertificateKind::Client, Some(issuer.id), "client.test"),
            &["client.test".into()],
        )
        .await
        .unwrap();
    assert!(saved.aliases.is_empty());
    assert_eq!(saved.certificate.kind, CertificateKind::Client);
    assert!(
        store
            .attach_certificate_to_server(saved.certificate.id, 1)
            .await
            .is_err()
    );
    assert!(
        store
            .add_certificate_to_servers("client.test", &["edge.test".into()])
            .await
            .is_err()
    );
    assert!(store.certificates_for_server(1).await.unwrap().is_empty());
    let public = store.public_certificates_by_issuer(issuer.id).await.unwrap();
    assert_eq!(public.len(), 1);
    assert_eq!(public[0].id, saved.certificate.id);
    assert_eq!(public[0].kind, CertificateKind::Client);
    assert_eq!(public[0].certs_der, saved.certificate.certs_der);
    assert_eq!(
        store.certificate_by_cn("client.test").await.unwrap().unwrap(),
        saved
    );
    assert!(matches!(
        store
            .insert_unassigned_certificate(
                material(
                    CertificateKind::Client,
                    Some(issuer.id),
                    "client.test"
                ),
                &[]
            )
            .await,
        Err(HostingError::Conflict(_))
    ));
    let acme = store.ensure_acme_issuer(1).await.unwrap();
    assert_eq!(store.ensure_acme_issuer(1).await.unwrap(), acme);
    store
        .insert_certificate(
            material(CertificateKind::Server, Some(acme.id), "server.test"),
            &["server.test".into(), "www.test".into()],
            1,
        )
        .await
        .unwrap();
    let server = store.certificates_for_server(1).await.unwrap().pop().unwrap();
    assert_eq!(server.aliases.len(), 1);
    assert_eq!(server.aliases[0].hostname, "www.test");
    assert_eq!(server.certificate.issuer_id, Some(acme.id));
    assert_eq!(store.acme_certificates(true, None).await.unwrap(), [server]);
    // Another issuer's server assignment does not affect this public view.
    assert_eq!(
        store.public_certificates_by_issuer(issuer.id).await.unwrap(),
        public
    );
    assert!(
        store.public_certificates_by_issuer(i32::MAX).await.unwrap().is_empty()
    );
    assert!(
        store.attach_certificate_to_server(ca.certificate.id, 1).await.is_err()
    );
}
