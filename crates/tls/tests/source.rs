use super::*;
use claviron_types::ssl::{generators, parsers};

#[tokio::test]
async fn source_reads_trust_and_identity_without_sni_or_default_certificate() {
    let directory = tempfile::tempdir().unwrap();
    let ca_key = generators::generate_ca_key().unwrap();
    let ca = generators::generate_ca_cert(
        "Fixture CA",
        ca_key.serialize_pem().as_bytes(),
        30,
        None,
        None,
    )
    .unwrap();
    let files: CertStore = serde_yaml::from_str(&format!(
        "dir: {}\nauthority: {{local: fixture}}",
        directory.path().display()
    ))
    .unwrap();
    std::fs::create_dir_all(files.ca_dir("fixture")).unwrap();
    std::fs::write(files.ca_cert_path("fixture"), &ca).unwrap();
    let key_path = files.ca_key_path("fixture");
    let source = CertificateStore::new(files);
    let trust = source.authority().await.unwrap().unwrap();
    assert_eq!(trust.roots().len(), 1);
    let selected: CertificateSource =
        serde_yaml::from_str("local: fixture").unwrap();
    assert_eq!(source.trust(&selected).await.unwrap().roots(), trust.roots());
    assert!(source.signer(&trust).await.is_err());
    std::fs::write(key_path, ca_key.serialize_pem()).unwrap();
    assert!(source.signer(&trust).await.is_ok());
    std::fs::write(directory.path().join("ca.pem"), &ca).unwrap();
    let key = generators::generate_private_key().unwrap();
    let server = generators::generate_leaf_cert(
        CertificateKind::Server,
        "vpn.test",
        key.serialize_pem().as_bytes(),
        &ca,
        ca_key.serialize_pem().as_bytes(),
        1,
        &[] as &[String],
    )
    .unwrap();
    std::fs::write(directory.path().join("server.pem"), &server).unwrap();
    std::fs::write(
        directory.path().join(PRIVKEY_FILENAME),
        key.serialize_pem(),
    )
    .unwrap();
    let selection = CertificateSource::Path("server.pem".into());
    let identity =
        source.identity(&selection, CertificateKind::Server).await.unwrap();
    assert_eq!(identity.cert.len(), 1);
    assert_eq!(
        identity.cert[0].as_ref(),
        parsers::concat_chain_bytes_from_pem(&server).unwrap()
    );
    assert!(source.identity(&selection, CertificateKind::Ca).await.is_err());
    let ca_selection = CertificateSource::Path("ca.pem".into());
    let error = source
        .identity(&ca_selection, CertificateKind::Server)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("explicit signing capability"));
    std::fs::remove_file(directory.path().join("server.pem")).unwrap();
    // Explicit paths and partial material must not silently fall back to hosting.
    assert!(
        source.identity(&selection, CertificateKind::Server).await.is_err()
    );
}

#[tokio::test]
async fn configured_file_authority_signs_with_its_intermediate_key() {
    let directory = tempfile::tempdir().unwrap();
    let root_key = generators::generate_ca_key().unwrap().serialize_pem();
    let root = generators::generate_ca_cert(
        "Root CA",
        root_key.as_bytes(),
        30,
        None,
        None,
    )
    .unwrap();
    let key = generators::generate_ca_key().unwrap().serialize_pem();
    let intermediate = generators::generate_intermediate_ca_cert(
        key.as_bytes(),
        &root,
        root_key.as_bytes(),
        "Intermediate CA",
        7,
        &Default::default(),
        Some(0),
    )
    .unwrap();
    let path = |name| directory.path().join(name);
    std::fs::write(path("root.pem"), &root).unwrap();
    std::fs::write(path("intermediate.pem"), &intermediate).unwrap();
    std::fs::write(path("signer.key"), &key).unwrap();
    let files: CertStore = serde_yaml::from_str(&format!(
        "dir: {}\nauthority:\n  cert_path: {}\n  key_path: {}\n  ca_path: {}",
        directory.path().display(),
        path("intermediate.pem").display(),
        path("signer.key").display(),
        path("root.pem").display(),
    ))
    .unwrap();
    let source = CertificateStore::new(files);
    let authority = source.authority().await.unwrap().unwrap();
    assert_eq!(authority.roots().len(), 2);
    let signer = source.signer(&authority).await.unwrap();
    assert_eq!(signer.issuer_id(), None);
    let device = generators::generate_private_key().unwrap();
    let csr = generators::generate_server_csr(
        &device,
        "device.test",
        Vec::new(),
        &Default::default(),
    )
    .unwrap();
    let issued = signer
        .issue_client_csr(csr.der().as_ref(), "device", None, None, 1)
        .unwrap();
    let leaf = parsers::take_first_cert_from_der(&issued).unwrap();
    let issuer =
        parsers::take_first_cert_from_der(&issued[leaf.as_ref().len()..])
            .unwrap();
    leaf.verify_signature(Some(issuer.public_key())).unwrap();
    assert_eq!(issuer.as_ref(), authority.roots()[0].as_ref());
    assert_eq!(issued.len(), leaf.as_ref().len() + issuer.as_ref().len());

    std::fs::write(path("signer.key"), root_key).unwrap();
    assert!(source.signer(&authority).await.is_err());
}

#[test]
fn source_registration_does_not_register_sni_or_edge() {
    let state = AppState::standalone("source-fixture", ".");
    register(&state).unwrap();
    register(&state).unwrap();
    assert!(state.registry_ref().resolve::<CertificateProvider>().is_some());
    assert!(
        state.registry_ref().resolve::<crate::CertResolverProvider>().is_none()
    );
    assert!(state.registry_ref().resolve::<crate::CertResolver>().is_none());
}

#[cfg(feature = "db")]
#[tokio::test]
async fn public_authority_never_queries_private_key_column() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("fixtures/public.sql"))
        .execute(&pool)
        .await
        .unwrap();
    let key = generators::generate_ca_key().unwrap();
    let ca = generators::generate_ca_cert(
        "Fixture CA",
        key.serialize_pem().as_bytes(),
        30,
        None,
        None,
    )
    .unwrap();
    let der = parsers::concat_chain_bytes_from_pem(&ca).unwrap();
    sqlx::query("INSERT INTO hosting_certificates (id, kind, issuer_id, cn, certs_der, expires_at, issued_at, updated_at, enabled) VALUES (1, 'ca', NULL, 'Fixture CA', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL, 1)")
        .bind(&der).execute(&pool).await.unwrap();
    let store = claviron_hosting::HostingStore::sqlite(pool.clone());
    let authority =
        CertAuthority::from_issuer(&store, "Fixture CA").await.unwrap();
    assert_eq!(authority.roots()[0].as_ref(), der);
    assert_eq!(authority.issuer_id(), Some(1));
    assert!(authority.path().is_none());
    authority.validate_issuer(&store).await.unwrap();
    // Issuer identity is its persisted ID; renaming it does not rotate trust.
    sqlx::query("UPDATE hosting_certificate_issuers SET name = 'Renamed CA'")
        .execute(&pool)
        .await
        .unwrap();
    authority.validate_issuer(&store).await.unwrap();
    assert_eq!(
        CertAuthority::from_cn(&store, "Fixture CA")
            .await
            .unwrap()
            .issuer_name(),
        Some("Renamed CA")
    );
    assert!(CertAuthority::from_cn(&store, "Renamed CA").await.is_err());
    // The fixture deliberately has no private-key column. Public reads work,
    // while explicit signing cannot obtain material that was never provided.
    assert!(CertSigner::from_authority(&store, &authority).await.is_err());
    sqlx::query("ALTER TABLE hosting_certificates ADD COLUMN key_der BLOB")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hosting_certificates SET key_der = ?")
        .bind(key.serialize_der())
        .execute(&pool)
        .await
        .unwrap();
    let signer = CertSigner::from_authority(&store, &authority).await.unwrap();
    let issued = signer
        .issue_certificate(CertificateKind::Client, "client.test", &[], 1, None)
        .unwrap();
    assert!(!issued.key_der.is_empty());
    assert!(
        store
            .certificate_by_cn_and_kind("Fixture CA", CertificateKind::Client)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE hosting_certificates SET enabled = 0")
        .execute(&pool)
        .await
        .unwrap();
    assert!(authority.validate_issuer(&store).await.is_err());
    assert!(CertAuthority::from_issuer(&store, "Renamed CA").await.is_err());
    assert!(CertAuthority::from_cn(&store, "Fixture CA").await.is_err());
    assert!(signer.validate_issuer(&store).await.is_err());
}
