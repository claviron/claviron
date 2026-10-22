use std::path::PathBuf;

use super::*;

/// What a process without hosting hands certificate loading.
fn hosting(state: &AppState) -> HostingDeps {
    use claviron_runtime::registry::Resolve;
    HostingDeps::resolve(state.registry_ref()).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn empty_default_declaration_selects_the_conventional_identity() {
    use claviron_types::ssl::certstore::CertificateSource;
    for declaration in ["", "default:", "default: null", "default_cert: null"] {
        let yaml =
            format!("certstore: {{dir: ., authority: root}}\n{declaration}\n");
        let config = parse_tls_config(yaml).await.unwrap();
        assert!(matches!(
            config.default_cert_entry(),
            CertificateSource::Name(name) if name.as_str() == "default"
        ));
    }
    for declaration in
        ["default: []", "default: {cn: ''}", "default: {path: ''}"]
    {
        let yaml = format!("certstore: {{dir: .}}\n{declaration}\n");
        assert!(parse_tls_config(yaml).await.is_err());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn named_authority_preserves_ca_identity_without_relaxing_hosts() {
    use claviron_types::ssl::certstore::AuthorityEntry;
    let yaml = "certstore: {dir: ., authority: {cn: 'Roma CA'}}\ndefault: server.test\n";
    let config = parse_tls_config(yaml.to_owned()).await.unwrap();
    let Some(AuthorityEntry::Cn(name)) = config.certstore().authority_entry()
    else {
        panic!("expected named authority");
    };
    assert_eq!(name.as_str(), "Roma CA");
    assert!(config.certstore().authority_cert_path().is_none());
    let state = AppState::standalone("authority-config-test", ".");
    let error = load_authority(&hosting(&state), &config).await.err().unwrap();
    #[cfg(feature = "db")]
    assert!(error.to_string().contains("requires HostingService"));
    #[cfg(not(feature = "db"))]
    assert!(error.to_string().contains("requires database support"));
    assert!(
        parse_tls_config(yaml.replace("server.test", "Roma CA")).await.is_ok()
    );
    assert!(
        parse_tls_config(yaml.replace("server.test", "../escape"))
            .await
            .is_err()
    );
    assert!(parse_tls_config(yaml.replace("'Roma CA'", "' '")).await.is_err());
    let pem = "certstore: {dir: certs, authority: {path: ca.pem}}\ndefault: server.test\n";
    let config = parse_tls_config(pem.to_owned()).await.unwrap();
    assert!(matches!(
        config.certstore().authority_entry(),
        Some(AuthorityEntry::Path(_))
    ));
    assert_eq!(
        config.certstore().authority_cert_path(),
        Some(PathBuf::from("certs/ca.pem"))
    );
    let absolute = std::env::temp_dir().join("authority-test.pem");
    let config =
        parse_tls_config(pem.replace("ca.pem", absolute.to_str().unwrap()))
            .await
            .unwrap();
    assert_eq!(config.certstore().authority_cert_path(), Some(absolute));
    for declaration in
        ["{cn: 'Roma CA', path: ca.pem}", "{}", "{path: ''}", "{file: ca.pem}"]
    {
        let yaml = format!(
            "certstore: {{dir: ., authority: {declaration}}}\ndefault: server.test\n"
        );
        assert!(parse_tls_config(yaml).await.is_err());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_config_resolution_keeps_store_errors_and_valid_config() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/certstore.yaml");
    let yaml = format!(
        "certstore: {}default: localhost\n",
        serde_yaml::to_string(&path).unwrap()
    );
    let config = parse_tls_config(yaml).await.expect("resolved config");
    assert_eq!(
        config.certstore().dir(),
        &path.parent().unwrap().join("certificates")
    );

    let yaml = format!(
        "certstore: {}default: localhost\n",
        serde_yaml::to_string(&path.parent().unwrap()).unwrap()
    );
    let error = parse_tls_config(yaml).await.err().expect("store read error");
    assert!(matches!(error, Error::Other(_)));
    assert!(matches!(
        parse_tls_config("default: [".to_owned()).await,
        Err(Error::Other(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn failed_reload_file_snapshot_keeps_serving_certificates() {
    #[derive(Debug)]
    struct UnusedSigningKey;
    impl rustls::sign::SigningKey for UnusedSigningKey {
        fn choose_scheme(
            &self,
            _: &[rustls::SignatureScheme],
        ) -> Option<Box<dyn rustls::sign::Signer>> {
            None
        }
        fn algorithm(&self) -> rustls::SignatureAlgorithm {
            rustls::SignatureAlgorithm::ECDSA
        }
    }
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        Directory(std::env::temp_dir().join(format!(
            "claviron-tls-reload-{}-{unique}",
            std::process::id()
        )));
    std::fs::create_dir(&directory.0).unwrap();
    // The shared name source loads files before consulting hosting, including
    // the reserved default identity, which is not a DNS Host.
    use claviron_types::ssl::certstore::{
        CA_CERT_FILENAME, CertificateSource, FULLCHAIN_CERT_FILENAME,
        PRIVKEY_FILENAME,
    };
    use claviron_types::ssl::generators;
    let key = generators::generate_ca_key().unwrap();
    let pem = generators::generate_ca_cert(
        "Fixture CA",
        key.serialize_pem().as_bytes(),
        1,
        None,
        None,
    )
    .unwrap();
    let server_dir = directory.0.join("servers/default");
    let ca_dir = directory.0.join("ca/Fixture CA");
    std::fs::create_dir_all(&server_dir).unwrap();
    std::fs::create_dir_all(&ca_dir).unwrap();
    std::fs::write(server_dir.join(FULLCHAIN_CERT_FILENAME), &pem).unwrap();
    std::fs::write(server_dir.join(PRIVKEY_FILENAME), key.serialize_pem())
        .unwrap();
    std::fs::write(ca_dir.join(CA_CERT_FILENAME), &pem).unwrap();
    let yaml = format!(
        "certstore: {{dir: {}, authority: 'Fixture CA'}}",
        directory.0.display()
    );
    let config = parse_tls_config(yaml.clone()).await.unwrap();
    assert!(
        matches!(config.default_cert_entry(), CertificateSource::Name(name) if name.as_str() == "default")
    );
    let file_state =
        AppState::standalone("tls-file-first-fixture", &directory.0);
    assert!(
        load_authority(&hosting(&file_state), &config).await.unwrap().is_some()
    );
    let source = config.default_cert_entry().clone();
    let mut snapshot = load_file_snapshot(config).await.unwrap();
    load_default_from_database(&hosting(&file_state), &mut snapshot, &source)
        .await
        .unwrap();
    assert!(snapshot.default_cert().is_some());
    std::fs::write(server_dir.join(FULLCHAIN_CERT_FILENAME), "broken PEM")
        .unwrap();
    assert!(
        load_file_snapshot(parse_tls_config(yaml.clone()).await.unwrap())
            .await
            .is_err()
    );
    std::fs::remove_file(server_dir.join(FULLCHAIN_CERT_FILENAME)).unwrap();
    assert!(
        load_file_snapshot(parse_tls_config(yaml.clone()).await.unwrap())
            .await
            .is_err()
    );
    std::fs::remove_file(server_dir.join(PRIVKEY_FILENAME)).unwrap();
    let mut snapshot =
        load_file_snapshot(parse_tls_config(yaml).await.unwrap())
            .await
            .unwrap();
    assert!(
        load_default_from_database(
            &hosting(&file_state),
            &mut snapshot,
            &source
        )
        .await
        .is_err()
    );
    // A directory cannot be read as a PEM certificate: fail after config resolution.
    let path = serde_yaml::to_string(&directory.0).unwrap();
    std::fs::write(
        directory.0.join("tls.yaml"),
        format!("certstore: {{dir: .}}\ndefault_cert:\n  cert_path: {path}  key_path: {path}")
    )
    .unwrap();

    // Never used for a handshake; its Arc identity proves failed publication is inert.
    let old = Arc::new(rustls::sign::CertifiedKey::new(
        vec![],
        Arc::new(UnusedSigningKey),
    ));
    let resolver = CertResolver::new(
        std::collections::HashMap::from([(
            "serving.example".to_owned(),
            old.clone(),
        )]),
        old.clone(),
    );
    let state = AppState::standalone("tls-reload-fixture", &directory.0);
    // The reload takes the declaration Congif prepared, as the runtime's
    // reload batch would have.
    let owner = CertificateProvider::new(&state);
    state.config_reader().prepare_owners(&[&owner]).await.unwrap();
    let error = reload_resolver_certs(&resolver, &state)
        .await
        .expect_err("PEM read failure");
    assert!(error.to_string().contains("certificate error"), "{error}");
    assert!(Arc::ptr_eq(&old, &resolver.get_default()));
    assert!(Arc::ptr_eq(&old, &resolver.get_cert("serving.example").unwrap()));
    assert_eq!(resolver.count(), 1);
}
