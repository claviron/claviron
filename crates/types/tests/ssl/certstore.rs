use super::*;

#[test]
fn client_material_uses_the_client_subtree_and_existing_key_filename() {
    let store: CertStore =
        serde_yaml::from_str("dir: /tmp/certstore-test\n").unwrap();
    assert_eq!(
        store.client_dir("operator"),
        Path::new("/tmp/certstore-test/clients/operator")
    );
    assert_eq!(
        store.client_key_path("operator"),
        store.client_dir("operator").join(CLIENT_KEY_FILENAME)
    );
    assert_eq!(
        store.client_cert_path("operator"),
        store.client_dir("operator").join(FULLCHAIN_CERT_FILENAME)
    );
    let operator = Basename::new("operator").unwrap();
    assert_ne!(store.client_dir("operator"), store.ca_dir(operator));
}

#[test]
fn only_a_basename_selects_a_store_folder() {
    let source = |yaml: &str| serde_yaml::from_str::<CertificateSource>(yaml);
    let named = source("fixture").unwrap();
    assert_eq!(named.basename(), Basename::new("fixture"));
    assert_eq!(source("local: fixture").unwrap().basename(), named.basename());
    // A bare name that is not a basename stays a name for hosting; it never
    // becomes a path, whatever it spells.
    for name in ["'Fixture CA'", "server.test", "../escape"] {
        let source = source(name).unwrap();
        assert!(matches!(source, CertificateSource::Name(_)), "{name}");
        assert_eq!(source.basename(), None, "{name}");
    }
    assert_eq!(source("cn: fixture").unwrap().basename(), None);
    for local in ["'Fixture CA'", "../escape", "''"] {
        assert!(source(&format!("local: {local}")).is_err(), "{local}");
    }
}
