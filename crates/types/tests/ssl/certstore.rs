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
    assert_ne!(store.client_dir("operator"), store.ca_dir("operator"));
}
