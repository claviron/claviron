use crate::plain::HttpServerConfig;
#[cfg(feature = "tls")]
use crate::secure::HttpsServerConfig;

#[test]
fn sibling_transport_changes_do_not_invalidate_cleartext() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/listeners.yaml"
    ))
    .unwrap();
    let original = serde_yaml::from_str::<HttpServerConfig>(&source);
    if !cfg!(feature = "tls") {
        assert!(
            original.is_err(),
            "unsupported TLS must fail before projection"
        );
        return;
    }
    let original = original.unwrap();
    let changed = serde_yaml::from_str::<HttpServerConfig>(
        &source.replace("8443", "9443"),
    )
    .unwrap();
    assert!(original == changed);
    let changed = serde_yaml::from_str::<HttpServerConfig>(
        &source.replace("8080", "9080"),
    )
    .unwrap();
    assert!(original != changed);
}

#[cfg(feature = "tls")]
#[test]
fn sibling_transport_changes_do_not_invalidate_tls() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/listeners.yaml"
    ))
    .unwrap();
    let original = serde_yaml::from_str::<HttpsServerConfig>(&source).unwrap();
    let changed = serde_yaml::from_str::<HttpsServerConfig>(
        &source.replace("8080", "9080"),
    )
    .unwrap();
    assert!(original == changed);
    let changed = serde_yaml::from_str::<HttpsServerConfig>(
        &source.replace("8443", "9443"),
    )
    .unwrap();
    assert!(original != changed);
}
