use crate::edge::tcp::plain::HttpServerConfig;
#[cfg(feature = "tls")]
use crate::edge::tcp::secure::HttpsServerConfig;

#[test]
fn sibling_transport_changes_do_not_invalidate_cleartext() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/edge/tcp/fixtures/listeners.yaml"
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
        "/tests/edge/tcp/fixtures/listeners.yaml"
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

#[cfg(feature = "tls")]
#[test]
fn listener_execution_changes_invalidate_only_their_transport_projection() {
    let source = include_str!("../fixtures/listeners.yaml");
    let plain = serde_yaml::from_str::<HttpServerConfig>(source).unwrap();
    let secure = serde_yaml::from_str::<HttpsServerConfig>(source).unwrap();
    for (name, changes_plain, changes_secure) in
        [("plain", true, false), ("secure", false, true)]
    {
        let changed = source.replace(
            &format!("  {name}:\n"),
            &format!("  {name}:\n    execution: current_thread\n"),
        );
        assert_eq!(
            plain
                != serde_yaml::from_str::<HttpServerConfig>(&changed).unwrap(),
            changes_plain
        );
        assert_eq!(
            secure
                != serde_yaml::from_str::<HttpsServerConfig>(&changed).unwrap(),
            changes_secure
        );
    }
}
