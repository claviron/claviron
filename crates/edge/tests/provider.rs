#![cfg(feature = "server")]

use std::path::PathBuf;
use std::sync::Arc;

use claviron_edge::HttpConfigProvider;
use claviron_http::config::ListenerCapabilities;

#[test]
fn unavailable_transports_never_replace_the_active_snapshot() {
    for capabilities in [
        ListenerCapabilities { tls: false, h3: false },
        ListenerCapabilities { tls: true, h3: false },
        ListenerCapabilities { tls: false, h3: true },
        ListenerCapabilities::ALL,
    ] {
        let provider = HttpConfigProvider::with_capabilities(
            PathBuf::from("unused-http.yaml"),
            capabilities,
        );
        provider.replace_from_yaml("listeners:\n  local:\n    transport: tcp\n    listen: 127.0.0.1:8080\n    protocols: h1\n    requests: {max_body_size: off, body_read_timeout: off}").unwrap();
        for (transport, expected) in [
            (
                "transport: tcp\n    tls: true\n    protocols: [h1, h2]",
                capabilities.tls,
            ),
            (
                "transport: quic\n    protocols: h3",
                capabilities.tls && capabilities.h3,
            ),
        ] {
            let active = provider.snapshot();
            let candidate = format!(
                "listeners:\n  secure:\n    listen: 127.0.0.1:443\n    {transport}\n    requests: {{max_body_size: off, body_read_timeout: off}}"
            );
            let result = provider.replace_from_yaml(&candidate);
            assert_eq!(
                result.is_ok(),
                expected,
                "{capabilities:?}: {result:?}"
            );
            if !expected {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("listener 'secure'")
                );
                assert!(Arc::ptr_eq(&active, &provider.snapshot()));
            }
        }
    }
}

#[test]
fn failed_candidate_keeps_the_active_snapshot() {
    let provider = HttpConfigProvider::empty(PathBuf::from("unused-http.yaml"));
    provider.replace_from_yaml("listeners:\n  local:\n    transport: tcp\n    listen: 127.0.0.1:8080\n    protocols: h1\n    requests: {max_body_size: off, body_read_timeout: off}").unwrap();
    let active = provider.snapshot();
    for invalid in [
        "listeners:\n  bad/name: {}",
        "listeners:\n  invalid:\n    transport: quic\n    tls: false\n    listen: 127.0.0.1:443\n    protocols: h3",
    ] {
        assert!(provider.replace_from_yaml(invalid).is_err());
        assert!(Arc::ptr_eq(&active, &provider.snapshot()));
    }
    provider.replace_from_yaml("{}").unwrap();
    assert!(provider.snapshot().listeners.is_empty());
    assert_eq!(
        active.listeners.len(),
        1,
        "existing consumers retain their generation"
    );
}

#[tokio::test]
async fn http_configuration_preparation_compares_values_and_retains_valid_candidates()
 {
    use claviron_runtime::congif::{ConfigReader, ConfigRegistration};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("http.yaml");
    let fixtures =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/http");
    std::fs::copy(fixtures.join("base.yaml"), &path).unwrap();
    let provider = HttpConfigProvider::empty(path.clone());
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let owners: [&dyn ConfigRegistration; 1] = [&provider];
    reader.initialize_owners(&owners).await.unwrap();
    let initial = reader.snapshot::<HttpConfigProvider>().unwrap();
    // Preparation is pure: runtime publication is a separate lifecycle step.
    assert!(provider.snapshot().listeners.is_empty());
    reader.configure_registered(&provider).await.unwrap();
    assert!(*initial == *provider.snapshot());
    assert!(reader.prepare_changed_owners(&owners).await.unwrap().is_empty());
    assert!(*initial == *reader.snapshot::<HttpConfigProvider>().unwrap());
    let source = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("# formatting-only change\n{source}\n"))
        .unwrap();
    assert!(reader.prepare_changed_owners(&owners).await.unwrap().is_empty());

    std::fs::copy(fixtures.join("changed.yaml"), &path).unwrap();
    assert_eq!(reader.prepare_changed_owners(&owners).await.unwrap().len(), 1);
    let changed = reader.snapshot::<HttpConfigProvider>().unwrap();
    assert!(*initial != *changed);
    assert!(reader.prepared_config(&provider).is_some());
    std::fs::copy(fixtures.join("invalid.yaml"), &path).unwrap();
    assert!(reader.prepare_owners(&owners).await.is_err());
    assert!(Arc::ptr_eq(
        &changed,
        &reader.snapshot::<HttpConfigProvider>().unwrap()
    ));
}
