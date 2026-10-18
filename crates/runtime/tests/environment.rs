use super::*;

fn base() -> dotenw::Snapshot {
    dotenw::Snapshot::new(HashMap::from([
        ("APPLICATION_MODE".into(), "inherited".into()),
        ("INHERITED_LITERAL".into(), "${KEEP_LITERAL}".into()),
    ]))
}

fn env_file() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/environment/application.env")
}

#[test]
fn deployment_selection_overrides_common_values_without_reexpanding_base() {
    for (selection, expected) in [
        ("production", "production"),
        ("preview", "preview"),
        ("test", "common"),
    ] {
        let environment = ConfigEnvironment::from_snapshot(
            base(),
            selection,
            Some(&env_file()),
        )
        .unwrap();
        assert_eq!(environment.environment(), selection);
        assert_eq!(
            environment.snapshot().get("APPLICATION_MODE").as_deref(),
            Some(expected)
        );
        assert_eq!(
            environment.snapshot().get("INHERITED_LITERAL").as_deref(),
            Some("${KEEP_LITERAL}")
        );
        assert_eq!(
            environment
                .handle
                .snapshot()
                .unwrap()
                .get("APPLICATION_MODE")
                .map(String::as_str),
            Some(expected)
        );
    }
}

#[test]
fn unsafe_environment_path_names_are_rejected() {
    for selection in [
        "",
        "../production",
        "production/local",
        "production.local",
        "two words",
    ] {
        assert!(
            ConfigEnvironment::from_snapshot(base(), selection, None).is_err()
        );
    }
}

#[test]
fn configless_environment_preserves_the_inherited_snapshot() {
    let environment =
        ConfigEnvironment::from_snapshot(base(), "production", None).unwrap();
    assert_eq!(
        environment.snapshot().get("APPLICATION_MODE").as_deref(),
        Some("inherited")
    );
    assert!(environment.handle.sources().unwrap().is_empty());
}
