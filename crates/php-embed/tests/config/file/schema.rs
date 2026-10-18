use super::*;

#[test]
fn event_backlog_is_configurable_and_invalid_bounds_are_rejected() {
    let config = parse("sapi: null\nevents: {capacity: 128}\n", |_| None)
        .unwrap()
        .resolve()
        .unwrap();
    assert_eq!(config.events.capacity, 128);
    assert!(
        parse("sapi: null\nevents: {capacity: 0}\n", |_| None)
            .unwrap()
            .resolve()
            .is_err()
    );
    assert!(parse("sapi: null\nevents: {capacitty: 128}\n", |_| None).is_err());
    assert!(crate::PhpEventConfig { capacity: usize::MAX }.validate().is_err());
}

#[test]
fn single_application_uses_the_embed_schema_without_a_second_name_or_pool() {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/config/program.php");
    let raw = format!(
        "application: {{script_path: {}}}\n",
        serde_yaml::to_string(&script).unwrap().trim()
    );
    let config = parse_application(&raw, "example-app", |_| None).unwrap();
    let resolved = config.resolve().unwrap();
    assert!(resolved.sapi.is_none());
    assert_eq!(resolved.applications.len(), 1);
    assert_eq!(resolved.applications[0].name, "example-app");
    assert!(
        parse_application(
            "applications: {}\napplication: {}\n",
            "example",
            |_| None
        )
        .is_err()
    );
    assert!(
        parse_application("application: {unknown: true}\n", "example", |_| {
            None
        })
        .is_err()
    );
}
