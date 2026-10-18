use super::*;

fn context() -> EnvironmentContext {
    let store = dotenw::EnvStore::from_base(dotenw::Snapshot::new(
        [
            ("BASE_VALUE".into(), "inherited".into()),
            ("SECRET".into(), "host-only".into()),
        ]
        .into(),
    ))
    .unwrap()
    .with_environment("test")
    .unwrap();
    EnvironmentContext { store, base: dotenw::Environment::new().detached() }
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/program.env")
}

#[test]
fn execution_recipe_resolves_variants_then_inline_without_exposing_the_base() {
    let context = context();
    let recipe = context
        .recipe(&[fixture()], [("OVERRIDE".into(), "inline".into())])
        .unwrap();
    let first = context.store.read(recipe.clone()).unwrap();
    let second = context.store.read(recipe).unwrap();
    assert_eq!(first.as_ref(), second.as_ref());
    assert_eq!(first.get("RESOLVED").map(String::as_str), Some("inherited"));
    assert_eq!(first.get("SELECTED").map(String::as_str), Some("test"));
    assert_eq!(first.get("OVERRIDE").map(String::as_str), Some("inline"));
    assert!(!first.contains_key("SECRET"));
    assert!(!first.contains_key("BASE_VALUE"));
}

#[test]
fn required_file_and_undefined_inline_variable_are_rejected() {
    let context = context();
    let missing = fixture().with_file_name("missing.env");
    assert!(context.recipe(&[missing], []).is_err());
    let recipe = context
        .recipe(&[], [("VALUE".into(), "${MISSING_KEY}".into())])
        .unwrap();
    let error = context.store.read(recipe).unwrap_err();
    assert!(error.to_string().contains("MISSING_KEY"));
}

#[test]
fn an_empty_application_set_parks_instead_of_spinning() {
    let environments = ApplicationEnvironments::default();
    assert!(environments.changed().now_or_never().is_none());
    assert!(environments.snapshot("missing").is_err());
}
