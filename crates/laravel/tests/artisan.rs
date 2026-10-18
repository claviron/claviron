use super::*;

#[test]
fn only_the_local_artisan_entry_is_required() {
    let root = tempfile::tempdir().unwrap();
    let error = validate_project(root.path()).unwrap_err();
    assert!(error.contains("No Laravel application found here"));
    assert!(!error.contains("os error"));

    std::fs::create_dir(root.path().join("artisan")).unwrap();
    assert!(validate_project(root.path()).is_err());
    std::fs::remove_dir(root.path().join("artisan")).unwrap();
    std::fs::write(root.path().join("artisan"), "<?php").unwrap();
    validate_project(root.path()).unwrap();
}

#[test]
fn command_preserves_arguments_environment_and_cancellation() {
    let token = CancellationToken::new();
    let args = vec!["example:run".into(), "literal argument; $VALUE".into()];
    let environment = BTreeMap::from([
        ("APP_PROFILE".into(), "example".into()),
        ("APP_RUNNING_IN_CONSOLE".into(), "false".into()),
    ]);
    let command = command(
        PathBuf::from("example-app"),
        args.clone(),
        environment,
        token.clone(),
    );
    assert_eq!(command.cwd, PathBuf::from("example-app"));
    assert_eq!(command.script, PathBuf::from("artisan"));
    assert_eq!(command.args, args);
    assert_eq!(command.environment["APP_PROFILE"], "example");
    assert_eq!(command.environment["APP_RUNNING_IN_CONSOLE"], "true");
    token.cancel();
    assert!(command.cancellation.is_cancelled());
}
