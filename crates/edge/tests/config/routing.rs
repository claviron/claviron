use super::*;

#[test]
fn flat_root_directives_keep_file_fields_out_of_extensions() {
    let config: HostConfig = serde_yaml::from_str(
        "root: .\nindex: [index.html]\ntry_files: true\nspa: home.html\najax_only: true\nstatics:\n  - path: /assets\n    dir: assets\n",
    ).unwrap();
    let root = config.routes.find("/").unwrap();
    assert_eq!(root.extensions.len(), 1);
    assert_eq!(root.extensions["ajax_only"], Value::Bool(true));
    assert_eq!(root.index, ["index.html"]);
    assert!(root.try_files);
    assert_eq!(
        root.spa.as_ref().unwrap().entry,
        std::path::Path::new("home.html")
    );
    assert_eq!(root.statics.as_ref().unwrap().len(), 1);
    assert!(!root.selected_static);
}

#[test]
#[cfg(feature = "php_embed")]
fn flat_root_php_remains_a_builtin_owner() {
    let config: HostConfig = serde_yaml::from_str(
        "root: .\nindex: [index.php]\nphp:\n  runner: script\n  entry_script: index.php\najax_only: true\n",
    ).unwrap();
    let root = config.routes.find("/").unwrap();
    assert_eq!(root.extensions.len(), 1);
    assert!(root.owns_handler);
    assert_eq!(root.php.as_ref().unwrap().len(), 1);
    assert_eq!(root.index, ["index.php"]);
}
#[cfg(not(feature = "rate-limit"))]
#[test]
fn disabled_rate_limit_is_rejected_instead_of_silently_ignored() {
    for source in [
        "root: public\nrate_limit: {}\n",
        "root: public\nroutes:\n  /api:\n    rate_limit: {}\n",
    ] {
        let error = serde_yaml::from_str::<crate::config::HostConfig>(source)
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("rate-limit")
                || error.to_string().contains("not enabled")
        );
    }
}
