use super::*;

#[test]
fn file_routes_flat_root_directives_keep_file_fields_out_of_extensions() {
    let config: HostConfig = serde_yaml::from_str(
        "root: .\nindex: [index.html]\ntry_files: true\nspa: home.html\najax_only: true\nroutes:\n  /assets:\n    static: {}\n",
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
    assert!(root.statics.is_none());
    assert_eq!(config.statics.as_ref().unwrap().len(), 1);
    assert_eq!(
        config.routes.find("/assets").unwrap().statics.as_ref().unwrap()[0].dir,
        std::env::current_dir().unwrap().join("assets")
    );
}

#[test]
fn file_routes_reject_duplicate_roots_and_mount_lists() {
    for source in [
        "root: public\nstatics: [{path: /assets, dir: assets}]",
        "root: public\nstatic: {dir: assets}",
        "routes: {'/': {root: public}}",
        "root: public\nroutes: {'/assets': {root: other}}",
        "root: public\nroutes: {'/': {statics: [{path: /assets, dir: assets}]}}",
        "root: public\nroutes: {'/assets': {static: {dir: assets, path: /other}}}",
        "root: public\nroutes: {'/assets': {static: {dir: assets, root: other}}}",
        "root: public\nroutes: {'/assets': {static: {dir: other}}}",
        "root: public\nroutes: {'/': {statics: [{dir: ../outside}]}}",
        "root: public\nroutes: {'/': {statics: [{dir: /outside}]}}",
        "root: public\nroutes: {'/': {statics: [{dir: assets}, {dir: assets}]}}",
    ] {
        assert!(
            serde_yaml::from_str::<HostConfig>(source).is_err(),
            "{source}"
        );
    }
}

#[test]
fn file_routes_directory_groups_keep_one_route_and_relative_policy_paths() {
    let config: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /frontend:\n    path: shared\n    statics:\n      - dir: assets\n        rules: [{test: '^app\\.js$'}]\n      - dir: icons\n",
    ).unwrap();
    assert_eq!(config.routes.iter().count(), 1);
    let route = config.routes.find("/frontend/assets/app.js").unwrap();
    assert!(!route.owns_handler);
    assert!(route.extensions.is_empty());
    let mounts = route.statics.as_ref().unwrap();
    assert_eq!(mounts.len(), 2);
    assert_eq!(mounts[0].path.as_str(), "/frontend/assets");
    assert_eq!(
        mounts[0].dir,
        std::env::current_dir().unwrap().join("public/shared/assets")
    );
    assert_eq!(mounts[1].path.as_str(), "/frontend/icons");
}

#[test]
fn file_routes_policy_child_keeps_the_parent_policy_and_mapped_subdirectory() {
    let config: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /files:\n    path: shared\n    static: {none_found: true}\n  /files/private:\n    cors: disabled\n",
    ).unwrap();
    let child = config.routes.find("/files/private").unwrap();
    assert_eq!(child.files.root, std::path::Path::new("public"));
    let mount = &child.statics.as_ref().unwrap()[0];
    assert_eq!(mount.path.as_str(), "/files/private");
    assert_eq!(
        mount.dir,
        std::env::current_dir().unwrap().join("public/shared/private")
    );
    assert!(mount.none_found);
    assert_eq!(config.statics.as_ref().unwrap().len(), 1);
}

#[cfg(feature = "upstream")]
#[test]
fn file_routes_static_declarations_preserve_upstream_owners() {
    let config: HostConfig = serde_yaml::from_str(
        "root: public\nupstream: backend\nroutes:\n  /assets:\n    path: build\n    static: {}\n  /assets/private:\n    cors: disabled\n",
    ).unwrap();
    assert_eq!(config.upstream_name(), Some("backend"));
    assert_eq!(config.statics.as_ref().unwrap()[0].path.as_str(), "/assets");
    let config: HostConfig = serde_yaml::from_str(
        "root: public\nupstream: backend\nroutes:\n  /other:\n    upstream: other-backend\n",
    ).unwrap();
    assert_eq!(config.routes.find("/other").unwrap().upstream_name(), Some("other-backend"));
}

#[test]
#[cfg(feature = "php_embed")]
fn file_routes_flat_root_php_remains_a_builtin_owner() {
    let config: HostConfig = serde_yaml::from_str(
        "root: .\nindex: [index.php]\nphp:\n  runner: script\n  entry_script: index.php\najax_only: true\n",
    ).unwrap();
    let root = config.routes.find("/").unwrap();
    assert_eq!(root.extensions.len(), 1);
    assert!(root.owns_handler);
    assert_eq!(root.php.as_ref().unwrap().len(), 1);
    assert_eq!(root.index, ["index.php"]);
}

#[cfg(feature = "php")]
#[test]
fn file_routes_php_preparation_uses_the_same_mapping_as_files() {
    let directory = tempfile::tempdir().unwrap();
    let config: HostConfig = serde_yaml::from_str(&format!(
        "root: {}\nroutes:\n  /admin:\n    path: mapped\n    php: {{runner: script, entry_script: index.php}}\n  /plain:\n    php: {{runner: script}}\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    )).unwrap();
    let state =
        claviron_runtime::AppState::standalone("php-mapping", directory.path());
    for (prefix, disk) in [("/admin", "mapped"), ("/plain", "plain")] {
        let route = config.routes.find(prefix).unwrap();
        let runtime = route.build_runtimes(&state).pop().unwrap();
        assert_eq!(runtime.dir, directory.path().join(disk));
        assert_eq!(
            runtime.dir,
            route.files.mapping.as_ref().unwrap().directory()
        );
        assert_eq!(
            runtime.relative_path(&format!("{prefix}/index.php")),
            Some("index.php")
        );
        assert_eq!(
            runtime.script_relative_path("/index.php", true),
            Some("index.php")
        );
    }
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
