use super::*;

#[cfg(feature = "orbit")]
#[test]
fn link_lifecycle_destinations_include_flat_and_scoped_routes() {
    let config: HostConfig = serde_yaml::from_str(
        "orbit: {fleet_name: first, application: root}\nroutes:\n  /api:\n    orbit: {fleet_name: second, application: api}\n  /assets:\n    static: {}\n",
    ).unwrap();
    let destinations: Vec<_> = config.link_routes()
        .map(|route| (route.fleet_name.as_str(), route.application.as_str()))
        .collect();
    assert_eq!(destinations, [("first", "root"), ("second", "api")]);
}

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
        "root: public\nroutes:\n  /files:\n    path: shared\n    static:\n      none_found: true\n      rules: [{test: '^private/app\\.js$'}]\n  /files/private:\n    cors: disabled\n",
    ).unwrap();
    let child = config.routes.find("/files/private").unwrap();
    assert_eq!(child.files.root, std::path::Path::new("public"));
    assert_eq!(
        child.files.mapping.as_ref().unwrap().directory(),
        std::env::current_dir().unwrap().join("public/shared/private")
    );
    let mount = &child.statics.as_ref().unwrap()[0];
    assert_eq!(mount.path.as_str(), "/files");
    assert_eq!(
        mount.dir,
        std::env::current_dir().unwrap().join("public/shared")
    );
    let mapping = RouteMapping::new(mount.path.clone(), mount.dir.clone());
    let relative = mapping.relative_path("/files/private/app.js").unwrap();
    assert_eq!(relative, "private/app.js");
    assert!(mount.rules.as_ref().unwrap()[0].test.is_match(relative));
    assert!(mount.none_found);
    assert_eq!(config.statics.as_ref().unwrap().len(), 1);
}

#[test]
fn file_routes_policy_child_inherits_directory_groups_without_sibling_mounts() {
    let config: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /:\n    path: shared\n    statics: [{dir: frontend/assets}, {dir: frontend/icons}]\n  /frontend:\n    cors: disabled\n  /frontend/private:\n    path: private\n    static: {}\n",
    ).unwrap();
    let child = config.routes.find("/frontend").unwrap();
    let mounts = child.statics.as_ref().unwrap();
    assert_eq!(mounts.len(), 2);
    for (mount, directory) in mounts.iter().zip(["assets", "icons"]) {
        assert_eq!(mount.path.as_str(), format!("/frontend/{directory}"));
        assert_eq!(
            mount.dir,
            std::env::current_dir().unwrap()
                .join("public/shared/frontend").join(directory)
        );
    }
}

#[test]
fn file_routes_inherited_statics_respect_an_explicit_child_directory() {
    let config: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /files:\n    path: shared\n    static: {none_found: true}\n  /files/private:\n    path: relocated\n    cors: disabled\n",
    ).unwrap();
    let child = config.routes.find("/files/private").unwrap();
    let mount = &child.statics.as_ref().unwrap()[0];
    assert_eq!(mount.path.as_str(), "/files/private");
    assert_eq!(
        mount.dir,
        std::env::current_dir().unwrap().join("public/relocated")
    );
    assert!(mount.none_found);
}

#[test]
fn file_routes_reject_dot_segments_in_derived_directory_suffixes() {
    for source in [
        "root: public\nroutes: {'/../outside': {static: {}}}\n",
        "root: public\nroutes: {'/files/./item': {try_files: true}}\n",
        "root: public\nroutes:\n  /files:\n    path: shared\n    static: {}\n  /files/../outside: {}\n",
        "root: public\nroutes:\n  /:\n    path: shared\n  /../outside: {}\n",
    ] {
        let error = serde_yaml::from_str::<HostConfig>(source)
            .err()
            .expect("URL suffixes must not navigate filesystem directories");
        assert!(error.to_string().contains("dot segments"), "{error}");
    }

    let config: HostConfig = serde_yaml::from_str(
        "root: public\nroutes: {'/files': {path: ../shared, static: {}}}\n",
    ).unwrap();
    let route = config.routes.find("/files").unwrap();
    assert_eq!(
        route.files.mapping.as_ref().unwrap().directory(),
        std::env::current_dir().unwrap().join("public/../shared")
    );
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
    assert_eq!(
        config.routes.find("/other").unwrap().upstream_name(),
        Some("other-backend")
    );
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

#[cfg(not(feature = "upstream"))]
#[test]
fn disabled_upstream_is_rejected_in_flat_and_scoped_declarations() {
    for source in [
        "root: public\nupstream: example\n",
        "root: public\nupstream: example\nroutes: {ajax_only: true}\n",
        "root: public\nroutes: {'/': {upstream: example}}\n",
        "root: public\nroutes: {'/api': {upstream: example}}\n",
    ] {
        let error = serde_yaml::from_str::<HostConfig>(source)
            .err()
            .expect("unavailable upstream must not become an unbound owner");
        let message = error.to_string();
        assert!(message.contains("directive 'upstream'"), "{message}");
        assert!(message.contains("is not enabled"), "{message}");
    }
}
