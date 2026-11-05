#[cfg(feature = "php_embed")]
use super::*;

#[cfg(feature = "php_embed")]
#[test]
fn file_routes_php_preparation_uses_the_same_mapping_as_files() {
    let directory = tempfile::tempdir().unwrap();
    let config: HostConfig = serde_yaml::from_str(&format!(
        "root: {}\nroutes:\n  /admin:\n    directory: mapped\n    php: {{runner: script, entry_script: index.php}}\n  /plain:\n    php: {{runner: script}}\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    )).unwrap();
    let state =
        claviron_runtime::AppState::standalone("php-mapping", directory.path());
    for (prefix, disk) in [("/admin", "mapped"), ("/plain", "plain")] {
        let route = config.routes.find(prefix).unwrap();
        let declared = declarations(route).unwrap();
        let runtime = prepare_runtimes(&state, route, &declared).pop().unwrap();
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
