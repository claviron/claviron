use super::*;
use claviron_http::config::mapping::RouteMapping;
use claviron_php::PhpConfig;
use claviron_runtime::AppState;

#[tokio::test]
async fn file_routes_embed_preserves_uri_and_pinned_entry_with_mapped_directory()
 {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("index.php"), "<?php").unwrap();
    std::fs::write(directory.path().join("app.js"), "script").unwrap();
    let state = AppState::standalone("php-mapping", directory.path());
    let template: PhpConfig = serde_yaml::from_str("{}").unwrap();
    let mut runtime = PhpRuntime::from_config(
        &state,
        &template,
        directory.path().into(),
        &[],
    );
    runtime.mapping = Some(RouteMapping::new(
        serde_yaml::from_str("/admin").unwrap(),
        runtime.dir.clone(),
    ));
    for (path, script_name, path_info) in [
        ("/admin/index.php", "/admin/index.php", None),
        ("/admin/", "/admin/index.php", None),
        ("/admin/index.php/profile", "/admin/index.php", Some("/profile")),
    ] {
        let request =
            resolve_request(&runtime, path, "q=%2F").await.unwrap().unwrap();
        assert_eq!(request.script_file, runtime.dir.join("index.php"));
        assert_eq!(request.script_name, script_name);
        assert_eq!(request.path_info.as_deref(), path_info);
        let expected_path =
            if path_info.is_some() { script_name } else { path };
        assert_eq!(request.request_uri, format!("{expected_path}?q=%2F"));
        assert_eq!(request.query_string, "q=%2F");
    }
    assert!(
        try_files_candidate_exists(&runtime, "/admin/app.js").await.unwrap()
    );
    assert!(
        !try_files_candidate_exists(&runtime, "/other/app.js").await.unwrap()
    );
    runtime.entry_script = Some("pinned.php".into());
    // The pinned file deliberately does not exist: no freshness/existence IO.
    let request = resolve_request(&runtime, "/admin/profile", "q=%2F")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.script_file, runtime.dir.join("pinned.php"));
    assert_eq!(request.script_name, "/pinned.php");
    assert_eq!(request.request_uri, "/admin/profile?q=%2F");
    runtime.entry_query = Some("route=$uri&$args".into());
    let request = resolve_request(&runtime, "/admin/profile", "q=%2F")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.request_uri, "/pinned.php?route=/admin/profile&q=%2F");
}
