use http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

use super::*;

#[tokio::test]
async fn mounts_serve_assets_and_leave_the_rest_to_the_owner() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("build")).unwrap();
    std::fs::write(dir.path().join("build/app.css"), "body{}").unwrap();
    std::fs::write(dir.path().join("build/secret.php"), "private PHP source")
        .unwrap();
    std::fs::write(dir.path().join("build/.env"), "private environment")
        .unwrap();
    let files = Arc::new(Files {
        root: dir.path().to_owned(),
        try_files: false,
        statics: vec![StaticConfig {
            path: serde_yaml::from_str("/build").unwrap(),
            dir: "build".into(),
            none_found: false,
            rules: None,
        }],
    });
    let service =
        in_front(files, Router::new().fallback(|| async { "application" }));
    for (method, path, status, body) in [
        (Method::GET, "/build/app.css", StatusCode::OK, "body{}"),
        (Method::HEAD, "/build/app.css", StatusCode::OK, ""),
        (Method::POST, "/build/app.css", StatusCode::OK, "application"),
        (Method::GET, "/runtime", StatusCode::OK, "application"),
        // Dynamic files go to the application; their source is never served.
        (Method::GET, "/build/secret.php", StatusCode::OK, "application"),
        (Method::GET, "/builder/app.css", StatusCode::OK, "application"),
    ] {
        let response = service
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{path}");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            body,
            "{path}"
        );
    }
    for path in
        ["/build/.env", "/build/%2e%2e/secret.php", "/build/missing.css"]
    {
        let response = service
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::OK, "{path}");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(!String::from_utf8_lossy(&body).contains("private"));
    }
}

#[tokio::test]
async fn asset_shorthand_and_opt_in_probe_share_the_file_service() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("build")).unwrap();
    std::fs::write(dir.path().join("build/app.js"), "asset").unwrap();
    std::fs::write(dir.path().join("robots.txt"), "robots").unwrap();
    std::fs::write(dir.path().join("secret.php"), "private PHP source")
        .unwrap();
    std::fs::write(dir.path().join(".env"), "private environment").unwrap();
    for enabled in [false, true] {
        let mut files =
            Files::public().with_assets(&["build".into()], enabled).unwrap();
        files.root = dir.path().to_owned();
        let service = in_front(
            Arc::new(files),
            Router::new().fallback(|| async { "application" }),
        );
        for (method, path, status, body) in [
            (Method::GET, "/build/app.js", StatusCode::OK, "asset"),
            (Method::HEAD, "/build/app.js", StatusCode::OK, ""),
            (
                Method::GET,
                "/robots.txt",
                StatusCode::OK,
                if enabled { "robots" } else { "application" },
            ),
            (Method::HEAD, "/robots.txt", StatusCode::OK, ""),
            (Method::POST, "/robots.txt", StatusCode::OK, "application"),
            (Method::GET, "/missing.txt", StatusCode::OK, "application"),
            (Method::GET, "/secret.php", StatusCode::OK, "application"),
        ] {
            let response = service
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{enabled}: {path}");
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                body,
                "{enabled}: {path}"
            );
        }
        if enabled {
            for path in ["/.env", "/%2e%2e/secret.php"] {
                let response = service
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(path)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
            }
        }
    }
}

#[test]
fn shorthand_cannot_escape_the_root_or_replace_an_explicit_policy() {
    for path in [
        "",
        "/private",
        "../private",
        "build/../private",
        ".env",
        "build/%2e%2e",
        "build?x",
        "build\\private",
    ] {
        assert!(
            Files::public().with_assets(&[path.into()], false).is_err(),
            "{path}"
        );
    }
    let source: FilesSource = serde_yaml::from_str(
        "root: public\nstatics: [{path: /build, dir: custom, none_found: true}]",
    ).unwrap();
    let files =
        source.parse().unwrap().with_assets(&["build".into()], false).unwrap();
    assert_eq!(files.statics.len(), 1);
    assert_eq!(files.statics[0].dir, PathBuf::from("custom"));
    assert!(files.statics[0].none_found);
}
