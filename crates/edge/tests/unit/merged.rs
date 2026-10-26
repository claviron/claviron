use std::time::Duration;

use axum::body::{Body, to_bytes};
use claviron_php_embed::{
    ApplicationPool, PhpApplication, PhpApplicationService, PhpEmbedRuntime,
    PhpScriptService, RequestPool,
};
use claviron_runtime::ServiceStatus;
use claviron_runtime::registry::Registry;
use http::{Request, StatusCode};
use tower::ServiceExt;

use super::*;

#[test]
fn application_routes_isolated_process() {
    const CHILD: &str = "CLAVIRON_APPLICATION_ROUTING_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let (_, module) = module_path!().split_once("::").unwrap();
        let ini = tempfile::tempdir().unwrap();
        std::fs::write(
            ini.path().join("php.ini"),
            "max_execution_time=0\ndisplay_errors=0\nlog_errors=1\n",
        )
        .unwrap();
        let output =
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    &format!("{module}::application_routes_isolated_process"),
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("PHPRC", ini.path())
                .env("PHP_INI_SCAN_DIR", "")
                .output()
                .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains("test result: ok. 1 passed;")
        );
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(25), exercise())
                .await
                .unwrap();
        });
}

async fn wait_for(mut predicate: impl FnMut() -> bool) {
    while !predicate() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    body: &str,
) -> (StatusCode, String) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .extension(claviron_http::policy::HttpPolicy::unlimited())
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

async fn exercise() {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("application.php");
    std::fs::write(directory.path().join("index.php"), "<?php echo 'classic';")
        .unwrap();
    std::fs::create_dir_all(directory.path().join("live/build")).unwrap();
    std::fs::write(
        directory.path().join("live/build/app.css"),
        "body { color: red; }",
    )
    .unwrap();
    std::fs::create_dir(directory.path().join("live/optional")).unwrap();
    std::fs::write(
        directory.path().join("live/data.json"),
        "must not bypass PHP",
    )
    .unwrap();
    std::fs::write(&script, include_str!("../fixtures/unit/merged.php"))
        .unwrap();
    let state =
        AppState::standalone("application-router-test", directory.path());
    PhpEmbedRuntime::builder()
        .request_pool(RequestPool::fixed(1))
        .application(
            PhpApplication::new("live", &script)
                .args(["live"])
                .pool(ApplicationPool::fixed(1)),
        )
        .application(PhpApplication::new("admin", &script).args(["admin"]))
        .graceful_timeout(Some(Duration::from_secs(2)))
        .build()
        .register(&state)
        .unwrap();
    let php = state.registry_ref().resolve::<PhpEmbedRuntime>().unwrap();
    claviron_runtime::AnyComponent::boot(
        php.as_ref(),
        state.registry_ref(),
        &state,
    )
    .await
    .unwrap();
    let application =
        state.registry_ref().resolve::<PhpApplicationService>().unwrap();
    let sapi = state.registry_ref().resolve::<PhpScriptService>().unwrap();
    let registry = Registry::<AppState>::default();
    registry.register(php).unwrap();
    registry.register(Arc::clone(&application)).unwrap();
    registry.register(sapi).unwrap();
    let mut runtime = claviron_runtime::Runtime::default();
    let manager = runtime.manager();
    assert_eq!(runtime.spawn_all(&registry, state.clone()).unwrap(), 2);
    let run_state = state.clone();
    let run = tokio::spawn(async move {
        runtime.wait_until_shutdown(&run_state).await.unwrap();
        runtime.drain().await.unwrap();
    });
    wait_for(|| {
        application.snapshot().len() == 2
            && application.snapshot().values().all(|p| p.ready == 1)
    })
    .await;

    // Directory policies share the PHP owner so misses and dynamic files can
    // continue to it. The unsorted PHP mounts still require longest-prefix selection.
    let config: HostConfig = serde_yaml::from_str(&format!(
        r#"
root: {}
try_files: true
routes:
  /:
    statics:
      - dir: live/build
        none_found: false
      - dir: live/optional
        none_found: true
php:
  - path: /live/admin
    runner: application
    application: admin
  - path: /
    runner: script
    entry_script: index.php
  - path: /missing
    runner: application
    application: missing
  - path: /live/
    runner: application
    application: live
"#,
        serde_yaml::to_string(directory.path()).unwrap().trim()
    ))
    .unwrap();
    let host = "app.test".parse::<Host>().unwrap();
    let router = create_default_router(
        state.clone(),
        &host,
        Arc::new(config),
        #[cfg(feature = "websocket")]
        None,
    )
    .unwrap();
    for (count, uri) in
        ["/live", "/live/", "/live/index.php", "/live/data.json?x=1"]
            .into_iter()
            .enumerate()
    {
        assert_eq!(
            call(&router, "GET", uri, "").await,
            (StatusCode::OK, format!("live#{} GET {uri} ", count + 1))
        );
    }
    assert_eq!(
        call(&router, "POST", "/live/echo?q=two", "payload").await,
        (StatusCode::OK, "live#5 POST /live/echo?q=two payload".into())
    );
    assert_eq!(
        call(&router, "GET", "/live/admin/settings", "").await,
        (StatusCode::OK, "admin#1 GET /live/admin/settings ".into())
    );
    for uri in ["/", "/lively", "/liveness/check"] {
        assert_eq!(
            call(&router, "GET", uri, "").await,
            (StatusCode::OK, "classic".into())
        );
    }
    assert_eq!(
        call(&router, "GET", "/missing/index.php", "").await,
        (StatusCode::SERVICE_UNAVAILABLE, String::new())
    );
    assert_eq!(
        call(&router, "GET", "/live/build/app.css", "").await,
        (StatusCode::OK, "body { color: red; }".into())
    );
    assert_eq!(
        call(&router, "GET", "/live/build/missing.css", "").await.0,
        StatusCode::NOT_FOUND
    );
    for uri in [
        "/live/building/app.css",
        "/live/optional/missing.css",
        "/live/build/index.php",
    ] {
        let (status, body) = call(&router, "GET", uri, "").await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        assert!(body.contains(&format!("GET {uri}")), "{uri}: {body}");
    }
    assert_eq!(
        call(&router, "GET", "/live/build/.env", "").await.0,
        StatusCode::FORBIDDEN
    );
    manager.stop("PhpApplicationService").await.unwrap();
    loop {
        if manager.list().await.unwrap().iter().any(|s| {
            s.name() == "PhpApplicationService"
                && s.status() == ServiceStatus::Stopped
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        call(&router, "GET", "/live/", "").await,
        (StatusCode::SERVICE_UNAVAILABLE, String::new())
    );
    assert_eq!(
        call(&router, "GET", "/", "").await,
        (StatusCode::OK, "classic".into())
    );
    assert_eq!(
        call(&router, "GET", "/live/build/app.css", "").await,
        (StatusCode::OK, "body { color: red; }".into())
    );
    manager.start("PhpApplicationService").await.unwrap();
    wait_for(|| {
        application.snapshot().get("live").is_some_and(|p| p.ready == 1)
    })
    .await;
    // The same router holds a service handle, not a stale generation/pool handle.
    assert_eq!(
        call(&router, "GET", "/live/", "").await,
        (StatusCode::OK, "live#1 GET /live/ ".into())
    );
    state.initiate_shutdown();
    run.await.unwrap();
    assert!(application.snapshot().is_empty());
}
