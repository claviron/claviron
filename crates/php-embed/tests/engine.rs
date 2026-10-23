use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use claviron_php::PhpRunner;
use claviron_runtime::RunMode;

use super::*;

fn temp_root() -> std::path::PathBuf {
    let nanos =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let root =
        std::env::temp_dir().join(format!("claviron-sapi-engine-{nanos}"));
    std::fs::create_dir_all(&root).unwrap();
    root
}

pub(super) fn runtime(root: std::path::PathBuf) -> Arc<PhpRuntime> {
    Arc::new(PhpRuntime {
        mapping: None,
        path: None,
        entry_script: None,
        entry_query: None,
        try_files: false,
        entry_extensions: Arc::from(Vec::<String>::new().into_boxed_slice()),
        validate_script: false,
        indexes: Arc::from(vec!["index.php".to_string()].into_boxed_slice()),
        dir: root,
        fpm: Arc::from(Vec::<String>::new().into_boxed_slice()),
        environment: Arc::new(HashMap::new()),
        env_file: Arc::from([]),
        execution_environment: Default::default(),
        php_runner: PhpRunner::Script,
        application: None,
        requests: Default::default(),
        responses: Default::default(),
        server_run_mode: RunMode::Standalone,
        server_worker_count: 1,
        server_hostname: "test-host".to_string(),
        server_software: "claviron-test".to_string(),
    })
}

fn sapi_request(root: &std::path::Path) -> PhpSapiRequest {
    PhpSapiRequest {
        script_name: "/index.php".to_string(),
        script_file: root.join("index.php"),
        document_root: root.to_path_buf(),
        document_uri: "/index.php".to_string(),
        request_uri: "/index.php".to_string(),
        query_string: String::new(),
        path_info: None,
    }
}

fn http_request() -> Request<RouteBody> {
    Request::builder()
        .extension(claviron_http::policy::HttpPolicy::unlimited())
        .body(claviron_http::empty_body())
        .unwrap()
}

#[test]
fn script_failure_preserves_error_status_but_normalizes_success() {
    let mut captured =
        CapturedResponse { status: StatusCode::OK, ..Default::default() };
    captured.mark_script_failure();
    assert_eq!(captured.status, StatusCode::INTERNAL_SERVER_ERROR);

    let mut captured =
        CapturedResponse { status: StatusCode::FOUND, ..Default::default() };
    captured.mark_script_failure();
    assert_eq!(captured.status, StatusCode::INTERNAL_SERVER_ERROR);

    let mut captured = CapturedResponse {
        status: StatusCode::NOT_FOUND,
        ..Default::default()
    };
    captured.mark_script_failure();
    assert_eq!(captured.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn run_drains_and_resumes_php_pool() {
    let root = temp_root();
    std::fs::write(
        root.join("index.php"),
        include_str!("fixtures/engine/response.php"),
    )
    .unwrap();

    let engine = PhpSapiEngine::new();
    engine
        .start_pool(
            SapiPoolSettings {
                max_requests: None,
                threads_min: 1,
                threads_max: 2,
                threads_max_idle: 1,
                thread_idle_timeout: std::time::Duration::from_secs(60),
                thread_max_life: None,
            },
            CancellationToken::new(),
        )
        .unwrap();
    let response = engine
        .execute(
            runtime(root.clone()),
            sapi_request(&root),
            http_request(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.headers().get("x-claviron-test").unwrap(), "yes");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, Bytes::from_static(b"hello sapi"));

    assert!(engine.drain(None).await);
    let stopped = engine.pool_snapshot().unwrap();
    assert_eq!(stopped.threads_current, 0);
    let rejected = engine
        .execute(
            runtime(root.clone()),
            sapi_request(&root),
            http_request(),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        rejected,
        SapiGatewayError::ServiceUnavailable(ref message)
            if message == "php request pool is not accepting requests"
    ));
    let after_rejection = engine.pool_snapshot().unwrap();
    assert_eq!(after_rejection.threads_current, stopped.threads_current);
    assert_eq!(
        after_rejection.threads_created_total,
        stopped.threads_created_total
    );

    engine
        .start_pool(
            SapiPoolSettings {
                max_requests: None,
                threads_min: 1,
                threads_max: 2,
                threads_max_idle: 1,
                thread_idle_timeout: std::time::Duration::from_secs(60),
                thread_max_life: None,
            },
            CancellationToken::new(),
        )
        .unwrap();
    let restarted = engine
        .execute(
            runtime(root.clone()),
            sapi_request(&root),
            http_request(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(restarted.status(), StatusCode::ACCEPTED);
    assert!(engine.drain(None).await);
}

#[test]
fn request_scheme_isolated_process() {
    const CHILD: &str = "CLAVIRON_SCHEME_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let ini = tempfile::tempdir().unwrap();
        std::fs::write(
            ini.path().join("php.ini"),
            "max_execution_time=0\ndisplay_errors=0\nlog_errors=1\n",
        )
        .unwrap();
        let (_, module) = module_path!().split_once("::").unwrap();
        let output =
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    &format!("{module}::request_scheme_isolated_process"),
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
            tokio::time::timeout(std::time::Duration::from_secs(20), async {
                use claviron_http::ext::{ClientAuthStatus, TlsInfo};
                let root = tempfile::tempdir().unwrap();
                std::fs::write(
                    root.path().join("index.php"),
                    include_str!("fixtures/engine/scheme.php"),
                )
                .unwrap();
                let engine = PhpSapiEngine::new();
                engine
                    .start_pool(
                        SapiPoolSettings {
                            max_requests: None,
                            threads_min: 1,
                            threads_max: 1,
                            threads_max_idle: 1,
                            thread_idle_timeout: std::time::Duration::from_secs(
                                60,
                            ),
                            thread_max_life: None,
                        },
                        CancellationToken::new(),
                    )
                    .unwrap();
                for tls in [Some(false), Some(true), Some(false), None] {
                    let mut req = Request::builder()
                        .uri("/index.php?q=%2F")
                        .extension(
                            claviron_http::policy::HttpPolicy::unlimited(),
                        )
                        .header("x-forwarded-proto", "spoofed")
                        .header("forwarded", "proto=spoofed")
                        .body(claviron_http::empty_body())
                        .unwrap();
                    if let Some(tls) = tls {
                        req.extensions_mut().insert(Arc::new(ConnCtx {
                            listener: "test".parse().unwrap(),
                            proto: None,
                            remote: "127.0.0.1:1234".parse().unwrap(),
                            local_ip: "127.0.0.1".parse().unwrap(),
                            local_port: 8080,
                            real_ip: None,
                            country_code: None,
                            tls: tls.then_some(TlsInfo {
                                protocol_version: None,
                                cipher_suite: None,
                                client_auth: ClientAuthStatus::NotProvided,
                                client_cert: None,
                            }),
                        }));
                    }
                    let mut script = sapi_request(root.path());
                    script.request_uri = req.uri().to_string();
                    let response = engine
                        .execute(
                            runtime(root.path().to_path_buf()),
                            script,
                            req,
                            None,
                        )
                        .await
                        .unwrap();
                    let body = response
                        .into_body()
                        .collect()
                        .await
                        .unwrap()
                        .to_bytes();
                    let expected = match tls {
                        Some(true) => "https|on|https|-|NONE|/index.php?q=%2F",
                        Some(false) => "http|off|http|-|-|/index.php?q=%2F",
                        None => "-|-|-|-|-|/index.php?q=%2F",
                    };
                    assert_eq!(body.as_ref(), expected.as_bytes());
                }
                assert!(engine.drain(None).await);
            })
            .await
            .unwrap();
        });
}
