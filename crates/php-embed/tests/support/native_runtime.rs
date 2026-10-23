use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use claviron_php::{PhpRunner, PhpRuntime};
use claviron_php_embed::extension::{PhpEmbedExtension, register_extension};
use claviron_php_embed::{
    PhpEmbedRuntime, PhpEmbedStateExt, PhpSapiRequest, PhpScriptService,
    RequestPool,
};
use claviron_runtime::registry::Runnable;
use claviron_runtime::{AppState, RunContext, RunMode};
use http::Request;
use http_body_util::BodyExt;
use tokio_util::sync::CancellationToken;

/// One runtime per integration-test process, with no listener or fleet boot.
pub async fn run_fixture(
    fixture: &Path,
    extension: Option<Arc<dyn PhpEmbedExtension>>,
) {
    let root = tempfile::tempdir().unwrap();
    let ini = root.path().join("php.ini");
    std::fs::write(
        &ini,
        "max_execution_time=0\ndisplay_errors=0\nlog_errors=1\n",
    )
    .unwrap();
    let script = root.path().join("index.php");
    std::fs::copy(fixture, &script).unwrap();
    let state = AppState::standalone("php-extension-test", root.path());
    if let Some(extension) = extension {
        register_extension(&state, extension).unwrap();
    }
    PhpEmbedRuntime::builder()
        .request_pool(RequestPool::fixed(2))
        .cache_invalidation(false)
        .ini_path(ini)
        .ini_scan_dir(root.path())
        .build()
        .register(&state)
        .unwrap();
    let php = state.php_embed().unwrap();
    claviron_runtime::AnyComponent::boot(
        php.as_ref(),
        state.registry_ref(),
        &state,
    )
    .await
    .unwrap();
    let service = state.registry_ref().resolve::<PhpScriptService>().unwrap();
    let cancellation = CancellationToken::new();
    let generation = service.clone().run(
        php.clone(),
        state,
        RunContext::new(cancellation.clone()),
    );
    tokio::pin!(generation);
    // The first poll synchronously starts the pool, then parks on cancellation.
    assert!(futures_util::poll!(&mut generation).is_pending());
    let runtime = Arc::new(PhpRuntime {
        mapping: None,
        path: None,
        entry_script: None,
        entry_query: None,
        try_files: false,
        entry_extensions: Arc::from([]),
        validate_script: false,
        indexes: Arc::from(["index.php".to_owned()]),
        dir: root.path().to_path_buf(),
        fpm: Arc::from([]),
        environment: Arc::new(HashMap::new()),
        env_file: Arc::from([]),
        execution_environment: Default::default(),
        php_runner: PhpRunner::Script,
        application: None,
        requests: Default::default(),
        responses: Default::default(),
        server_run_mode: RunMode::Standalone,
        server_worker_count: 1,
        server_hostname: "app.test".to_owned(),
        server_software: "claviron-test".to_owned(),
    });
    // Both ZTS threads and a reused request context must retain the metadata.
    for _ in 0..3 {
        let request = PhpSapiRequest {
            script_name: "/index.php".to_owned(),
            script_file: script.clone(),
            document_root: root.path().to_path_buf(),
            document_uri: "/index.php".to_owned(),
            request_uri: "/index.php".to_owned(),
            query_string: String::new(),
            path_info: None,
        };
        let response = service
            .execute(
                runtime.clone(),
                request,
                Request::builder()
                    .extension(claviron_http::policy::HttpPolicy::unlimited())
                    .body(claviron_http::empty_body())
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        assert_eq!(
            parts.status,
            http::StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(parts.headers["x-registration-test"], "ok");
        assert_eq!(body.as_ref(), b"ok");
    }
    cancellation.cancel();
    generation.await.unwrap();
    assert_eq!(php.pool_snapshot().unwrap().threads_current, 0);
}
