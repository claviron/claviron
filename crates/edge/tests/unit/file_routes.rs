use super::*;
use crate::HostConfig;
use axum::body::{Body, to_bytes};
use claviron_files::{FileSite, ResourceAdapter};
use claviron_http::{RequestContext, RouteResponse, bytes_body};
use claviron_runtime::AppState;
use http::Request;
use http::StatusCode;

struct Fallback;

#[async_trait::async_trait]
impl claviron_files::Handler for Fallback {
    async fn handle(
        &self,
        _state: &AppState,
        _site: &dyn FileSite,
        _context: RequestContext<'_>,
    ) -> Result<RouteResponse, StatusCode> {
        Ok(http::Response::new(bytes_body("resource fallback".into())))
    }
}

struct Resource(Arc<ResourceAdapter>);

impl HostHandler for Resource {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        let request = request.into_request();
        let site = request
            .extensions()
            .get::<crate::config::SelectedRoute>()
            .unwrap()
            .0
            .clone();
        let canonical = request.extensions().get::<Arc<HostConfig>>().unwrap();
        assert!(!Arc::ptr_eq(canonical, &site));
        let adapter = self.0.clone();
        Box::pin(async move {
            ServiceResult::Handled(
                adapter.handle_with_site(request, site.as_ref()).await.unwrap(),
            )
        })
    }
}

#[tokio::test]
async fn file_routes_reach_native_intercepts_with_the_selected_file_policy() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(directory.path().join("frontend/assets")).unwrap();
    std::fs::create_dir_all(directory.path().join("frontend/icons")).unwrap();
    std::fs::create_dir_all(directory.path().join("uploads/qr")).unwrap();
    std::fs::write(directory.path().join("frontend/assets/app.js"), "script")
        .unwrap();
    std::fs::write(directory.path().join("frontend/icons/icon.png"), "icon")
        .unwrap();
    std::fs::write(directory.path().join("uploads/qr/saved.png"), "saved")
        .unwrap();
    let state = AppState::standalone("file-routes", directory.path());
    let config: HostConfig = serde_yaml::from_str(&format!(
        "root: {}\nroutes:\n  /frontend:\n    try_files: true\n    statics:\n      - dir: assets\n        rules: [{{test: '^app\\.js$', caching: {{client: 1y, immutable: true}}}}]\n      - dir: icons\n  /storage:\n    path: uploads\n    static: {{none_found: true}}\n  /storage/qr:\n    cors: permissive\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    )).unwrap();
    // Deliberately has no mounts: the native handler must receive the selected
    // route, not fall back to this original host view.
    let adapter = Arc::new(ResourceAdapter::from_handler(
        Fallback,
        state.clone(),
        Arc::new(HostConfig::new(directory.path())),
    ));
    let app: Arc<dyn HostHandler> = Arc::new(Resource(adapter));
    let config = Arc::new(config);
    let owner = crate::pipeline::host::compile::prepare(
        &state,
        &"cdn.example.test".parse().unwrap(),
        config.clone(),
        &[app],
        &[],
        #[cfg(feature = "websocket")]
        None,
    )
    .unwrap();
    for (uri, status, expected) in [
        ("/frontend/assets/app.js", StatusCode::OK, "script"),
        ("/frontend/icons/icon.png", StatusCode::OK, "icon"),
        ("/frontend/assets/missing.js", StatusCode::NOT_FOUND, ""),
        ("/storage/qr/saved.png", StatusCode::OK, "saved"),
        ("/storage/qr/new.png", StatusCode::OK, "resource fallback"),
        ("/frontend/.hidden", StatusCode::FORBIDDEN, ""),
        ("/frontend/assets/%2e%2e/index.html", StatusCode::FORBIDDEN, ""),
    ] {
        let response = owner
            .handle(
                Request::builder()
                    .uri(uri)
                    .extension(config.clone())
                    .body(claviron_http::empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{uri}");
        if uri == "/frontend/assets/app.js" {
            assert!(
                response.headers()[http::header::CACHE_CONTROL]
                    .to_str()
                    .unwrap()
                    .contains("immutable")
            );
        }
        assert_eq!(
            to_bytes(Body::new(response.into_body()), 4096).await.unwrap(),
            expected,
            "{uri}"
        );
    }
}

#[tokio::test]
async fn file_routes_feed_axum_free_resource_adapters() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("build")).unwrap();
    std::fs::write(directory.path().join("build/app.js"), "other").unwrap();
    let config: HostConfig = serde_yaml::from_str(&format!(
        "root: {}\nroutes:\n  /assets:\n    path: build\n    static: {{}}\n  /storage:\n    path: uploaded\n    static: {{none_found: true}}\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    ))
    .unwrap();
    let adapter = ResourceAdapter::from_handler(
        Fallback,
        AppState::standalone("file-other", directory.path()),
        Arc::new(config),
    );
    for (uri, status, body) in [
        ("/assets/app.js", StatusCode::OK, "other"),
        ("/assets/missing.js", StatusCode::NOT_FOUND, ""),
        ("/storage/missing.png", StatusCode::OK, "resource fallback"),
    ] {
        let response = adapter
            .handle(
                http::Request::builder()
                    .uri(uri)
                    .body(claviron_http::empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{uri}");
        assert_eq!(
            to_bytes(Body::new(response.into_body()), 4096).await.unwrap(),
            body,
            "{uri}"
        );
    }
}

#[tokio::test]
async fn file_routes_static_and_try_files_share_explicit_and_inherited_mappings()
 {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(directory.path().join("mapped/assets")).unwrap();
    std::fs::create_dir_all(directory.path().join("plain")).unwrap();
    std::fs::write(directory.path().join("mapped/assets/app.js"), "mapped")
        .unwrap();
    std::fs::write(directory.path().join("mapped/index.html"), "index")
        .unwrap();
    std::fs::write(directory.path().join("plain/app.js"), "implicit").unwrap();
    let config: HostConfig = serde_yaml::from_str(&format!(
        "root: {}\nroutes:\n  /frontend:\n    path: mapped\n    index: [index.html]\n    try_files: true\n    statics: [{{dir: assets}}]\n  /frontend/assets:\n    try_files: true\n  /plain:\n    try_files: true\n",
        serde_yaml::to_string(directory.path()).unwrap().trim(),
    )).unwrap();
    let config = Arc::new(config);
    let adapter = ResourceAdapter::from_handler(
        Fallback,
        AppState::standalone("file-mapping", directory.path()),
        config.clone(),
    );
    for (path, expected) in [
        ("/frontend/assets/app.js?version=1", "mapped"),
        ("/frontend/", "index"),
        ("/plain/app.js", "implicit"),
    ] {
        let uri: http::Uri = path.parse().unwrap();
        let response = adapter
            .handle(
                http::Request::builder()
                    .uri(uri.clone())
                    .body(claviron_http::empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(
            to_bytes(Body::new(response.into_body()), 4096).await.unwrap(),
            expected
        );
        if !uri.path().ends_with('/') {
            let response = match claviron_files::try_files_for_site(
                config.as_ref(),
                &uri,
                &http::HeaderMap::new(),
            )
            .await
            {
                claviron_http::ServiceResult::Handled(response) => response,
                claviron_http::ServiceResult::Unhandled(()) => {
                    panic!("missing mapped file: {path}")
                }
            };
            assert_eq!(
                to_bytes(Body::new(response.into_body()), 4096).await.unwrap(),
                expected
            );
        }
    }
}
