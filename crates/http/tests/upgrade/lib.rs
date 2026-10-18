use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use claviron_runtime::AppState;
use claviron_runtime::registry::Result;
use http::{HeaderMap, Request};
use hyper::upgrade::Upgraded;

use super::{
    H1UpgradeAccept, H1UpgradeContext, H1UpgradeHandler, H1UpgradeRegistry,
    H1UpgradeResult, H1UpgradeState, is_h1_header_upgrade,
    websocket_accept_key,
};

struct TestHandler;

#[async_trait]
impl H1UpgradeHandler for TestHandler {
    fn is_upgrade(
        &self,
        req: &Request<Body>,
    ) -> bool {
        is_h1_header_upgrade(req, "websocket")
    }

    async fn accept(
        &self,
        _state: &AppState,
        _ctx: &H1UpgradeContext,
    ) -> H1UpgradeResult {
        Ok(H1UpgradeAccept::anonymous(HeaderMap::new()))
    }

    async fn handle_upgraded(
        &self,
        _state: AppState,
        _ctx: H1UpgradeContext,
        _accepted: Option<H1UpgradeState>,
        _upgraded: Upgraded,
    ) -> Result<()> {
        Ok(())
    }
}

#[test]
fn websocket_accept_key_matches_rfc_example() {
    assert_eq!(
        websocket_accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
}

#[test]
fn header_upgrade_requires_http_11_connection_and_upgrade_headers() {
    let request = Request::builder()
        .version(http::Version::HTTP_11)
        .header(http::header::CONNECTION, "keep-alive, Upgrade")
        .header(http::header::UPGRADE, "websocket")
        .body(Body::empty())
        .unwrap();
    assert!(is_h1_header_upgrade(&request, "websocket"));

    let request = Request::builder()
        .version(http::Version::HTTP_2)
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::UPGRADE, "websocket")
        .body(Body::empty())
        .unwrap();
    assert!(!is_h1_header_upgrade(&request, "websocket"));
}

#[test]
fn registry_rejects_invalid_and_duplicate_exact_routes() {
    let registry = H1UpgradeRegistry::default();
    let handler: Arc<dyn H1UpgradeHandler> = Arc::new(TestHandler);

    assert!(
        registry
            .register_handler_for_host(
                "example.test",
                "socket",
                handler.clone()
            )
            .is_err()
    );
    registry
        .register_handler_for_host("example.test", "/socket", handler.clone())
        .unwrap();
    assert!(
        registry
            .register_handler_for_host("example.test", "/socket", handler)
            .is_err()
    );
}
