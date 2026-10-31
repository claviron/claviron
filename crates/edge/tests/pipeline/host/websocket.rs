use super::*;

use async_trait::async_trait;
use bytes::Bytes;
use claviron_http::upgrade::{
    H1UpgradeAccept, H1UpgradeContext, H1UpgradeHandler, H1UpgradeResult,
    H1UpgradeState, is_h1_header_upgrade,
};
use claviron_runtime::registry::Result;
use http_body::{Body, Frame};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

struct Handler(Arc<AtomicUsize>);

#[async_trait]
impl H1UpgradeHandler for Handler {
    fn is_upgrade(&self, request: &Request<RouteBody>) -> bool {
        self.0.fetch_add(1, Ordering::Relaxed);
        is_h1_header_upgrade(request, "websocket")
    }

    async fn accept(&self, _: &AppState, _: &H1UpgradeContext) -> H1UpgradeResult {
        Ok(H1UpgradeAccept::anonymous(Default::default()))
    }

    async fn handle_upgraded(
        &self, _: AppState, _: H1UpgradeContext, _: Option<H1UpgradeState>,
        _: hyper::upgrade::Upgraded,
    ) -> Result<()> {
        unreachable!("ordinary requests must not enter a session")
    }
}

struct Unread(Arc<AtomicUsize>);

impl Body for Unread {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<std::result::Result<Frame<Bytes>, Self::Error>>> {
        panic!("WebSocket selection must not read a request body")
    }
}

impl Drop for Unread {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
}

#[test]
fn only_declared_hosts_and_protocols_prepare_a_websocket_step() {
    let state = AppState::standalone("websocket-plan", ".");
    let host: Host = "example.test".parse().unwrap();
    let registry = Arc::new(H1UpgradeRegistry::default());
    let checks = Arc::new(AtomicUsize::new(0));
    assert!(WebSocketStage::new(state.clone(), host.clone(), &registry).is_none());
    registry.register_handler(host.clone(), "/socket", Arc::new(Handler(checks))).unwrap();
    let mut config = HostConfig::new("public");
    config.transport.h1.upgrade = false;
    assert!(WebSocketStage::prepare(&state, &host, &config, Some(registry.clone())).is_none());
    config.transport.h1.upgrade = true;
    assert!(WebSocketStage::prepare(&state, &host, &config, Some(registry.clone())).is_some());
    config.transport.protocols = Some(vec![claviron_http::listener::HttpProtocol::H2]);
    assert!(WebSocketStage::prepare(&state, &host, &config, Some(registry.clone())).is_none());
    config.transport.protocols = Some(Vec::new());
    assert!(WebSocketStage::prepare(&state, &host, &config, Some(registry.clone())).is_none());
    assert!(WebSocketStage::new(state, "other.test".parse().unwrap(), &registry).is_none());
}

#[test]
fn ordinary_requests_pass_the_original_body_inline_on_exact_path_lookup() {
    let state = AppState::standalone("websocket-pass", ".");
    let host: Host = "example.test".parse().unwrap();
    let registry = H1UpgradeRegistry::default();
    let checks = Arc::new(AtomicUsize::new(0));
    registry.register_handler(host.clone(), "/socket", Arc::new(Handler(checks.clone()))).unwrap();
    let stage = WebSocketStage::new(state, host, &registry).unwrap();
    for (path, checked) in [("/elsewhere", 0), ("/socket/child", 0), ("/socket", 1)] {
        let drops = Arc::new(AtomicUsize::new(0));
        let request = Request::builder().uri(path)
            .body(claviron_http::boxed_body(Unread(drops.clone()))).unwrap();
        let mut decision = stage.call(request);
        let Poll::Ready(ServiceResult::Unhandled(request)) =
            Pin::new(&mut decision).poll(&mut Context::from_waker(Waker::noop()))
        else { panic!("ordinary requests must pass synchronously"); };
        assert_eq!(request.uri().path(), path);
        assert_eq!(checks.load(Ordering::Relaxed), checked);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(request);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
