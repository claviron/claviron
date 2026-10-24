use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::middleware::from_fn_with_state;
use bytes::Bytes;
use claviron_http::ext::ConnCtx;
use claviron_http::session::{SessionOwner, SessionSpawner};
use claviron_http::upgrade::{
    H1UpgradeAccept, H1UpgradeContext, H1UpgradeHandler,
    H1UpgradeMiddlewareState, H1UpgradeRegistry, H1UpgradeResult,
    H1UpgradeState, h1_upgrade_middleware, is_h1_header_upgrade,
};
use claviron_runtime::AppState;
use claviron_runtime::gate::{Gate, GateDrainOutcome};
use claviron_runtime::registry::{Error, Result};
use http::{HeaderMap, Request, StatusCode, header};
use http_body_util::Full;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

const TIMEOUT: Duration = Duration::from_secs(5);

struct Released(Arc<AtomicUsize>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Handler {
    released: Arc<AtomicUsize>,
    entered: tokio::sync::mpsc::UnboundedSender<()>,
}

#[async_trait]
impl H1UpgradeHandler for Handler {
    fn is_upgrade(
        &self,
        request: &Request<Body>,
    ) -> bool {
        is_h1_header_upgrade(request, "websocket")
    }

    async fn accept(
        &self,
        _: &AppState,
        _: &H1UpgradeContext,
    ) -> H1UpgradeResult {
        Ok(H1UpgradeAccept::with_state(
            HeaderMap::new(),
            Released(self.released.clone()),
        ))
    }

    async fn handle_upgraded(
        &self,
        _: AppState,
        context: H1UpgradeContext,
        accepted: Option<H1UpgradeState>,
        upgraded: Upgraded,
    ) -> Result<()> {
        let _accepted = accepted.unwrap().take::<Released>()?;
        let mut io = TokioIo::new(upgraded);
        self.entered.send(()).unwrap();
        context.sessions.wait_graceful_shutdown().await;
        io.write_all(b"close").await.map_err(|e| Error::msg(e.to_string()))?;
        let mut data = [0; 4];
        io.read_exact(&mut data)
            .await
            .map_err(|e| Error::msg(e.to_string()))?;
        assert_eq!(&data, b"done");
        Ok(())
    }
}

fn router(
    state: AppState,
    handler: Arc<Handler>,
) -> Router {
    let registry = Arc::new(H1UpgradeRegistry::default());
    registry.register_handler_for_host("app.test", "/socket", handler).unwrap();
    Router::new().fallback(|| async { StatusCode::NOT_FOUND }).layer(
        from_fn_with_state(
            H1UpgradeMiddlewareState {
                app_state: state,
                registry,
                host: "app.test".parse().unwrap(),
            },
            h1_upgrade_middleware,
        ),
    )
}

fn context() -> Arc<ConnCtx> {
    Arc::new(ConnCtx {
        state: Default::default(),
        listener: "test".parse().unwrap(),
        proto: Some(claviron_http::listener::HttpProtocol::H1),
        remote: "127.0.0.1:12345".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 80,
        real_ip: None,
        country_code: None,
        tls: None,
    })
}

fn request<B>(body: B) -> Request<B> {
    Request::builder()
        .uri("/socket")
        .header(header::HOST, "app.test")
        .header(header::CONNECTION, "upgrade")
        .header(header::UPGRADE, "websocket")
        .extension(context())
        .body(body)
        .unwrap()
}

#[tokio::test]
async fn builtin_upgrade_is_owned_after_101_and_joins_on_graceful_or_forced_stop()
 {
    tokio::time::timeout(TIMEOUT, async {
        for grace in [None, Some(Duration::ZERO)] {
            let directory = tempfile::tempdir().unwrap();
            let state = AppState::standalone("upgrade-test", directory.path());
            let released = Arc::new(AtomicUsize::new(0));
            let (entered, mut running) = tokio::sync::mpsc::unbounded_channel();
            let router = router(
                state,
                Arc::new(Handler { released: released.clone(), entered }),
            );
            let owner = SessionOwner::new("test".parse().unwrap());
            let gate = Gate::new(Some(1), TIMEOUT);
            let scope = owner.bind(gate.try_enter().unwrap());
            let sessions = scope.sessions();
            let (client_io, server_io) = tokio::io::duplex(8192);
            let server = tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |mut request: Request<hyper::body::Incoming>| {
                        request.extensions_mut().insert(context());
                        request.extensions_mut().insert(sessions.clone());
                        router.clone().oneshot(request)
                    },
                );
                let _scope = scope;
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(server_io), service)
                    .with_upgrades()
                    .await
                    .unwrap();
            });
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                    .await
                    .unwrap();
            let client = tokio::spawn(connection.with_upgrades());
            let mut response = sender
                .send_request(request(Full::new(Bytes::new())))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
            let mut io =
                TokioIo::new(hyper::upgrade::on(&mut response).await.unwrap());
            running.recv().await.unwrap();
            server.await.unwrap();
            assert_eq!(
                gate.count(),
                1,
                "101 does not release the connection slot"
            );
            assert!(gate.try_enter().is_err());
            let mut drain = owner.close();
            gate.graceful_shutdown(grace);
            if grace.is_none() {
                let mut data = [0; 5];
                io.read_exact(&mut data).await.unwrap();
                assert_eq!(&data, b"close");
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(25),
                        gate.wait_all_done()
                    )
                    .await
                    .is_err()
                );
                assert_eq!(released.load(Ordering::SeqCst), 0);
                io.write_all(b"done").await.unwrap();
                assert_eq!(
                    gate.wait_all_done().await,
                    GateDrainOutcome::Drained
                );
            } else {
                assert!(matches!(
                    gate.wait_all_done().await,
                    GateDrainOutcome::Forced { remaining: 1 }
                ));
                drain.abort_all();
            }
            drain.join().await;
            assert_eq!(released.load(Ordering::SeqCst), 1);
            assert_eq!(gate.count(), 0);
            drop(io);
            drop(sender);
            client.await.unwrap().unwrap();
        }
    })
    .await
    .expect("native upgrade drain must finish");
}

#[tokio::test]
async fn missing_or_sealed_owner_never_returns_101_and_releases_accepted_state()
{
    let directory = tempfile::tempdir().unwrap();
    let state = AppState::standalone("upgrade-test", directory.path());
    let released = Arc::new(AtomicUsize::new(0));
    let (entered, mut running) = tokio::sync::mpsc::unbounded_channel();
    let router = router(
        state,
        Arc::new(Handler { released: released.clone(), entered }),
    );
    let response =
        router.clone().oneshot(request(Body::empty())).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        released.load(Ordering::SeqCst),
        0,
        "missing owner fails before accept"
    );
    let owner = SessionOwner::new("test".parse().unwrap());
    let gate = Gate::new(None, TIMEOUT);
    let scope = owner.bind(gate.try_enter().unwrap());
    let mut req = request(Body::empty());
    req.extensions_mut().insert::<SessionSpawner>(scope.sessions());
    let drain = owner.close();
    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        released.load(Ordering::SeqCst),
        1,
        "rejected hand-off releases handler state"
    );
    assert!(running.try_recv().is_err());
    drop(scope);
    drain.join().await;
    assert_eq!(gate.count(), 0);
}
