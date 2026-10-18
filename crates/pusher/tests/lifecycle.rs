use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use claviron_http::ext::RealIpExt;
use claviron_http::session::SessionOwner;
use claviron_http::{RouteBody, boxed_body, bytes_body, empty_body};
use claviron_pusher::{HttpTargetConfig, PusherAdapter};
use claviron_runtime::gate::{Gate, GateDrainOutcome};
use http::{Request, Response, StatusCode, header};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

const TIMEOUT: Duration = Duration::from_secs(5);
const PROBE: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    Graceful,
    Forced,
    Sealed,
    Declined,
}

fn config(
    address: std::net::SocketAddr,
    timeout: &str,
) -> Arc<HttpTargetConfig> {
    Arc::new(serde_yaml::from_str(&format!(
        "address: '{address}'\nrequest_timeout: {timeout}\nconnect_timeout: 1s\nidle_timeout: 1h"
    )).unwrap())
}

fn request() -> Request<RouteBody> {
    Request::builder()
        .uri("/app/test?probe=1")
        .header(header::HOST, "app.test")
        .header(header::CONNECTION, "upgrade")
        .header(header::UPGRADE, "websocket")
        .header("x-forwarded-proto", "spoofed")
        .header("x-forwarded-for", "192.0.2.250")
        .body(empty_body())
        .unwrap()
}

async fn lifecycle(case: Case) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let adapter =
        PusherAdapter::new(config(listener.local_addr().unwrap(), "1s"));
    let (seen, mut requests) = mpsc::unbounded_channel();
    let (respond, allowed) = watch::channel(false);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let handler = service_fn(move |mut req: Request<Incoming>| {
            assert_eq!(req.uri(), "/app/test?probe=1");
            assert_eq!(
                req.headers()["x-forwarded-proto"],
                if case == Case::Graceful { "http" } else { "https" }
            );
            assert_eq!(req.headers()["x-forwarded-for"], "203.0.113.10");
            assert_eq!(req.headers()["x-forwarded-host"], "app.test");
            seen.send(hyper::upgrade::on(&mut req)).unwrap();
            let mut allowed = allowed.clone();
            async move {
                allowed.wait_for(|value| *value).await.unwrap();
                let response = if case == Case::Declined {
                    // A non-101 response must not wait for a keep-alive driver
                    // or for the entire unconsumed backend response body.
                    Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(bytes_body(Bytes::from(vec![b'x'; 65536])))
                        .unwrap()
                } else {
                    Response::builder()
                        .status(StatusCode::SWITCHING_PROTOCOLS)
                        .header(header::CONNECTION, "upgrade")
                        .header(header::UPGRADE, "websocket")
                        .body(empty_body())
                        .unwrap()
                };
                Ok::<_, Infallible>(response)
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(socket), handler)
            .with_upgrades()
            .await;
    });

    let mut owner = Some(SessionOwner::new("pusher-test".parse().unwrap()));
    let gate = Gate::new(Some(1), TIMEOUT);
    let scope = owner.as_ref().unwrap().bind(gate.try_enter().unwrap());
    let sessions = scope.sessions();
    let handler = service_fn(move |mut req: Request<Incoming>| {
        req.extensions_mut().insert(sessions.clone());
        req.extensions_mut().insert(if case == Case::Graceful {
            http::uri::Scheme::HTTP
        } else {
            http::uri::Scheme::HTTPS
        });
        req.extensions_mut().insert(RealIpExt("203.0.113.10".parse().unwrap()));
        let adapter = adapter.clone();
        async move { adapter.handle(req.map(boxed_body)).await }
    });
    let (client_io, server_io) = tokio::io::duplex(8192);
    tasks.spawn(async move {
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(server_io), handler)
            .with_upgrades()
            .await;
    });
    let (mut sender, driver) =
        hyper::client::conn::http1::handshake(TokioIo::new(client_io))
            .await
            .unwrap();
    tasks.spawn(async move {
        let _ = driver.with_upgrades().await;
    });
    let pending = tokio::spawn(async move {
        let response = sender.send_request(request()).await.unwrap();
        (sender, response)
    });
    let origin_upgrade = requests.recv().await.unwrap();
    let mut drain =
        (case == Case::Sealed).then(|| owner.take().unwrap().close());
    respond.send(true).unwrap();
    let (sender, mut response) = pending.await.unwrap();
    match case {
        Case::Declined => {
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            drop(origin_upgrade);
        }
        Case::Sealed => {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let mut origin = TokioIo::new(origin_upgrade.await.unwrap());
            assert_eq!(origin.read(&mut [0; 1]).await.unwrap(), 0);
        }
        Case::Graceful | Case::Forced => {
            assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
            let mut client =
                TokioIo::new(hyper::upgrade::on(&mut response).await.unwrap());
            let mut origin = TokioIo::new(origin_upgrade.await.unwrap());
            client.write_all(b"ping").await.unwrap();
            let mut bytes = [0; 4];
            origin.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"ping");
            origin.write_all(b"pong").await.unwrap();
            client.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"pong");
            drop(scope);
            assert_eq!(gate.count(), 1);
            let mut drain = owner.take().unwrap().close();
            gate.graceful_shutdown(if case == Case::Forced {
                Some(Duration::ZERO)
            } else {
                None
            });
            if case == Case::Graceful {
                assert!(
                    tokio::time::timeout(PROBE, gate.wait_all_done())
                        .await
                        .is_err()
                );
                // Indefinite listener grace is not replaced by an execution timeout.
                client.shutdown().await.unwrap();
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
            assert_eq!(origin.read(&mut [0; 1]).await.unwrap(), 0);
            assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
            assert_eq!(gate.count(), 0);
            drop(sender);
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return;
        }
    }
    drop(scope);
    let drain = drain.take().unwrap_or_else(|| owner.take().unwrap().close());
    drain.join().await;
    assert_eq!(gate.count(), 0);
    drop(sender);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[tokio::test]
async fn native_tunnel_drains_or_forces_and_rejects_handoff_after_stop() {
    for case in [Case::Graceful, Case::Forced, Case::Sealed, Case::Declined] {
        tokio::time::timeout(TIMEOUT, lifecycle(case)).await.unwrap();
    }
}

#[tokio::test]
async fn missing_owner_fails_before_dialing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let adapter =
        PusherAdapter::new(config(listener.local_addr().unwrap(), "1s"));
    assert_eq!(
        adapter.handle(request()).await.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(tokio::time::timeout(PROBE, listener.accept()).await.is_err());
}

#[tokio::test]
async fn rest_forwarding_uses_native_scheme_and_trusted_client_identity() {
    tokio::time::timeout(TIMEOUT, async {
        for scheme in [
            Some(http::uri::Scheme::HTTP),
            Some(http::uri::Scheme::HTTPS),
            None,
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let adapter = PusherAdapter::new(config(
                listener.local_addr().unwrap(),
                "1s",
            ));
            let expected = scheme.clone();
            let origin = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let handler = service_fn(move |req: Request<Incoming>| {
                    assert_eq!(req.uri(), "/apps/42/events?probe=1");
                    assert_eq!(
                        req.headers()
                            .get("x-forwarded-proto")
                            .map(|v| v.to_str().unwrap()),
                        expected.as_ref().map(|v| v.as_str())
                    );
                    assert_eq!(
                        req.headers()["x-forwarded-for"],
                        "203.0.113.10"
                    );
                    assert_eq!(req.headers()["x-forwarded-host"], "app.test");
                    assert!(!req.headers().contains_key("forwarded"));
                    async {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .header(header::CONNECTION, "close")
                                .body(empty_body())
                                .unwrap(),
                        )
                    }
                });
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), handler)
                    .await
                    .unwrap();
            });
            let mut req = Request::builder()
                .uri("/apps/42/events?probe=1")
                .header(header::HOST, "app.test")
                .header("forwarded", "for=192.0.2.250;proto=spoofed")
                .header("x-forwarded-for", "192.0.2.250")
                .header("x-forwarded-proto", "spoofed")
                .body(empty_body())
                .unwrap();
            if let Some(scheme) = scheme {
                req.extensions_mut().insert(scheme);
            }
            req.extensions_mut()
                .insert(RealIpExt("203.0.113.10".parse().unwrap()));
            let response = adapter.handle(req).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            drop(response);
            origin.await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn timeout_before_101_closes_the_inline_backend_driver() {
    tokio::time::timeout(TIMEOUT, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let adapter =
            PusherAdapter::new(config(listener.local_addr().unwrap(), "25ms"));
        let origin = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            // Never reply; request timeout must drop the backend socket itself.
            assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
        });
        let owner = SessionOwner::new("timeout".parse().unwrap());
        let gate = Gate::new(Some(1), TIMEOUT);
        let scope = owner.bind(gate.try_enter().unwrap());
        let mut req = request();
        req.extensions_mut().insert(scope.sessions());
        assert_eq!(
            adapter.handle(req).await.unwrap().status(),
            StatusCode::GATEWAY_TIMEOUT
        );
        origin.await.unwrap();
        drop(scope);
        owner.close().join().await;
        assert_eq!(gate.count(), 0);
    })
    .await
    .unwrap();
}
