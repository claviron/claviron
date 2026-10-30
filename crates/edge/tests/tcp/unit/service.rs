use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::deny::DenyService;
use crate::transport::{
    AnyAuthenticated, AppKind, BypassService, H2ConnectAccept,
    H2ConnectHandler, H2ConnectReject, H2ConnectResult, HostIngress,
    resolve_sni_from_table,
};
use async_trait::async_trait;
use bytes::Bytes;
use claviron_http::RouteBody;
use claviron_http::config::{HttpPolicy, ProxyConfig, RouterConfig};
use claviron_http::ext::ConnCtx;
use claviron_http::session::SessionOwner;
use claviron_runtime::gate::Gate;
use http::Response;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

struct EchoConnect {
    calls: Arc<AtomicUsize>,
    accepts: Arc<AtomicUsize>,
    tasks: tokio::sync::mpsc::UnboundedSender<()>,
}

#[async_trait]
impl H2ConnectHandler for EchoConnect {
    async fn accept(
        &self,
        _: &AppState,
        ctx: &AuthContext,
    ) -> H2ConnectResult {
        self.accepts.fetch_add(1, Ordering::SeqCst);
        if ctx.path.as_ref() == "/reject" {
            return Err(H2ConnectReject::forbidden());
        }
        assert_eq!(ctx.conn.real_ip, Some("203.0.113.9".parse().unwrap()));
        Ok(H2ConnectAccept::authenticated(42_u32))
    }

    async fn handle(
        &self,
        _: AppState,
        ctx: AuthContext,
        authenticated: Option<AnyAuthenticated>,
        mut request: Request<RouteBody>,
    ) -> claviron_runtime::registry::Result<RouteResponse> {
        assert_eq!(authenticated.unwrap().take::<u32>()?, 42);
        assert!(Arc::ptr_eq(
            &ctx.conn,
            request.extensions().get::<Arc<ConnCtx>>().unwrap()
        ));
        assert_eq!(request.uri().path(), ctx.path.as_ref());
        self.calls.fetch_add(1, Ordering::SeqCst);
        let upgrade = hyper::upgrade::on(&mut request);
        let tasks = self.tasks.clone();
        request
            .extensions()
            .get::<SessionSpawner>()
            .unwrap()
            .spawn(async move {
                let mut io = TokioIo::new(upgrade.await.unwrap());
                let mut data = [0; 4];
                io.read_exact(&mut data).await.unwrap();
                assert_eq!(&data, b"ping");
                io.write_all(b"pong").await.unwrap();
                io.shutdown().await.unwrap();
                tasks.send(()).unwrap();
            })
            .unwrap();
        Ok(Response::new(claviron_http::empty_body()))
    }
}

#[tokio::test]
async fn h2_connect_keeps_native_body_and_runs_shared_guards_before_auth() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::standalone("h2-dispatch-test", dir.path());
        let registry = Arc::new(H2ConnectRegistry::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let accepts = Arc::new(AtomicUsize::new(0));
        let (tasks, mut owned) = tokio::sync::mpsc::unbounded_channel();
        let handler =
            Arc::new(EchoConnect { calls: calls.clone(), accepts: accepts.clone(), tasks });
        for path in ["/socket", "/reject", "/blocked"] {
            registry.register_handler_for_host("app.test", path, handler.clone()).unwrap();
        }
        state.registry_ref().insert(registry).unwrap();
        let table = serde_yaml::from_str(
            "app.test: {root: public, transport: {h2: {connect_protocol: true}}}"
        )
        .unwrap();
        let host = resolve_sni_from_table(&table, Some("app.test")).unwrap();
        let access = Arc::new(crate::transport::VhostResolver::default());
        access.upsert_host(host.vhost_key.0.as_ref().clone(), host.vhost_config.clone());
        let connection = Arc::new(ConnCtx {
            state: Default::default(),
            listener: "test".parse().unwrap(),
            proto: Some(HttpProtocol::H2),
            remote: "10.0.0.1:1234".parse().unwrap(),
            local_ip: "127.0.0.1".parse().unwrap(),
            local_port: 8080,
            real_ip: None,
            country_code: None,
            tls: None
        });
        let config: RouterConfig = serde_yaml::from_str("deny: ['^/blocked']").unwrap();
        let host = HostService::new(
            AppKind::Bypass(BypassService::Noop),
            connection,
            host,
            HostIngress {
                proxy: Some(ProxyConfig {
                    trusted: vec!["10.0.0.0/8".parse().unwrap()],
                    headers: vec![http::HeaderName::from_static("x-real-ip")]
                }),
                deny: Arc::new(DenyService::from_set(config.deny_set()))
            },
            access.clone(),
            HttpPolicy::unlimited()
        );
        let (client_io, server_io) = tokio::io::duplex(8192);
        let sessions = SessionOwner::new("test".parse().unwrap());
        let gate = Gate::new(None, Duration::from_secs(5));
        let scope = sessions.bind(gate.try_enter().unwrap());
        let server = tokio::spawn(async move {
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .enable_connect_protocol()
                .serve_connection(
                    TokioIo::new(server_io),
                    RequestService::new(host, state, scope.sessions())
                )
                .await
        });
        let (mut sender, client) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake::<_, Full<Bytes>>(TokioIo::new(client_io))
            .await
            .unwrap();
        let client = tokio::spawn(client);
        let updated = Arc::new(
            serde_yaml::from_str("root: public\nacl: {'/socket': {policy: deny}}").unwrap()
        );
        access.upsert_host("app.test".parse().unwrap(), updated);
        let denied = Request::builder()
            .method(Method::CONNECT)
            .uri("http://app.test/socket")
            .extension(hyper::ext::Protocol::from_static("test"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = sender.send_request(denied).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        response.into_body().collect().await.unwrap();
        assert_eq!(accepts.load(Ordering::SeqCst), 0, "ACL precedes application authentication");
        for (policy, status, location) in [
            ("acl: {'/': {policy: deny}}", StatusCode::FORBIDDEN, None),
            (
                "redirect: {path: /socket, to: /elsewhere}",
                StatusCode::TEMPORARY_REDIRECT,
                Some("/elsewhere")
            )
        ] {
            access.upsert_host(
                "app.test".parse().unwrap(),
                Arc::new(serde_yaml::from_str(&format!("root: public\n{policy}")).unwrap())
            );
            let request = Request::builder()
                .method(Method::CONNECT)
                .uri("http://app.test/socket")
                .extension(hyper::ext::Protocol::from_static("test"))
                .body(Full::new(Bytes::new()))
                .unwrap();
            let response = sender.send_request(request).await.unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(
                response.headers().get(http::header::LOCATION).map(|v| v.to_str().unwrap()),
                location
            );
            response.into_body().collect().await.unwrap();
            assert_eq!(
                accepts.load(Ordering::SeqCst),
                0,
                "host replies precede CONNECT acceptance"
            );
        }
        access.upsert_host(
            "app.test".parse().unwrap(),
            Arc::new(serde_yaml::from_str("root: public").unwrap())
        );
        for (host, path, status) in [
            ("other.test", "/socket", StatusCode::MISDIRECTED_REQUEST),
            ("app.test", "/blocked", StatusCode::FORBIDDEN),
            ("app.test", "/reject", StatusCode::FORBIDDEN),
            ("app.test", "/socket", StatusCode::OK)
        ] {
            let req = Request::builder()
                .method(Method::CONNECT)
                .uri(format!("http://{host}{path}"))
                .header("x-real-ip", "203.0.113.9")
                .extension(hyper::ext::Protocol::from_static("test"))
                .body(Full::new(Bytes::new()))
                .unwrap();
            let mut response = sender.send_request(req).await.unwrap();
            assert_eq!(response.status(), status);
            if status == StatusCode::OK {
                let mut io = TokioIo::new(hyper::upgrade::on(&mut response).await.unwrap());
                assert_eq!(gate.count(), 1, "CONNECT shares the inbound connection slot");
                io.write_all(b"ping").await.unwrap();
                let mut data = [0; 4];
                io.read_exact(&mut data).await.unwrap();
                assert_eq!(&data, b"pong");
                drop(io);
                owned.recv().await.unwrap();
            } else {
                response.into_body().collect().await.unwrap();
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(accepts.load(Ordering::SeqCst), 2);
        drop(sender);
        client.abort();
        let _ = client.await;
        let _ = server.await;
        sessions.close().join().await;
        assert_eq!(gate.count(), 0);
    })
    .await
    .expect("test-owned H2 connection must finish");
}
