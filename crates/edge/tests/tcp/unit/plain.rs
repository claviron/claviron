use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::config::HostConfig;
use crate::transport::{
    HostAppRegistry, HostAppResolver, OtherHandler, OtherService,
};
use axum::Router;
use axum::routing::any;
use bytes::Bytes;
use claviron_http::config::{HttpYaml, RouterConfig};
use claviron_http::ext::{ConnCtx, HostKey, HostName, RealIpExt};
use claviron_http::session::{SessionOwner, SessionSpawner};
use claviron_http::{RouteBody, RouteFuture, boxed_body};
use claviron_runtime::gate::Gate;
use http::uri::Scheme;
use http::{Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::*;

const TIMEOUT: Duration = Duration::from_secs(5);

#[path = "plain/framing.rs"]
mod framing;

#[path = "plain/authority.rs"]
mod authority;

#[path = "plain/timeout.rs"]
mod timeout;

use crate::tcp::framing_cases;

fn describe<B>(request: &Request<B>) -> String {
    let ctx = request.extensions().get::<Arc<ConnCtx>>().unwrap();
    let key = request.extensions().get::<HostKey>().unwrap();
    let name = request.extensions().get::<HostName>().unwrap();
    let ip = request.extensions().get::<RealIpExt>().unwrap();
    assert_eq!(request.extensions().get::<Scheme>(), Some(&Scheme::HTTP));
    assert_eq!(request.extensions().get::<ListenerId>(), Some(&ctx.listener));
    assert_eq!(ctx.real_ip, Some(ip.0));
    assert_eq!(ctx.scheme(), Scheme::HTTP);
    assert!(ctx.tls.is_none());
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        key.0,
        name.0,
        request.uri(),
        ip.0,
        ctx.remote,
        ctx.local_ip,
        ctx.local_port
    )
}

struct Echo(Arc<AtomicUsize>);
impl OtherHandler for Echo {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let context = describe(&request);
            let body = request.into_body().collect().await.unwrap().to_bytes();
            Ok(Response::new(boxed_body(Full::new(Bytes::from(format!(
                "{context}|{}",
                String::from_utf8_lossy(&body)
            ))))))
        })
    }
}

struct Fixture {
    state: AppState,
    vhosts: Arc<VhostResolver>,
    snapshot: Arc<ServerSnapshot>,
    builds: Arc<AtomicUsize>,
    host_builds: [Arc<AtomicUsize>; 2],
    calls: Arc<AtomicUsize>,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::standalone("http-test", dir.path());
        let vhosts = Arc::new(VhostResolver::default());
        for (name, yaml) in [
            (
                "app.test",
                "root: public\naliases: [alias.test]\ntransport: {non_ssl: serve}",
            ),
            ("other.test", "root: public\ntransport: {non_ssl: serve}"),
        ] {
            vhosts.upsert_host(
                name.parse().unwrap(),
                Arc::new(serde_yaml::from_str(yaml).unwrap()),
            );
        }
        state.registry_ref().register(vhosts.clone()).unwrap();
        let factories = Arc::new(HostAppRegistry::default());
        let builds = Arc::new(AtomicUsize::new(0));
        let host_builds =
            std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
        let calls = Arc::new(AtomicUsize::new(0));
        let count = builds.clone();
        let host_count = Arc::clone(&host_builds[0]);
        let entries = calls.clone();
        factories.register_router("app.test", move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            host_count.fetch_add(1, Ordering::SeqCst);
            let entries = entries.clone();
            Router::new().fallback(any(
                move |request: Request<axum::body::Body>| {
                    entries.fetch_add(1, Ordering::SeqCst);
                    async move { describe(&request) }
                },
            ))
        });
        let count = builds.clone();
        let host_count = Arc::clone(&host_builds[1]);
        let entries = calls.clone();
        factories.register_other("other.test", move |_, _, _| {
            count.fetch_add(1, Ordering::SeqCst);
            host_count.fetch_add(1, Ordering::SeqCst);
            Ok(OtherService::Handler(Arc::new(Echo(entries.clone()))))
        });
        state.registry_ref().insert(factories).unwrap();
        let apps = Arc::new(HostAppResolver::default());
        claviron_runtime::AnyComponent::boot(
            apps.as_ref(),
            state.registry_ref(),
            &state,
        )
        .await
        .unwrap();
        state.registry_ref().register(apps).unwrap();

        let config: HttpYaml = serde_yaml::from_str(
            "listeners:\n  local:\n    transport: tcp\n    tls: false\n    listen: '127.0.0.1:8080'\n    protocols: h1\n    requests: {max_body_size: off, body_read_timeout: off}\n    proxy:\n      trusted: ['127.0.0.0/8']\n      headers: [x-real-ip]"
        ).unwrap();
        let resolved = config.resolve().unwrap();
        let listener =
            resolved.listeners[&"local".parse::<ListenerId>().unwrap()].clone();
        let ListenerSettings::Tcp(config) = &listener.settings else {
            unreachable!()
        };
        let TcpSecurity::Cleartext { redirect_port, acme } = &config.security
        else {
            unreachable!("fixture listener is cleartext")
        };
        let redirect_port = *redirect_port;
        assert!(!acme);
        let (tcp, h1) = (config.tcp.clone(), config.h1.clone());
        let snapshot = Arc::new(ServerSnapshot {
            listener,
            tcp,
            h1,
            redirect_port,
            http01_directory: None,
            deny: Arc::new(DenyService::empty()),
        });
        Self { state, vhosts, snapshot, builds, host_builds, calls, _dir: dir }
    }

    fn assert_builds(
        &self,
        expected: [usize; 2],
    ) {
        assert_eq!(
            self.host_builds
                .each_ref()
                .map(|count| count.load(Ordering::SeqCst)),
            expected,
            "factory counts for [app.test, other.test]"
        );
        assert_eq!(
            self.builds.load(Ordering::SeqCst),
            expected.iter().sum::<usize>()
        );
    }

    fn host(
        &self,
        name: &str,
        policy: &str,
    ) {
        let config: HostConfig =
            serde_yaml::from_str(&format!("root: public\n{policy}")).unwrap();
        self.vhosts.upsert_host(name.parse().unwrap(), Arc::new(config));
    }

    async fn raw(&self) -> RawWire {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = listener.local_addr().unwrap();
        let client = TcpStream::connect(local).await.unwrap();
        let peer = client.local_addr().unwrap();
        let (stream, accepted_peer) = listener.accept().await.unwrap();
        assert_eq!(peer, accepted_peer);
        let gate = Gate::new(None, TIMEOUT);
        let sessions = SessionOwner::new(self.snapshot.listener.id.clone());
        let server = tokio::spawn(HttpServer::handle_connection(
            stream,
            accepted_peer,
            self.snapshot.clone(),
            self.vhosts.clone(),
            self.state.clone(),
            sessions.bind(gate.enter().await.unwrap()),
        ));
        RawWire { stream: client, server, gate, sessions, peer, local }
    }

    async fn connect(&self) -> Wire {
        let RawWire { stream, server, gate, sessions, peer, local } =
            self.raw().await;
        let (sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .unwrap();
        let client =
            tokio::spawn(
                async move { connection.with_upgrades().await.unwrap() },
            );
        Wire { sender, server, client, gate, sessions, peer, local }
    }
}

struct RawWire {
    stream: TcpStream,
    server: JoinHandle<()>,
    gate: Gate,
    sessions: SessionOwner,
    peer: SocketAddr,
    local: SocketAddr,
}

impl RawWire {
    async fn finish(self) {
        drop(self.stream);
        tokio::time::timeout(TIMEOUT, self.server).await.unwrap().unwrap();
        tokio::time::timeout(TIMEOUT, self.sessions.close().join())
            .await
            .unwrap();
        assert_eq!(self.gate.count(), 0, "connection permit must be released");
    }
}

struct Wire {
    sender: SendRequest<Full<Bytes>>,
    server: JoinHandle<()>,
    client: JoinHandle<()>,
    gate: Gate,
    sessions: SessionOwner,
    peer: SocketAddr,
    local: SocketAddr,
}

impl Wire {
    async fn send(
        &mut self,
        request: Request<Full<Bytes>>,
    ) -> Response<Incoming> {
        tokio::time::timeout(TIMEOUT, self.sender.send_request(request))
            .await
            .unwrap()
            .unwrap()
    }

    async fn finish(self) {
        drop(self.sender);
        tokio::time::timeout(TIMEOUT, self.client).await.unwrap().unwrap();
        tokio::time::timeout(TIMEOUT, self.server).await.unwrap().unwrap();
        tokio::time::timeout(TIMEOUT, self.sessions.close().join())
            .await
            .unwrap();
        assert_eq!(self.gate.count(), 0, "connection permit must be released");
    }
}

fn request(
    host: &str,
    uri: &str,
) -> Request<Full<Bytes>> {
    Request::builder()
        .uri(uri)
        .header(header::HOST, host)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

#[tokio::test]
async fn unknown_host_churn_returns_empty_404_without_application_dispatch() {
    tokio::time::timeout(TIMEOUT, async {
        let fixture = Fixture::new().await;
        let table = fixture.vhosts.snapshot();
        let mut wire = fixture.connect().await;
        for index in 0..32 {
            let response = wire
                .send(request(&format!("unhosted-{index}.test"), "/"))
                .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert!(
                response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .is_empty()
            );
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        assert!(
            Arc::ptr_eq(&table, &fixture.vhosts.snapshot()),
            "request names cannot publish host state"
        );
        for host in ["app.test", "other.test"] {
            let response = wire.send(request(host, "/")).await;
            assert_eq!(response.status(), StatusCode::OK);
            response.into_body().collect().await.unwrap();
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
        wire.finish().await;
    })
    .await
    .expect("unknown-name churn must leave healthy traffic usable");
}

#[tokio::test]
async fn early_deny_allows_both_apps_and_keeps_other_host_guards() {
    let mut fixture = Fixture::new().await;
    let router: RouterConfig =
        serde_yaml::from_str("deny: ['^/blocked(?:/|$)']").unwrap();
    Arc::get_mut(&mut fixture.snapshot).unwrap().deny =
        Arc::new(DenyService::from_set(router.deny_set()));
    let mut wire = fixture.connect().await;
    for host in ["app.test", "other.test"] {
        for _ in 0..2 {
            let response =
                wire.send(request(host, "/blocked-other?raw=%2F")).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert!(
                !response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .is_empty()
            );
        }
        // Passing path deny must not bypass the separate host upgrade policy.
        let mut req = request(host, "/socket");
        req.headers_mut().insert(header::UPGRADE, "echo".parse().unwrap());
        req.headers_mut()
            .insert(header::CONNECTION, "upgrade".parse().unwrap());
        let response = wire.send(req).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        response.into_body().collect().await.unwrap();
    }
    assert_eq!(fixture.builds.load(Ordering::SeqCst), 2);
    wire.finish().await;
}

#[tokio::test]
async fn early_deny_drops_before_host_resolution_and_non_ssl_actions() {
    let mut fixture = Fixture::new().await;
    let router: RouterConfig =
        serde_yaml::from_str("deny: ['^/blocked']").unwrap();
    Arc::get_mut(&mut fixture.snapshot).unwrap().deny =
        Arc::new(DenyService::from_set(router.deny_set()));
    fixture.host("redirect.test", "transport: {non_ssl: redirect}");
    fixture.host("reject.test", "transport: {non_ssl: reject}");
    for host in [
        "app.test",
        "other.test",
        "unknown.test",
        "user@app.test",
        "redirect.test",
        "reject.test",
    ] {
        for upgrade in [false, true] {
            let mut wire = fixture.connect().await;
            let mut req = request(host, "/blocked?raw=%2F");
            if upgrade {
                req.headers_mut()
                    .insert(header::UPGRADE, "echo".parse().unwrap());
                req.headers_mut()
                    .insert(header::CONNECTION, "upgrade".parse().unwrap());
            }
            let response =
                tokio::time::timeout(TIMEOUT, wire.sender.send_request(req))
                    .await
                    .unwrap();
            assert!(
                response.is_err(),
                "blocked bot traffic must be dropped without an HTTP reply"
            );
            wire.finish().await;
        }
    }
}

#[tokio::test]
async fn path_acl_reload_covers_axum_and_other_on_the_same_keepalive_connection()
 {
    let fixture = Fixture::new().await;
    let mut builds = [1, 1];
    fixture.assert_builds(builds);
    let mut wire = fixture.connect().await;
    for (index, host) in ["app.test", "other.test"].into_iter().enumerate() {
        let before = wire.send(request(host, "/admin")).await;
        assert_eq!(before.status(), StatusCode::OK);
        before.into_body().collect().await.unwrap();
        fixture.assert_builds(builds);
        fixture.host(host, "transport: {non_ssl: serve}\nacl:\n  '/admin': {policy: deny}\n  '/admin/public': {policy: allow}");
        builds[index] += 1;
        // Publication rebuilds only the changed host, before any request.
        fixture.assert_builds(builds);
        for (path, status) in [
            ("/admin", StatusCode::FORBIDDEN),
            ("/adm%69n", StatusCode::FORBIDDEN),
            ("/admin/public", StatusCode::OK),
            ("/administrator", StatusCode::OK),
        ] {
            let response = wire.send(request(host, path)).await;
            assert_eq!(response.status(), status, "{host}{path}");
            response.into_body().collect().await.unwrap();
            fixture.assert_builds(builds);
        }
    }
    wire.finish().await;
}

#[tokio::test]
async fn host_replies_reach_both_apps_before_their_routes_on_keepalive() {
    let fixture = Fixture::new().await;
    let mut builds = [1, 1];
    fixture.assert_builds(builds);
    let mut wire = fixture.connect().await;
    for (index, host) in ["app.test", "other.test"].into_iter().enumerate() {
        let calls = fixture.calls.load(Ordering::SeqCst);
        fixture.host(host, "transport: {non_ssl: serve}\nredirect: {path: /old, code: 308, to: 'https://new.test$uri?$args'}");
        builds[index] += 1;
        fixture.assert_builds(builds);
        let response = wire.send(request(host, "/old?raw=%2F")).await;
        assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            response.headers()[header::LOCATION],
            "https://new.test/old?raw=%2F"
        );
        assert!(
            response.into_body().collect().await.unwrap().to_bytes().is_empty()
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), calls);
        fixture.assert_builds(builds);
        let response = wire.send(request(host, "/older")).await;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();
        assert_eq!(fixture.calls.load(Ordering::SeqCst), calls + 1);
        fixture.assert_builds(builds);
        fixture.host(
            host,
            "transport: {non_ssl: serve}\nacl: {'/': {policy: deny}}\nredirect: {to: /new}"
        );
        builds[index] += 1;
        fixture.assert_builds(builds);
        let response = wire.send(request(host, "/old")).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(!response.headers().contains_key(header::LOCATION));
        response.into_body().collect().await.unwrap();
        assert_eq!(fixture.calls.load(Ordering::SeqCst), calls + 1);
        fixture.assert_builds(builds);
    }
    wire.finish().await;
}

#[tokio::test]
async fn keepalive_selects_each_host_and_reuses_built_axum_and_other_apps() {
    let fixture = Fixture::new().await;
    let mut wire = fixture.connect().await;
    for (host, canonical, ip, suffix) in [
        ("alias.test", "app.test", "203.0.113.1", ""),
        ("other.test", "other.test", "203.0.113.2", "|payload"),
        ("app.test", "app.test", "203.0.113.3", ""),
    ] {
        let mut req = request(host, "/original?q=1");
        req.headers_mut().insert("x-real-ip", ip.parse().unwrap());
        req.headers_mut().insert("x-forwarded-proto", "https".parse().unwrap());
        if host == "other.test" {
            *req.method_mut() = http::Method::POST;
            *req.body_mut() = Full::new(Bytes::from_static(b"payload"));
        }
        let response = wire.send(req).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            body,
            format!(
                "{canonical}|{host}|/original?q=1|{ip}|{}|{}|{}{suffix}",
                wire.peer,
                wire.local.ip(),
                wire.local.port()
            )
        );
    }
    assert_eq!(
        fixture.builds.load(Ordering::SeqCst),
        2,
        "factories run once per host, not per request"
    );
    wire.finish().await;
}

#[tokio::test]
async fn plaintext_policy_and_client_auth_fail_closed() {
    let fixture = Fixture::new().await;
    let mut builds = [1, 1];
    fixture.assert_builds(builds);
    for (policy, expected) in [
        ("transport: {non_ssl: reject}", StatusCode::FORBIDDEN),
        (
            "transport: {non_ssl: serve}\nlisteners: []",
            StatusCode::MISDIRECTED_REQUEST,
        ),
        (
            "transport: {non_ssl: serve, protocols: [h2, h3]}",
            StatusCode::MISDIRECTED_REQUEST,
        ),
        (
            "transport: {non_ssl: serve, protocols: []}",
            StatusCode::MISDIRECTED_REQUEST,
        ),
        (
            "transport: {non_ssl: serve}\ntls: {client_auth: {mode: required}}",
            StatusCode::FORBIDDEN,
        ),
        (
            "transport: {non_ssl: serve}\ntls: {require_alpn: true}",
            StatusCode::OK,
        ),
        ("", StatusCode::PERMANENT_REDIRECT),
    ] {
        fixture.host("app.test", policy);
        builds[0] += 1;
        fixture.assert_builds(builds);
        let mut wire = fixture.connect().await;
        let response = wire.send(request("app.test:8080", "/path?raw=1")).await;
        assert_eq!(response.status(), expected, "{policy}");
        if expected == StatusCode::PERMANENT_REDIRECT {
            assert_eq!(
                response.headers()[header::LOCATION],
                "https://app.test/path?raw=1"
            );
        }
        response.into_body().collect().await.unwrap();
        wire.finish().await;
        fixture.assert_builds(builds);
    }
    fixture.host("missing.test", "transport: {non_ssl: serve}");
    // New hosts receive the default file router during publication, even
    // without a registered factory. A missing file is 404, not a missing app.
    fixture.assert_builds(builds);
    let calls = fixture.calls.load(Ordering::SeqCst);
    let mut wire = fixture.connect().await;
    for (host, status) in [
        ("unknown.test", StatusCode::NOT_FOUND),
        ("missing.test", StatusCode::NOT_FOUND),
    ] {
        let response = wire.send(request(host, "/")).await;
        assert_eq!(response.status(), status);
        response.into_body().collect().await.unwrap();
        assert_eq!(fixture.calls.load(Ordering::SeqCst), calls);
        fixture.assert_builds(builds);
    }
    wire.finish().await;
}

#[tokio::test]
async fn upgrade_permission_is_checked_for_each_keepalive_host() {
    let fixture = Fixture::new().await;
    fixture
        .host("other.test", "transport: {non_ssl: serve, h1: {upgrade: true}}");
    let mut wire = fixture.connect().await;
    for (host, expected) in [
        ("app.test", StatusCode::FORBIDDEN),
        ("other.test", StatusCode::OK),
        ("app.test", StatusCode::FORBIDDEN),
    ] {
        let mut req = request(host, "/");
        req.headers_mut().insert(header::UPGRADE, "websocket".parse().unwrap());
        req.headers_mut()
            .insert(header::CONNECTION, "upgrade".parse().unwrap());
        let response = wire.send(req).await;
        // An enabled policy permits the handler to decide. It does not turn
        // an ordinary response into an upgrade automatically.
        assert_eq!(response.status(), expected);
        response.into_body().collect().await.unwrap();
    }
    wire.finish().await;
}

#[tokio::test]
async fn invalid_request_authority_never_reaches_a_host_app() {
    let fixture = Fixture::new().await;
    for (host, uri) in [
        ("user@app.test", "/"),
        ("app.test", "http://user@app.test/"),
        ("app.test", "https://app.test/"),
    ] {
        let mut wire = fixture.connect().await;
        let response = wire.send(request(host, uri)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        response.into_body().collect().await.unwrap();
        wire.finish().await;
    }
}

#[tokio::test]
async fn enabled_host_handler_receives_real_upgraded_io() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let fixture = Fixture::new().await;
    fixture
        .host("app.test", "transport: {non_ssl: serve, h1: {upgrade: true}}");
    let (tasks, mut owned) = tokio::sync::mpsc::unbounded_channel();
    fixture
        .state
        .registry_ref()
        .resolve::<HostAppRegistry>()
        .unwrap()
        .register_router("app.test", move |_| {
            let tasks = tasks.clone();
            Router::new().fallback(any(
                move |mut request: Request<axum::body::Body>| {
                    let tasks = tasks.clone();
                    async move {
                        let upgrade = hyper::upgrade::on(&mut request);
                        request
                            .extensions()
                            .get::<SessionSpawner>()
                            .unwrap()
                            .spawn(async move {
                                let mut io =
                                    TokioIo::new(upgrade.await.unwrap());
                                let mut bytes = [0u8; 4];
                                io.read_exact(&mut bytes).await.unwrap();
                                assert_eq!(&bytes, b"ping");
                                io.write_all(b"pong").await.unwrap();
                                io.shutdown().await.unwrap();
                                tasks.send(()).unwrap();
                            })
                            .unwrap();
                        Response::builder()
                            .status(StatusCode::SWITCHING_PROTOCOLS)
                            .header(header::UPGRADE, "echo")
                            .header(header::CONNECTION, "upgrade")
                            .body(axum::body::Body::empty())
                            .unwrap()
                    }
                },
            ))
        });
    fixture
        .state
        .registry_ref()
        .resolve::<HostAppResolver>()
        .unwrap()
        .reload(&fixture.state)
        .await
        .unwrap();
    let mut wire = fixture.connect().await;
    let mut req = request("app.test", "/socket");
    req.headers_mut().insert(header::UPGRADE, "echo".parse().unwrap());
    req.headers_mut().insert(header::CONNECTION, "upgrade".parse().unwrap());
    let mut response = wire.send(req).await;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let upgraded =
        tokio::time::timeout(TIMEOUT, hyper::upgrade::on(&mut response))
            .await
            .unwrap()
            .unwrap();
    let mut io = TokioIo::new(upgraded);
    // Hyper's H1 task has handed off the IO, but the session still owns the slot.
    tokio::time::timeout(TIMEOUT, &mut wire.server).await.unwrap().unwrap();
    assert_eq!(wire.gate.count(), 1);
    io.write_all(b"ping").await.unwrap();
    let mut bytes = [0u8; 4];
    tokio::time::timeout(TIMEOUT, io.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, b"pong");
    drop(io);
    tokio::time::timeout(TIMEOUT, owned.recv()).await.unwrap().unwrap();
    drop(wire.sender);
    tokio::time::timeout(TIMEOUT, wire.client).await.unwrap().unwrap();
    tokio::time::timeout(TIMEOUT, wire.sessions.close().join()).await.unwrap();
    assert_eq!(wire.gate.count(), 0);
}

#[cfg(feature = "acme")]
#[tokio::test]
async fn configured_challenge_short_circuits_host_and_deny_policy() {
    let mut fixture = Fixture::new().await;
    fixture.host("app.test", "transport: {non_ssl: reject}");
    let directory = tempfile::tempdir().unwrap();
    let token = "test-token";
    std::fs::write(directory.path().join(token), "challenge-proof").unwrap();
    let snapshot = Arc::get_mut(&mut fixture.snapshot).unwrap();
    snapshot.http01_directory = Some(Arc::new(directory.path().to_owned()));
    let router: RouterConfig = serde_yaml::from_str("deny: ['^/']").unwrap();
    snapshot.deny = Arc::new(DenyService::from_set(router.deny_set()));
    let mut wire = fixture.connect().await;
    let response = wire
        .send(request(
            "app.test",
            &format!("/.well-known/acme-challenge/{token}"),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "challenge-proof"
    );
    wire.finish().await;
}
