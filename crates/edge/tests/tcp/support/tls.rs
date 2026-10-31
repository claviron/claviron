//! Loopback fixture for the production TLS/H1/H2 connection driver.
//!
//! Only TLS configuration/session storage is injected: no fleet SHM, machine
//! trust store, DNS, database, filesystem config or external application boots.
//! The real acceptor, ALPN decision, Hyper builders, admission and drain run.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::transport::{
    OtherHandler, OtherService, HostAppRegistry, HostAppResolver,
};
use bytes::Bytes;
use claviron_http::session::SessionOwner;
use claviron_http::{RouteBody, RouteFuture, bytes_body};
use claviron_runtime::AppState;
use claviron_runtime::gate::Gate;
use http::{Request, Response};
use http_body_util::BodyExt;
use tokio::task::JoinSet;
use tokio_rustls::client::TlsStream;

use super::*;
use crate::tcp::secure::HttpsServer;
use crate::tcp::secure::server::ServerSnapshot;

pub(super) const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct CountRequests {
    count: Arc<AtomicUsize>,
    handler: Option<Arc<dyn OtherHandler>>,
}

impl OtherHandler for CountRequests {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        self.count.fetch_add(1, Ordering::SeqCst);
        if let Some(handler) = &self.handler {
            return handler.call(request);
        }
        Box::pin(async move {
            // Test inputs are small; consume their body to exercise keepalive
            // framing without allowing the fixture to buffer arbitrary data.
            let mut body = request.into_body();
            let mut bytes = 0;
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame.unwrap().into_data() {
                    bytes += data.len();
                    assert!(bytes <= 1024, "bounded fixture request body");
                }
            }
            Ok(Response::new(bytes_body(Bytes::from_static(b"healthy"))))
        })
    }
}

pub(super) struct TlsFixture {
    listener: TcpListener,
    server: Arc<HttpsServer>,
    snapshot: Arc<ServerSnapshot>,
    roots: rustls::RootCertStore,
    provider: Arc<rustls::crypto::CryptoProvider>,
    sessions: SessionOwner,
    tasks: JoinSet<()>,
    pub(super) calls: Arc<AtomicUsize>,
    pub(super) gate: Gate,
}

impl TlsFixture {
    pub(super) async fn new() -> Self {
        Self::with_host("app.test", None).await
    }

    pub(super) async fn new_default_host() -> Self {
        Self::with_host("default", None).await
    }

    pub(super) async fn with_handler(handler: Arc<dyn OtherHandler>) -> Self {
        Self::with_host("app.test", Some(handler)).await
    }

    async fn with_host(
        host: &'static str,
        handler: Option<Arc<dyn OtherHandler>>,
    ) -> Self {
        let state = AppState::standalone("tls-wire-test", ".");
        let live = Arc::new(VhostResolver::default());
        live.upsert_host(
            host.parse().unwrap(),
            Arc::new(serde_yaml::from_str("root: public").unwrap()),
        );
        state.registry_ref().register(live.clone()).unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let factories = Arc::new(HostAppRegistry::default());
        let handler = Arc::new(CountRequests { count: calls.clone(), handler });
        factories.register_other(host, move |_, _, _| {
            Ok(OtherService::Handler(handler.clone()))
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

        let http = serde_yaml::from_str::<HttpYaml>(
            "listeners: {tls: {transport: tcp, tls: true, protocols: [h2, h1], listen: '127.0.0.1:0', max_connections: 1, requests: {max_body_size: off, body_read_timeout: off}}}"
        ).unwrap().resolve().unwrap();
        let listener_config = http.listeners.values().next().unwrap().clone();
        let ListenerSettings::Tcp(config) = &listener_config.settings else {
            unreachable!()
        };

        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let cert = rcgen::generate_simple_self_signed(vec!["app.test".into()])
            .unwrap();
        let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let mut tls = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
            .unwrap();
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        tls.session_storage = rustls::server::ServerSessionMemoryCache::new(8);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let acceptor = TlsAccept(Arc::new(TlsConfigResolver {
            listener: None,
            table: live.snapshot(),
            live,
            by_host: HashMap::from([(host.parse().unwrap(), Arc::new(tls))]),
        }));
        let snapshot = Arc::new(ServerSnapshot {
            listener: listener_config.clone(),
            config: config.clone(),
            deny: Arc::new(crate::deny::DenyService::from_set(
                http.router.deny_set(),
            )),
            tls_accept: acceptor,
        });
        Self {
            listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            server: Arc::new(HttpsServer::empty(state)),
            snapshot,
            roots,
            provider,
            sessions: SessionOwner::new(listener_config.id.clone()),
            tasks: JoinSet::new(),
            calls,
            gate: Gate::new(Some(1), TEST_TIMEOUT),
        }
    }

    pub(super) async fn tcp(&mut self) -> TcpStream {
        let client = TcpStream::connect(self.listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, peer) = self.listener.accept().await.unwrap();
        let scope = self.sessions.bind(self.gate.enter().await.unwrap());
        self.tasks.spawn(self.server.clone().handle_connection(
            self.snapshot.clone(),
            server,
            peer,
            scope,
        ));
        client
    }

    pub(super) fn h2_max_streams(
        &mut self,
        max: u32,
    ) {
        assert!(
            self.tasks.is_empty(),
            "configure before accepting connections"
        );
        Arc::get_mut(&mut self.snapshot)
            .unwrap()
            .config
            .h2
            .max_concurrent_streams = max;
    }

    pub(super) async fn connect(
        &mut self,
        name: &str,
        alpn: &[u8],
    ) -> io::Result<TlsStream<TcpStream>> {
        let mut config =
            rustls::ClientConfig::builder_with_provider(self.provider.clone())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(self.roots.clone())
                .with_no_client_auth();
        config.alpn_protocols = vec![alpn.to_vec()];
        TlsConnector::from(Arc::new(config))
            .connect(
                ServerName::try_from(name.to_owned()).unwrap(),
                self.tcp().await,
            )
            .await
    }

    pub(super) async fn reap_connection(&mut self) {
        self.tasks.join_next().await.expect("owned connection").unwrap();
        assert_eq!(
            self.gate.count(),
            0,
            "completed connection must release its slot"
        );
    }

    pub(super) async fn finish(mut self) {
        // The enclosing test deadline bounds failures; it is not production
        // grace policy. Normal teardown must join everything cooperatively.
        self.gate.graceful_shutdown(None);
        while let Some(result) = self.tasks.join_next().await {
            result.unwrap();
        }
        self.sessions.close().join().await;
        assert_eq!(self.gate.count(), 0);
    }
}
