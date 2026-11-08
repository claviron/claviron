//! Prepare typed HTTP/vhost declarations and resolvers outside request timing.
//! Fixtures pass through the same YAML deserializers as application config.
//! No provider boot, listener, filesystem service or shared fleet is started.

use std::sync::Arc;

use claviron::edge::service::HostService;
use claviron::edge::transport::{
    VhostResolver, VhostTable, resolve_sni_from_table,
};
use claviron::http::config::{HttpSnapshot, ListenerConfig};
use claviron::http::ext::ConnCtx;
use claviron::http::listener::HttpProtocol;
use claviron::pipeline::plan::HostExecution;

use super::json;

pub struct Hosts {
    pub http: HttpSnapshot,
    pub vhosts: Arc<VhostResolver>,
}

impl Hosts {
    pub fn load() -> Self {
        Self::parse(
            include_str!("fixtures/http.yaml"),
            include_str!("fixtures/vhosts.yaml"),
        )
    }

    pub fn http1() -> Self {
        Self::parse(
            include_str!("fixtures/http1.yaml"),
            include_str!("fixtures/http1-vhosts.yaml"),
        )
    }

    /// The same host behind a TLS listener, for the concurrent `tls` mode.
    #[cfg(feature = "tls")]
    pub fn http1_tls() -> Self {
        Self::parse(
            include_str!("fixtures/http1-tls.yaml"),
            include_str!("fixtures/http1-tls-vhosts.yaml"),
        )
    }

    fn parse(
        http: &str,
        vhosts: &str,
    ) -> Self {
        let http: HttpSnapshot =
            serde_yaml::from_str(http).expect("valid benchmark HTTP fixture");
        let table: VhostTable = serde_yaml::from_str(vhosts)
            .expect("valid benchmark vhost fixture");
        table
            .validate_listeners(Some(&http))
            .expect("benchmark hosts reference configured listeners");
        let vhosts = Arc::new(VhostResolver::default());
        for (canonical, record) in table.canonical_entries() {
            vhosts.upsert_host(canonical.clone(), record.config.clone());
        }
        Self { http, vhosts }
    }

    pub fn plain_host(
        &self,
        app: HostExecution,
    ) -> HostService {
        let listener = self.listener("plain");
        let local =
            listener.listen[0].socket().expect("benchmark uses a TCP listener");
        let remote: std::net::SocketAddr = "192.0.2.1:1234".parse().unwrap();
        let connection = Arc::new(ConnCtx {
            transport: claviron_http::listener::ListenerTransport::Tcp,
            state: Default::default(),
            listener: listener.id.clone(),
            proto: Some(HttpProtocol::H1),
            remote,
            local_ip: local.ip(),
            local_port: local.port(),
            real_ip: Some(remote.ip()),
            country_code: None,
            tls: None,
        });
        let host =
            resolve_sni_from_table(&self.vhosts.snapshot(), Some(json::HOST))
                .expect("benchmark host is published");
        HostService::new(app, connection, host, listener.policy())
    }

    fn listener(
        &self,
        name: &str,
    ) -> &ListenerConfig {
        let id: claviron::http::ListenerId = name.parse().unwrap();
        self.http.listeners.get(&id).expect("benchmark listener is configured")
    }
}
