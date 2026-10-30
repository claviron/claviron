//! Prepare typed HTTP/vhost declarations and resolvers outside request timing.
//! Fixtures pass through the same YAML deserializers as application config.
//! No provider boot, listener, filesystem service or shared fleet is started.

use std::sync::Arc;

use claviron::edge::deny::DenyService;
use claviron::edge::pipeline::HostApp;
use claviron::edge::transport::{
    HostIngress, HostService, VhostResolver, VhostTable, resolve_sni_from_table,
};
use claviron::http::config::{HttpSnapshot, ListenerConfig};
use claviron::http::ext::{ConnCtx, TlsInfo};
use claviron::http::listener::HttpProtocol;

use super::json;

pub struct Hosts {
    pub http: HttpSnapshot,
    pub vhosts: Arc<VhostResolver>,
}

impl Hosts {
    pub fn load() -> Self {
        let http: HttpSnapshot =
            serde_yaml::from_str(include_str!("fixtures/http.yaml"))
                .expect("valid benchmark HTTP fixture");
        let table: VhostTable =
            serde_yaml::from_str(include_str!("fixtures/vhosts.yaml"))
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
        app: impl Into<HostApp>,
    ) -> HostService {
        self.bind(app, "plain", json::HOST, HttpProtocol::H1, None)
    }

    fn bind(
        &self,
        app: impl Into<HostApp>,
        listener: &str,
        hostname: &str,
        protocol: HttpProtocol,
        tls: Option<TlsInfo>,
    ) -> HostService {
        let listener = self.listener(listener);
        let local = listener.listen[0];
        let remote: std::net::SocketAddr = "192.0.2.1:1234".parse().unwrap();
        let connection = Arc::new(ConnCtx {
            state: Default::default(),
            listener: listener.id.clone(),
            proto: Some(protocol),
            remote,
            local_ip: local.ip(),
            local_port: local.port(),
            real_ip: listener.proxy.is_none().then_some(remote.ip()),
            country_code: None,
            tls,
        });
        let host =
            resolve_sni_from_table(&self.vhosts.snapshot(), Some(hostname))
                .expect("benchmark host is published");
        HostService::new(
            app,
            connection,
            host,
            HostIngress {
                proxy: listener.proxy.clone(),
                deny: Arc::new(DenyService::from_set(
                    self.http.router.deny_set(),
                )),
            },
            self.vhosts.clone(),
            listener.policy(),
        )
    }

    fn listener(
        &self,
        name: &str,
    ) -> &ListenerConfig {
        let id: claviron::http::ListenerId = name.parse().unwrap();
        self.http.listeners.get(&id).expect("benchmark listener is configured")
    }
}

#[cfg(feature = "tls")]
pub struct Tls {
    pub hosts: Hosts,
    pub certificates: Arc<claviron::tls::CertResolver>,
    roots: rustls::RootCertStore,
}

#[cfg(feature = "tls")]
pub struct TlsConfigs {
    pub server: Arc<rustls::ServerConfig>,
    pub client: Arc<rustls::ClientConfig>,
}

#[cfg(feature = "tls")]
impl Tls {
    pub fn load() -> Self {
        let hosts = Hosts::load();
        let identity = super::certificate::generate(json::HOST);
        let certificates = Arc::new(claviron::tls::CertResolver::new(
            std::collections::HashMap::from([(
                json::HOST.to_owned(),
                identity.key.clone(),
            )]),
            identity.key,
        ));
        let mut roots = rustls::RootCertStore::empty();
        roots.add(identity.certificate).expect("trust benchmark certificate");
        Self { hosts, certificates, roots }
    }

    pub fn configs(
        &self,
        protocol: HttpProtocol,
    ) -> TlsConfigs {
        assert!(matches!(protocol, HttpProtocol::H1 | HttpProtocol::H2));
        let claviron::http::config::ListenerSettings::Tcp(listener) =
            &self.hosts.listener("tls").settings
        else {
            panic!("benchmark TLS listener must use TCP");
        };
        assert!(matches!(
            listener.security,
            claviron::http::config::TcpSecurity::Tls { .. }
        ));
        assert!(listener.protocols.contains(&protocol));
        let server = claviron::tls::create_tls_config(
            &[protocol],
            self.certificates.clone(),
        )
        .expect("prepare benchmark server TLS config");
        let mut client = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("safe benchmark client protocol versions")
        .with_root_certificates(self.roots.clone())
        .with_no_client_auth();
        client.alpn_protocols = server.alpn_protocols.clone();
        TlsConfigs { server: Arc::new(server), client: Arc::new(client) }
    }

    /// Call only after a real TLS handshake; identity comes from that session.
    pub fn host(
        &self,
        app: impl Into<HostApp>,
        connection: &rustls::ServerConnection,
    ) -> HostService {
        let protocol = match connection.alpn_protocol() {
            Some(b"http/1.1") => HttpProtocol::H1,
            Some(b"h2") => HttpProtocol::H2,
            other => panic!("unexpected benchmark ALPN: {other:?}"),
        };
        let tls = TlsInfo {
            protocol_version: connection.protocol_version(),
            cipher_suite: connection
                .negotiated_cipher_suite()
                .map(|suite| suite.suite()),
            client_auth: claviron::http::ext::ClientAuthStatus::NotProvided,
            client_cert: None,
        };
        self.hosts.bind(
            app,
            "tls",
            connection.server_name().expect("benchmark SNI was negotiated"),
            protocol,
            Some(tls),
        )
    }
}
