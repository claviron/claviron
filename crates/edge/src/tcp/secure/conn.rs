use std::net::SocketAddr;
use std::sync::Arc;

use crate::config::HostConfig;
use crate::deny::DenyService;
use crate::transport::{
    HostAppResolver, HostBinding, HostIngress, HostRejection, HostService,
    VhostCtx, VhostResolver,
};
use claviron_http::config::ListenerConfig;
use claviron_http::ext::{
    ClientAuthStatus, ConnCtx, TlsClientCertInfo, TlsInfo,
};
use claviron_http::session::SessionSpawner;
use claviron_runtime::AppState;
use claviron_tls::Proto;
use claviron_types::ssl::parsers::take_first_cert_from_der;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

use super::error::ConnectionError;
use crate::tcp::service::RequestService;

/// Native TLS metadata and SNI host binding for one H1/H2 connection.
///
/// Shared admission retains this host's captured policy and exact SNI authority.
/// A request for another name receives 421, even if it shares the certificate;
/// connection coalescing must not switch authentication or application policy.
pub(crate) struct TlsHostConnection {
    ctx: Arc<ConnCtx>,
    vctx: VhostCtx,
}

impl TlsHostConnection {
    /// Capture physical TLS metadata and the SNI host chosen by this generation.
    ///
    /// Missing, invalid or unhosted SNI rejects the connection.
    pub fn from_tls_stream(
        tls_stream: &TlsStream<TcpStream>,
        socket_addr: SocketAddr,
        local_addr: SocketAddr,
        listener: &ListenerConfig,
        vctx: VhostCtx,
    ) -> Result<Self, HostRejection> {
        // Extract SNI hostname from TLS handshake
        let server_name: Option<Arc<str>> =
            tls_stream.get_ref().1.server_name().map(Arc::from);

        // Binding was selected alongside authentication before the handshake.
        if !vctx.vhost_config.allows_listener(&listener.id) {
            return Err(HostRejection::NotHosted(
                server_name.as_deref().unwrap_or_default().to_owned(),
            ));
        }

        // The actual port the connection arrived on — already authoritative for
        // alt-svc / redirect targets without consulting `http.yaml`.
        let local_port = local_addr.port();

        let conn_ctx = Arc::new(ConnCtx {
            listener: listener.id.clone(),
            proto: None,
            remote: socket_addr,
            local_ip: local_addr.ip(),
            local_port,
            // A direct client is its own real address for the connection's
            // life; only behind a trusted proxy does it vary per request.
            real_ip: listener.proxy.is_none().then_some(socket_addr.ip()),
            tls: tls_info(tls_stream),
            country_code: crate::tcp::geo::country_code(socket_addr.ip()),
        });

        Ok(Self { ctx: conn_ctx, vctx })
    }

    /// Bind the shared host dispatcher to Hyper without re-reading live policy.
    pub fn create_service(
        &self,
        state: &AppState,
        proto: Proto,
        listener: &ListenerConfig,
        deny: Arc<DenyService>,
        sessions: SessionSpawner,
    ) -> Result<RequestService, ConnectionError> {
        let registry = state.registry_ref();

        let access = registry
            .resolve::<VhostResolver>()
            .ok_or(ConnectionError::HostPolicyUnavailable)?;

        let app = self
            .vctx
            .application
            .clone()
            .or_else(|| {
                registry
                    .resolve::<HostAppResolver>()
                    .and_then(|resolver| resolver.resolve(&self.vctx.vhost_key))
            })
            .ok_or(ConnectionError::ApplicationUnavailable)?;

        // Both values belong to the transport's accepted snapshot. Never
        // select proxy trust from a host or re-read the live HTTP provider.
        let proxy_cache = listener.proxy.clone();

        let mut conn_ctx = (*self.ctx).clone();
        conn_ctx.proto = Some(proto);

        let host = HostService::new(
            app,
            Arc::new(conn_ctx),
            self.vctx.clone(),
            HostIngress { proxy: proxy_cache, deny },
            access,
            listener.policy(),
        );
        Ok(RequestService::new(host, state.clone(), sessions))
    }

    pub fn host_config(&self) -> &HostConfig {
        self.vctx.vhost_config.as_ref()
    }

    /// Hold this connection's SNI host to the vhost it resolved to at
    /// acceptance. The binding wakes only when a vhost table is published and
    /// retires the connection when its host mapping or captured configuration
    /// is replaced; the request path never consults it.
    pub fn binding(
        &self,
        state: &AppState,
    ) -> Option<HostBinding> {
        state.registry_ref().resolve::<VhostResolver>().map(|resolver| {
            resolver.bind_config(
                Arc::clone(&self.vctx.hostname.0),
                Arc::clone(&self.vctx.vhost_key.0),
                self.vctx.vhost_config.clone(),
            )
        })
    }
}

fn tls_info(tls_stream: &TlsStream<TcpStream>) -> Option<TlsInfo> {
    let conn = tls_stream.get_ref().1;
    let protocol_version = conn.protocol_version();
    let cipher_suite =
        conn.negotiated_cipher_suite().map(|suite| suite.suite());
    let client_cert = conn
        .peer_certificates()
        .and_then(|certs| certs.first())
        .and_then(|cert| take_first_cert_from_der(cert.as_ref()).ok())
        .map(|cert| TlsClientCertInfo {
            subject_dn: cert.subject().to_string(),
            issuer_dn: cert.issuer().to_string(),
        });
    let client_auth = if client_cert.is_some() {
        ClientAuthStatus::Verified
    } else {
        ClientAuthStatus::NotProvided
    };

    Some(TlsInfo { protocol_version, cipher_suite, client_auth, client_cert })
}
