use std::net::SocketAddr;
use std::sync::Arc;

use crate::config::HostConfig;
use crate::transport::{
    HostBinding, HostRejection, HostService, VhostCtx, VhostResolver,
};
use claviron_http::config::ListenerConfig;
use claviron_http::ext::{ClientAuthStatus, ConnCtx, TlsInfo};
use claviron_runtime::AppState;
use claviron_tls::Proto;
use claviron_types::ssl::peer::peer_identity;
use tokio_rustls::server::TlsStream;

use super::error::ConnectionError;

/// Native TLS metadata and SNI host binding for one H1/H2 connection.
///
/// Shared admission retains this host's captured policy and exact SNI authority.
/// A request for another name receives 421, even if it shares the certificate;
/// connection coalescing must not switch authentication or application policy.
pub(crate) struct TlsHostConnection {
    /// Owned until the protocol is known; shared once, complete.
    ctx: ConnCtx,
    vctx: VhostCtx,
}

impl TlsHostConnection {
    /// Capture physical TLS metadata and the SNI host chosen by this generation.
    ///
    /// Missing, invalid or unhosted SNI rejects the connection.
    pub fn from_tls_stream<IO>(
        tls_stream: &TlsStream<IO>,
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

        let conn_ctx = ConnCtx {
            state: Default::default(),
            listener: listener.id.clone(),
            proto: None,
            remote: socket_addr,
            local_ip: local_addr.ip(),
            local_port,
            real_ip: None,
            tls: tls_info(tls_stream),
            country_code: crate::tcp::geo::country_code(socket_addr.ip()),
        };

        Ok(Self { ctx: conn_ctx, vctx })
    }

    /// Bind the shared host dispatcher to Hyper without re-reading live policy.
    /// The negotiated protocol completes the connection facts, which are
    /// shared from here on.
    pub fn bind_host(
        self,
        proto: Proto,
        listener: &ListenerConfig,
    ) -> Result<HostService, ConnectionError> {
        let Self { mut ctx, mut vctx } = self;
        let app = vctx
            .application
            .take()
            .ok_or(ConnectionError::ApplicationUnavailable)?;
        ctx.proto = Some(proto);
        Ok(HostService::new(app, Arc::new(ctx), vctx, listener.policy()))
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

fn tls_info<IO>(tls_stream: &TlsStream<IO>) -> Option<TlsInfo> {
    let conn = tls_stream.get_ref().1;
    let protocol_version = conn.protocol_version();
    let cipher_suite =
        conn.negotiated_cipher_suite().map(|suite| suite.suite());
    // A presented certificate was verified by the handshake. Its names are
    // parsed once; a certificate whose names do not parse stays verified
    // but carries no identity facts.
    let leaf = conn.peer_certificates().and_then(|certs| certs.first());
    let client_auth = if leaf.is_some() {
        ClientAuthStatus::Verified
    } else {
        ClientAuthStatus::NotProvided
    };
    let client_cert =
        leaf.and_then(|cert| peer_identity(cert.as_ref()).ok()).map(Arc::new);

    Some(TlsInfo { protocol_version, cipher_suite, client_auth, client_cert })
}
