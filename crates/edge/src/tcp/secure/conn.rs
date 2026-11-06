//! The host a TLS connection was accepted for, as the transport sees it.
//!
//! The handshake and the HTTP driver are `claviron_transport::tcp::secure`.
//! This is what Edge adds: the SNI host captured with its TLS policy, its
//! protocol policy, its retirement and the handoff to its prepared execution.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::transport::{
    HostBinding, HostRejection, HostService, VhostCtx, VhostResolver,
};
use claviron_http::config::ListenerConfig;
use claviron_http::config::transport::TransportPolicy;
use claviron_http::ext::ConnCtx;
use claviron_http::session::ConnectionScope;
use claviron_runtime::AppState;
use claviron_tls::Proto;
use claviron_transport::tcp::secure::SecureHost;
use tokio::net::TcpStream;
use tracing::info;

use super::error::ConnectionError;
use super::server::ServerSnapshot;
use crate::tcp::service::HostRequests;

/// A prepared TLS listener's native handler for accepted TCP streams.
/// The caller owns acceptance, connection capacity and session drain. Each
/// stream keeps the native handshake, host selection, ALPN choice, HTTP
/// driver and shutdown path.
#[derive(Clone)]
pub struct ConnectionHandler {
    snapshot: Arc<ServerSnapshot>,
}

impl ConnectionHandler {
    pub(super) fn new(snapshot: Arc<ServerSnapshot>) -> Self {
        Self { snapshot }
    }

    /// Serve one accepted stream until peer completion or its owned drain.
    /// No provider boot, socket bind or new connection permit is performed.
    pub fn serve(
        self,
        stream: TcpStream,
        peer: SocketAddr,
        scope: ConnectionScope,
    ) -> impl Future<Output = ()> + Send {
        claviron_transport::tcp::secure::serve(
            stream,
            peer,
            self.snapshot,
            scope,
        )
    }
}

/// The SNI host chosen for one connection by the publication it was accepted
/// under.
///
/// Shared admission retains this host's captured policy and exact SNI authority.
/// A request for another name receives 421, even if it shares the certificate;
/// connection coalescing must not switch authentication or application policy.
pub(crate) struct SelectedHost {
    pub(super) vctx: VhostCtx,
    pub(super) listener: Arc<ListenerConfig>,
    pub(super) state: AppState,
}

impl SelectedHost {
    /// Hold this connection's SNI host to the vhost it resolved to at
    /// acceptance. The binding wakes only when a vhost table is published and
    /// retires the connection when its host mapping or captured configuration
    /// is replaced; the request path never consults it.
    fn binding(&self) -> Option<HostBinding> {
        self.state.registry_ref().resolve::<VhostResolver>().map(|resolver| {
            resolver.bind_config(
                Arc::clone(&self.vctx.hostname.0),
                Arc::clone(&self.vctx.vhost_key.0),
                self.vctx.vhost_config.clone(),
            )
        })
    }
}

/// Complete when this connection's host binding retires; never without one.
async fn host_retired(
    binding: Option<HostBinding>,
    proto: Proto,
) {
    match binding {
        Some(mut binding) => {
            binding.retired().await;
            info!(
                protocol = %proto.version(),
                host = %binding.hostname(),
                canonical = %binding.canonical(),
                "🔀 host mapping or configuration changed: retiring this connection gracefully"
            );
        }
        None => std::future::pending().await,
    }
}

impl SecureHost for SelectedHost {
    type Requests = HostRequests;
    type Rejection = ConnectionError;

    /// Binding was selected alongside authentication before the handshake.
    fn admit(&self) -> Result<(), ConnectionError> {
        if self.vctx.vhost_config.allows_listener(&self.listener.id) {
            Ok(())
        } else {
            Err(HostRejection::NotHosted(self.vctx.hostname.0.to_string())
                .into())
        }
    }

    fn transport(&self) -> &TransportPolicy {
        self.vctx.vhost_config.transport_policy()
    }

    fn require_alpn(&self) -> bool {
        self.vctx.vhost_config.tls.require_alpn
    }

    fn retired(
        &self,
        protocol: Proto,
    ) -> impl Future<Output = ()> + Send + 'static {
        host_retired(self.binding(), protocol)
    }

    /// Bind the shared host dispatcher without re-reading live policy.
    fn bind(
        self,
        mut facts: ConnCtx,
    ) -> Result<HostRequests, ConnectionError> {
        let Self { mut vctx, listener, .. } = self;
        let app = vctx
            .application
            .take()
            .ok_or(ConnectionError::ApplicationUnavailable)?;
        // The facts are complete; what the host prepares from them is frozen
        // into the connection before its first request.
        facts.state = app.accept(&crate::pipeline::Accepted {
            connection: &facts,
            hostname: &vctx.hostname,
            host: &vctx.vhost_key,
            config: &vctx.vhost_config,
        });
        Ok(HostRequests::new(HostService::new(
            app,
            Arc::new(facts),
            vctx,
            listener.policy(),
        )))
    }
}
