//! The application behind the transport's sockets.
//!
//! Accepting and driving connections is `claviron_transport::doors`, the same
//! drivers an edge's listeners run. What is here is what makes them this
//! runner's: every request goes to the one [`Application`], with no vhost, and
//! the same service runs unchanged under the Orbit ingress, where there is no
//! socket at all.
//!
//! What it attaches is the edge's: the accepted connection's facts in
//! `ConnCtx` (none for a Unix socket, which has no addresses), the client
//! after [`Trust`] in `RealIpExt`, a trusted proxy's word in `Forwarded`, and
//! when the request arrived in `ReceivedAt`.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use claviron_http::ext::{ConnCtx, RealIpExt, ReceivedAt, RequestArrival};
use claviron_http::trust::Trust;
use claviron_http::{RouteBody, RouteResponse};
use claviron_runtime::{
    AppState, Component, RunContext, RunMode, Runnable, ServiceStartPolicy,
    registry,
};
use claviron_transport::doors::Doors;
pub use claviron_transport::doors::{Bind, Protocols};
use claviron_transport::owner::Owner as TransportOwner;
use http::Request;

use crate::routing::{AppRequest, Application, RequestBody};

/// Every socket the application answers on, as one runnable.
pub struct SocketIngress(Doors);

impl SocketIngress {
    pub fn new(
        binds: Vec<Bind>,
        protocols: Protocols,
    ) -> Self {
        Self(Doors::new(binds, protocols))
    }

    /// Set the pending TCP connection queue before starting this ingress.
    pub fn tcp_backlog(
        self,
        backlog: NonZeroU32,
    ) -> Self {
        Self(self.0.tcp_backlog(backlog))
    }

    /// Serve TLS on the TCP sockets. A Unix socket ignores it.
    #[cfg(feature = "tls")]
    pub fn tls(
        self,
        tls: Option<crate::Tls>,
    ) -> Self {
        Self(self.0.tls(tls))
    }
}

impl Component<AppState> for SocketIngress {
    type Deps = (Arc<Application>, Option<Arc<Trust>>);

    fn name(&self) -> &'static str {
        "SocketIngress"
    }

    /// A Unix socket file is one process's: workers cannot each bind it the
    /// way they share a TCP port. Refused at launch, in the master, rather
    /// than as a worker that fails and is restarted forever.
    fn validate(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        if !matches!(state.run_mode(), RunMode::Standalone)
            && let Some(Bind::Uds(path)) =
                self.0.binds().iter().find(|bind| matches!(bind, Bind::Uds(_)))
        {
            return Err(registry::Error::msg(format!(
                "unix:{} cannot be served by several workers; serve it standalone, or reach \
                 the workers over TCP or Orbit",
                path.display()
            )));
        }
        Ok(())
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

/// One application behind the doors: what they accept, attached to and handed
/// on.
struct Front {
    application: Arc<Application>,
    trust: Arc<Trust>,
}

impl TransportOwner for Front {
    fn call(
        &self,
        request: Request<RouteBody>,
        connection: Option<&Arc<ConnCtx>>,
        _arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<> {
        self.application.call(prepare(request, connection, &self.trust))
    }
}

#[async_trait]
impl Runnable<AppState, <Self as Component<AppState>>::Deps> for SocketIngress {
    /// A master with workers serves nothing; its workers do. Standalone is not
    /// a master and still runs.
    fn start_policy(
        &self,
        state: &AppState,
    ) -> ServiceStartPolicy {
        if state.is_master() {
            ServiceStartPolicy::Unavailable
        } else {
            ServiceStartPolicy::Automatic
        }
    }

    async fn run(
        self: Arc<Self>,
        (application, trust): <Self as Component<AppState>>::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let trust = trust.unwrap_or_default();
        tracing::info!(
            trusted =
                if trust.is_empty() { "nobody" } else { "registered peers" },
            "the application's doors are opening"
        );
        self.0
            .serve(
                Arc::new(Front { application, trust }),
                &context,
                state.run_mode(),
            )
            .await
    }
}

fn prepare(
    request: Request<RouteBody>,
    connection: Option<&Arc<ConnCtx>>,
    trust: &Trust,
) -> AppRequest {
    let peer = connection.map(|connection| connection.remote);
    let resolved = trust.resolve(peer, request.headers());
    let (mut parts, body) = request.into_parts();
    if let Some(host) = resolved.host.as_deref()
        && let Ok(value) = http::HeaderValue::from_str(host)
    {
        parts.headers.insert(http::header::HOST, value);
    }
    let mut request = Request::from_parts(parts, RequestBody::new(body));
    let extensions = request.extensions_mut();
    extensions.insert(ReceivedAt(SystemTime::now()));
    if let Some(client) = resolved.client {
        extensions.insert(RealIpExt(client.ip()));
    }
    if let Some(forwarded) = resolved.forwarded(peer) {
        extensions.insert(forwarded);
    }
    if let Some(connection) = connection {
        extensions.insert(connection.clone());
    }
    request
}

#[cfg(all(test, unix))]
#[path = "../tests/socket.rs"]
mod tests;
