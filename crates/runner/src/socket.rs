//! The runner's own sockets: TCP and Unix, HTTP/1 and HTTP/2, TLS on TCP.
//!
//! Accepting and driving connections is `claviron-transport`'s, the same code
//! an edge's listeners run. What is here is what makes them this runner's
//! doors: every request goes to the one [`Application`], with no vhost, and
//! the same service runs unchanged under the Orbit ingress, where there is no
//! socket at all. There is no HTTP/3 here: one application, reached over a
//! loopback, a private network or a Unix socket, where QUIC would buy a UDP
//! stack and a congestion controller for nothing.
//!
//! What it attaches is the edge's: the accepted connection's facts in
//! `ConnCtx` (none for a Unix socket, which has no addresses), the client
//! after [`Trust`] in `RealIpExt`, a trusted proxy's word in `Forwarded`, and
//! when the request arrived in `ReceivedAt`.
//!
//! Shutdown stops accepting, asks every connection to finish, and joins them
//! and the sessions they started, with no deadline of its own.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_http::RouteResponse;
use claviron_http::config::{
    H1Config, HttpSnapshot, ListenerConfig, ListenerSettings, TcpConfig,
    TcpListenerConfig,
};
use claviron_http::ext::{ConnCtx, RealIpExt, ReceivedAt};
use claviron_http::session::SessionSpawner;
use claviron_http::trust::Trust;
use claviron_runtime::{
    AppState, Component, RunContext, RunMode, Runnable, ServiceStartPolicy,
    registry,
};
use claviron_transport::tcp::Requests;
use claviron_transport::tcp::listener::TcpListenerSnapshot;
use claviron_transport::tcp::plain::{self, Cleartext};
use claviron_transport::tcp::prior::PriorKnowledge;
use http::Request;
use hyper::body::Incoming;
use tokio::task::JoinSet;

use crate::routing::{AppRequest, Application, RequestBody};

const LISTENER: &str = "runner";

/// Where a listener listens.
#[derive(Clone, Debug)]
pub enum Bind {
    Tcp(SocketAddr),
    Uds(PathBuf),
}

impl Bind {
    fn describe(
        &self,
        secure: bool,
    ) -> String {
        match self {
            Self::Tcp(address) if secure => format!("https://{address}"),
            Self::Tcp(address) => format!("http://{address}"),
            Self::Uds(path) => format!("unix:{}", path.display()),
        }
    }
}

/// Which HTTP versions the sockets accept. Naming neither is the same as
/// naming both: whatever the client speaks.
#[derive(Clone, Copy, Debug)]
pub struct Protocols {
    pub h1: bool,
    pub h2: bool,
}

impl Default for Protocols {
    fn default() -> Self {
        Self { h1: true, h2: true }
    }
}

impl Protocols {
    pub fn resolve(
        h1: bool,
        h2: bool,
    ) -> Self {
        if h1 || h2 { Self { h1, h2 } } else { Self::default() }
    }

    #[cfg(feature = "tls")]
    fn alpn(self) -> Vec<&'static [u8]> {
        let mut alpn: Vec<&'static [u8]> = Vec::new();
        if self.h2 {
            alpn.push(b"h2");
        }
        if self.h1 {
            alpn.push(b"http/1.1");
        }
        alpn
    }

    #[cfg(feature = "tls")]
    fn declared(self) -> &'static str {
        match (self.h1, self.h2) {
            (true, false) => "[h1]",
            (false, true) => "[h2]",
            _ => "[h2, h1]",
        }
    }
}

/// Every socket the application answers on, as one runnable: they share a
/// lifetime, a cancellation and an application, and differ only in where
/// they accept.
pub struct SocketIngress {
    binds: Vec<Bind>,
    protocols: Protocols,
    tcp_backlog: NonZeroU32,
    #[cfg(feature = "tls")]
    tls: Option<crate::Tls>,
}

impl SocketIngress {
    pub fn new(
        binds: Vec<Bind>,
        protocols: Protocols,
    ) -> Self {
        Self {
            binds,
            protocols,
            tcp_backlog: TcpConfig::default().backlog,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Set the pending TCP connection queue before starting this ingress.
    /// The default comes from `TcpConfig`; the OS may cap the requested value.
    /// Values above the socket API range fail binding, rather than wrapping.
    pub fn tcp_backlog(
        mut self,
        backlog: NonZeroU32,
    ) -> Self {
        self.tcp_backlog = backlog;
        self
    }

    /// Serve TLS on the TCP sockets. A Unix socket ignores it: its boundary is
    /// the filesystem's already.
    #[cfg(feature = "tls")]
    pub fn tls(
        mut self,
        tls: Option<crate::Tls>,
    ) -> Self {
        self.tls = tls;
        self
    }

    /// The transport's listener for one address, from the launch's options.
    /// Declared the way `http.yaml` is, so its defaults are the same ones. A
    /// runner sets no deadline of its own: a slow client's header read is the
    /// proxy's or the operator's to bound.
    fn listener(
        &self,
        address: Option<SocketAddr>,
        secure: bool,
    ) -> Result<Arc<ListenerConfig>, String> {
        let listen = address.map_or_else(
            || "127.0.0.1:0".to_owned(),
            |address| address.to_string(),
        );
        #[cfg(feature = "tls")]
        let protocols = if secure { self.protocols.declared() } else { "h1" };
        #[cfg(not(feature = "tls"))]
        let protocols = "h1";
        let backlog = self.tcp_backlog;
        let document = format!(
            "listeners:\n  {LISTENER}:\n    transport: tcp\n    tls: {secure}\n    protocols: {protocols}\n    listen: '{listen}'\n    tcp: {{backlog: {backlog}}}\n    requests: {{max_body_size: off, body_read_timeout: off}}\n    h1: {{header_read_timeout: null}}\n",
        );
        let snapshot: HttpSnapshot = serde_yaml::from_str(&document)
            .map_err(|error| error.to_string())?;
        snapshot
            .listeners
            .into_values()
            .next()
            .ok_or_else(|| "the listener was not declared".to_owned())
    }

    /// The application behind one address. Cleartext has no ALPN: HTTP/2 is
    /// by prior knowledge, beside HTTP/1.1 unless the launch named HTTP/2 alone.
    fn door(
        &self,
        address: Option<SocketAddr>,
        secure: bool,
        application: Arc<Application>,
        trust: Arc<Trust>,
    ) -> Result<Arc<Door>, String> {
        let listener = self.listener(address, secure)?;
        let ListenerSettings::Tcp(config) = &listener.settings else {
            return Err("the runner declares TCP listeners".to_owned());
        };
        let config = config.clone();
        let prior = (!secure && self.protocols.h2).then(|| PriorKnowledge {
            h2: config.h2.clone(),
            h1: self.protocols.h1,
        });
        Ok(Arc::new(Door {
            listener,
            config,
            prior,
            application,
            trust,
            #[cfg(feature = "tls")]
            tls: None,
            #[cfg(feature = "tls")]
            transport: Default::default(),
        }))
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
                self.binds.iter().find(|bind| matches!(bind, Bind::Uds(_)))
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

/// One application behind one listener's declared settings.
struct Door {
    listener: Arc<ListenerConfig>,
    config: TcpListenerConfig,
    prior: Option<PriorKnowledge>,
    application: Arc<Application>,
    trust: Arc<Trust>,
    #[cfg(feature = "tls")]
    tls: Option<Arc<rustls::ServerConfig>>,
    #[cfg(feature = "tls")]
    transport: claviron_http::config::transport::TransportPolicy,
}

impl Door {
    fn requests(
        self: &Arc<Self>,
        connection: Option<Arc<ConnCtx>>,
        sessions: SessionSpawner,
    ) -> ApplicationRequests {
        ApplicationRequests { door: self.clone(), connection, sessions }
    }
}

impl TcpListenerSnapshot for Door {
    fn listener(&self) -> &ListenerConfig {
        &self.listener
    }
}

impl Cleartext for Door {
    type Requests = ApplicationRequests;

    fn tcp(&self) -> &TcpConfig {
        &self.config.tcp
    }
    fn h1(&self) -> &H1Config {
        &self.config.h1
    }
    fn prior_knowledge(&self) -> Option<&PriorKnowledge> {
        self.prior.as_ref()
    }
    fn connection(
        self: &Arc<Self>,
        facts: Arc<ConnCtx>,
        sessions: SessionSpawner,
    ) -> ApplicationRequests {
        self.requests(Some(facts), sessions)
    }
}

#[cfg(unix)]
impl claviron_transport::unix::Unix for Door {
    type Requests = ApplicationRequests;

    fn h1(&self) -> &H1Config {
        &self.config.h1
    }
    fn prior_knowledge(&self) -> Option<&PriorKnowledge> {
        self.prior.as_ref()
    }
    fn connection(
        self: &Arc<Self>,
        sessions: SessionSpawner,
    ) -> ApplicationRequests {
        self.requests(None, sessions)
    }
}

#[cfg(feature = "tls")]
impl claviron_transport::tcp::secure::Secure for Door {
    type Host = OneHost;

    fn config(&self) -> &TcpListenerConfig {
        &self.config
    }
    fn select(
        self: &Arc<Self>,
        _server_name: Option<&str>,
    ) -> std::io::Result<(Arc<rustls::ServerConfig>, OneHost)> {
        let tls = self.tls.clone().ok_or_else(|| {
            std::io::Error::other("this listener has no TLS configuration")
        })?;
        Ok((tls, OneHost(self.clone())))
    }
}

/// A runner has one application, whatever name a client asks for.
#[cfg(feature = "tls")]
struct OneHost(Arc<Door>);

#[cfg(feature = "tls")]
impl claviron_transport::tcp::secure::SecureHost for OneHost {
    type Requests = ApplicationRequests;
    type Rejection = Infallible;

    fn admit(&self) -> Result<(), Infallible> {
        Ok(())
    }
    fn transport(&self) -> &claviron_http::config::transport::TransportPolicy {
        &self.0.transport
    }
    fn require_alpn(&self) -> bool {
        false
    }
    fn retired(
        &self,
        _protocol: claviron_tls::Proto,
    ) -> impl Future<Output = ()> + Send + 'static {
        std::future::pending()
    }
    fn bind(
        self,
        facts: ConnCtx,
        sessions: SessionSpawner,
    ) -> Result<ApplicationRequests, Infallible> {
        Ok(self.0.requests(Some(Arc::new(facts)), sessions))
    }
}

/// One connection's requests, on their way to the application.
struct ApplicationRequests {
    door: Arc<Door>,
    connection: Option<Arc<ConnCtx>>,
    sessions: SessionSpawner,
}

impl Requests for ApplicationRequests {
    fn call(
        &self,
        request: Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<>
    {
        let request = prepare(
            request,
            self.connection.clone(),
            &self.door.trust,
            self.sessions.clone(),
        );
        let answering = self.door.application.call(request);
        async move { Ok(answering.await) }
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

        // Read the certificate before any socket opens: a bad path or a key
        // that does not match is a boot failure, not a failed handshake later.
        #[cfg(feature = "tls")]
        let tls = self
            .tls
            .as_ref()
            .map(|tls| tls.config(&self.protocols.alpn()))
            .transpose()
            .map_err(failure)?;
        #[cfg(feature = "tls")]
        let secure = tls.is_some();
        #[cfg(not(feature = "tls"))]
        let secure = false;

        let run_mode = state.run_mode();
        let trusted =
            if trust.is_empty() { "nobody" } else { "registered peers" };
        // One socket that cannot be bound ends the others: a boot failure,
        // rather than a process that half serves.
        let owned = context.child();
        let mut serving = JoinSet::new();
        for bind in &self.binds {
            let (address, tls_here) = match bind {
                Bind::Tcp(address) => (Some(*address), secure),
                Bind::Uds(_) => (None, false),
            };
            #[cfg_attr(not(feature = "tls"), allow(unused_mut))]
            let mut door = self
                .door(address, tls_here, application.clone(), trust.clone())
                .map_err(failure)?;
            #[cfg(feature = "tls")]
            if tls_here {
                Arc::get_mut(&mut door)
                    .expect("the door is not shared yet")
                    .tls = tls.clone();
            }
            tracing::info!(trusted, "listening on {}", bind.describe(tls_here));
            let context = owned.child();
            match bind.clone() {
                Bind::Tcp(_) => {
                    serving.spawn(serve_tcp(door, context, run_mode, tls_here));
                }
                Bind::Uds(path) => {
                    #[cfg(unix)]
                    serving.spawn(async move {
                        claviron_transport::unix::serve(&path, door, &context)
                            .await
                    });
                    #[cfg(not(unix))]
                    return Err(failure(format!(
                        "unix:{}: this platform has no Unix sockets",
                        path.display()
                    )));
                }
            }
        }

        let mut failed = None;
        while let Some(finished) = serving.join_next().await {
            let result = finished.map_err(failure).and_then(|result| result);
            if let Err(error) = result {
                owned.cancellation_token().cancel();
                failed.get_or_insert(error);
            }
        }
        failed.map_or(Ok(()), Err)
    }
}

async fn serve_tcp(
    door: Arc<Door>,
    context: RunContext,
    run_mode: RunMode,
    secure: bool,
) -> registry::Result<()> {
    let snapshots = ArcSwapOption::from(Some(door.clone()));
    #[cfg(feature = "tls")]
    if secure {
        return claviron_transport::tcp::listener::serve(
            &snapshots,
            door,
            &context,
            run_mode,
            |door, stream, peer, scope| {
                claviron_transport::tcp::secure::serve(
                    stream, peer, door, scope,
                )
            },
        )
        .await;
    }
    #[cfg(not(feature = "tls"))]
    let _ = secure;
    claviron_transport::tcp::listener::serve(
        &snapshots,
        door,
        &context,
        run_mode,
        |door, stream, peer, scope| plain::serve(stream, peer, door, scope),
    )
    .await
}

fn prepare(
    request: Request<Incoming>,
    connection: Option<Arc<ConnCtx>>,
    trust: &Trust,
    sessions: SessionSpawner,
) -> AppRequest {
    let peer = connection.as_ref().map(|connection| connection.remote);
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
        extensions.insert(connection);
    }
    extensions.insert(sessions);
    request
}

fn failure(error: impl std::fmt::Display) -> registry::Error {
    registry::Error::msg(error.to_string())
}

#[cfg(all(test, unix))]
#[path = "../tests/socket.rs"]
mod tests;
