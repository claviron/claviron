use std::any::Any;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use claviron_http::ext::ConnCtx;
use claviron_http::{RouteBody, RouteResponse, err_status};
use claviron_runtime::AppState;
use claviron_runtime::registry::{Error, Result};
use claviron_types::Host;
use http::{HeaderMap, Request, StatusCode};

use crate::config::HostConfig;

/// Typed request context for long-lived transport attempts.
///
/// This is built before a long-lived protocol session is accepted, so handlers
/// can reject unauthorised clients while the request is still a plain
/// handshake.
#[derive(Clone)]
pub struct AuthContext {
    pub host: Host,
    pub host_config: Arc<HostConfig>,
    pub path: Arc<str>,
    pub uri: http::Uri,
    pub headers: HeaderMap,
    pub conn: Arc<ConnCtx>,
    /// Native connection admission, when supplied by the transport. Session
    /// handlers can observe listener drain without signalling any other work.
    pub admission: Option<Arc<claviron_runtime::gate::Permit>>,
    /// Native host retirement, retained after a WebTransport hand-off.
    pub graceful: Option<claviron_http::session::GracefulSignal>,
}

impl AuthContext {
    /// Cooperatively close this session on listener drain or host retirement.
    /// This does not cancel any other host, gate or accepted work.
    pub async fn wait_graceful_shutdown(&self) {
        let listener = async {
            match &self.admission {
                Some(permit) => permit.wait_graceful_shutdown().await,
                None => std::future::pending().await,
            }
        };
        let host = async {
            match &self.graceful {
                Some(signal) => signal.clone().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! { () = listener => {}, () = host => {} }
    }
}

/// Opaque authenticated payload carried from accept/auth phase to the
/// long-lived transport handler.
///
/// The transport pipeline never inspects this value. The handler that inserted
/// it knows its concrete type and can retrieve it with [`Self::take`].
pub struct AnyAuthenticated {
    value: Box<dyn Any + Send + Sync>,
}

impl AnyAuthenticated {
    pub fn new<T>(value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self { value: Box::new(value) }
    }

    pub fn take<T>(self) -> Result<T>
    where
        T: Send + Sync + 'static,
    {
        self.value
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| Error::msg("authenticated payload type mismatch"))
    }
}

pub struct H2ConnectAccept {
    authenticated: Option<AnyAuthenticated>,
}

pub type H2ConnectResult =
    std::result::Result<H2ConnectAccept, H2ConnectReject>;

impl H2ConnectAccept {
    pub fn anonymous() -> Self {
        Self { authenticated: None }
    }

    pub fn authenticated<T>(value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self { authenticated: Some(AnyAuthenticated::new(value)) }
    }

    pub fn into_authenticated(self) -> Option<AnyAuthenticated> {
        self.authenticated
    }
}

pub struct H2ConnectReject {
    status: StatusCode,
    reason: &'static str,
}

impl H2ConnectReject {
    pub fn new(status: StatusCode) -> Self {
        Self { status, reason: "handler_rejected" }
    }

    /// Attach a fixed diagnostic tag, never request data or error text.
    /// The tag is logged by the transport, not sent in the HTTP response.
    pub fn with_reason(
        mut self,
        reason: &'static str,
    ) -> Self {
        self.reason = reason;
        self
    }

    pub fn reason(&self) -> &'static str {
        self.reason
    }

    pub fn bad_request() -> Self {
        Self::new(StatusCode::BAD_REQUEST)
    }

    pub fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED)
    }

    pub fn forbidden() -> Self {
        Self::new(StatusCode::FORBIDDEN)
    }

    pub fn into_response(self) -> RouteResponse {
        err_status(self.status)
    }
}

/// Application-owned HTTP/2 CONNECT handler.
///
/// H2 CONNECT is request/response stream shaped: no WebTransport datagrams or
/// uni/bidi session API. The handler receives the already-authorized CONNECT
/// request with its transport body erased to `RouteBody`, and returns the
/// response body that Hyper will stream back on the same H2 stream.
///
/// For a Hyper upgrade hand-off, submit the entire session future through the
/// request's [`claviron_http::session::SessionSpawner`] before returning success.
/// It retains the inbound connection slot and joins the work during listener
/// shutdown. Detached `tokio::spawn` work is not owned by the listener.
#[async_trait]
pub trait H2ConnectHandler: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        "h2_connect_handler"
    }

    async fn accept(
        &self,
        _state: &AppState,
        _ctx: &AuthContext,
    ) -> H2ConnectResult {
        Ok(H2ConnectAccept::anonymous())
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        authenticated: Option<AnyAuthenticated>,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse>;
}

/// Resolved H2 CONNECT binding for a host/path.
#[derive(Clone)]
pub enum H2ConnectBinding {
    Handler(Arc<dyn H2ConnectHandler>),
}

impl H2ConnectBinding {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Handler(_) => "handler",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ConnectRouteKey {
    host: Host,
    path: Arc<str>,
}

/// Code-driven H2 CONNECT binding table.
///
/// This is intentionally separate from the WebTransport registry: both share
/// the auth context, but their accepted session/body APIs are different.
#[derive(Default)]
pub struct H2ConnectRegistry {
    routes: RwLock<HashMap<ConnectRouteKey, H2ConnectBinding>>,
}

impl H2ConnectRegistry {
    pub fn register_handler_for_host(
        &self,
        host: &str,
        path: impl Into<Arc<str>>,
        handler: Arc<dyn H2ConnectHandler>,
    ) -> Result<()> {
        let host = Host::from_str(host).map_err(|err| {
            Error::msg(format!("invalid H2 CONNECT host '{host}': {err}"))
        })?;
        self.register_handler(host, path, handler)
    }

    pub fn register_handler(
        &self,
        host: Host,
        path: impl Into<Arc<str>>,
        handler: Arc<dyn H2ConnectHandler>,
    ) -> Result<()> {
        let path = normalize_connect_path(path.into())?;
        let key = ConnectRouteKey { host, path };
        let mut routes =
            self.routes.write().expect("h2 connect registry lock poisoned");
        if routes.contains_key(&key) {
            return Err(Error::msg(format!(
                "duplicate H2 CONNECT binding for host={} path={}",
                key.host, key.path
            )));
        }
        routes.insert(key, H2ConnectBinding::Handler(handler));
        Ok(())
    }

    /// Bind a handler, replacing whatever the host/path had: the vhost
    /// pipeline rebuilds its bindings on every reload, and a rebuild is a
    /// replacement, not a second registration.
    pub fn bind_handler(
        &self,
        host: Host,
        path: impl Into<Arc<str>>,
        handler: Arc<dyn H2ConnectHandler>,
    ) -> Result<()> {
        let path = normalize_connect_path(path.into())?;
        let mut routes =
            self.routes.write().expect("h2 connect registry lock poisoned");
        routes.insert(
            ConnectRouteKey { host, path },
            H2ConnectBinding::Handler(handler),
        );
        Ok(())
    }

    pub fn resolve(
        &self,
        host: &Host,
        path: &str,
    ) -> Option<H2ConnectBinding> {
        let path = normalize_connect_path(Arc::<str>::from(path)).ok()?;
        let key = ConnectRouteKey { host: host.clone(), path };
        self.routes
            .read()
            .expect("h2 connect registry lock poisoned")
            .get(&key)
            .cloned()
    }
}

pub(super) fn normalize_connect_path(path: Arc<str>) -> Result<Arc<str>> {
    if path.starts_with('/') {
        Ok(path)
    } else {
        Err(Error::msg(format!("CONNECT path must start with '/': {path}")))
    }
}
