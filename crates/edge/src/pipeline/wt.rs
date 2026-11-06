use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::registry::{Error, Result};
use claviron_types::Host;
use http::StatusCode;

use super::connect::{AnyAuthenticated, AuthContext, normalize_connect_path};

pub use claviron_transport::quic::wt::session::{
    H3WebTransportBiStream, H3WebTransportSession, webtransport_session_ended,
};

/// Application-owned WebTransport session handler.
#[async_trait]
pub trait WebTransportHandler: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        "web_transport_handler"
    }

    async fn accept(
        &self,
        _state: &AppState,
        _ctx: &AuthContext,
    ) -> WebTransportResult {
        Ok(WebTransportAccept::anonymous())
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        authenticated: Option<AnyAuthenticated>,
        session: H3WebTransportSession,
    ) -> Result<()>;
}

pub struct WebTransportAccept {
    authenticated: Option<AnyAuthenticated>,
}

pub type WebTransportResult =
    std::result::Result<WebTransportAccept, WebTransportReject>;

impl WebTransportAccept {
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

pub struct WebTransportReject {
    status: StatusCode,
}

impl WebTransportReject {
    pub fn new(status: StatusCode) -> Self {
        Self { status }
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

    pub fn status(&self) -> StatusCode {
        self.status
    }
}

/// Resolved WebTransport binding for a host/path.
///
/// Single-variant on purpose for the first cut: code registers a handler and
/// the H3 transport gives it the accepted session. Future WebTransport
/// upstream/proxy support can grow a second variant without changing the
/// registry contract.
#[derive(Clone)]
pub enum WebTransportBinding {
    Handler(Arc<dyn WebTransportHandler>),
}

impl WebTransportBinding {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Handler(_) => "handler",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WebTransportRouteKey {
    host: Host,
    path: Arc<str>,
}

/// Code-driven WebTransport binding table.
///
/// Host config can enable or deny WebTransport for the vhost, but it cannot
/// name Rust code. Providers register concrete handlers here during boot, keyed
/// by canonical host and exact path (`/wt/orbit`, `/wt/nwd1`, ...).
/// Orbit composition may additionally bind a host fallback which selects the
/// application from the admitted host snapshot after exact native handlers.
#[derive(Default)]
pub struct WebTransportRegistry {
    routes: RwLock<HashMap<WebTransportRouteKey, WebTransportBinding>>,
    fallbacks: RwLock<HashMap<Host, WebTransportBinding>>,
}

impl WebTransportRegistry {
    pub fn register_handler_for_host(
        &self,
        host: &str,
        path: impl Into<Arc<str>>,
        handler: Arc<dyn WebTransportHandler>,
    ) -> Result<()> {
        let host = Host::from_str(host).map_err(|err| {
            Error::msg(format!("invalid WebTransport host '{host}': {err}"))
        })?;
        self.register_handler(host, path, handler)
    }

    pub fn register_handler(
        &self,
        host: Host,
        path: impl Into<Arc<str>>,
        handler: Arc<dyn WebTransportHandler>,
    ) -> Result<()> {
        let path = normalize_connect_path(path.into())?;
        let key = WebTransportRouteKey { host, path };
        let mut routes =
            self.routes.write().expect("web transport registry lock poisoned");
        if routes.contains_key(&key) {
            return Err(Error::msg(format!(
                "duplicate WebTransport binding for host={} path={}",
                key.host, key.path
            )));
        }
        routes.insert(key, WebTransportBinding::Handler(handler));
        Ok(())
    }

    pub fn resolve(
        &self,
        host: &Host,
        path: &str,
    ) -> Option<WebTransportBinding> {
        let path = normalize_connect_path(Arc::<str>::from(path)).ok()?;
        let key = WebTransportRouteKey { host: host.clone(), path };
        let exact = self
            .routes
            .read()
            .expect("web transport registry lock poisoned")
            .get(&key)
            .cloned();
        exact.or_else(|| {
            self.fallbacks
                .read()
                .expect("web transport fallback registry lock poisoned")
                .get(host)
                .cloned()
        })
    }

    /// A host's application dispatch after exact native session bindings.
    /// The handler must select and authorize its route from the accepted host
    /// snapshot; retaining this entry cannot revive a removed application.
    #[cfg(feature = "orbit")]
    pub(crate) fn bind_fallback(
        &self,
        host: Host,
        handler: Arc<dyn WebTransportHandler>,
    ) {
        self.fallbacks
            .write()
            .expect("web transport fallback registry lock poisoned")
            .insert(host, WebTransportBinding::Handler(handler));
    }

    /// Rebind a host/path during vhost snapshot preparation, as with H2
    /// CONNECT. Existing sessions retain their accepted handler/generation.
    pub fn bind_handler(
        &self,
        host: Host,
        path: impl Into<Arc<str>>,
        handler: Arc<dyn WebTransportHandler>,
    ) -> Result<()> {
        let path = normalize_connect_path(path.into())?;
        self.routes
            .write()
            .expect("web transport registry lock poisoned")
            .insert(
                WebTransportRouteKey { host, path },
                WebTransportBinding::Handler(handler),
            );
        Ok(())
    }

    pub fn has_host(
        &self,
        host: &Host,
    ) -> bool {
        self.routes
            .read()
            .expect("web transport registry lock poisoned")
            .keys()
            .any(|key| &key.host == host)
            || self
                .fallbacks
                .read()
                .expect("web transport fallback registry lock poisoned")
                .contains_key(host)
    }
}

#[cfg(all(test, feature = "orbit"))]
#[path = "../../tests/pipeline/wt.rs"]
mod tests;
