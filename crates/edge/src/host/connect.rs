use std::any::Any;
use std::sync::Arc;

use claviron_http::ext::ConnCtx;
use claviron_runtime::registry::{Error, Result};
use claviron_types::Host;
use http::HeaderMap;

use crate::config::Site;

/// Typed request context for long-lived transport attempts.
///
/// This is built before a long-lived protocol session is accepted, so handlers
/// can reject unauthorised clients while the request is still a plain
/// handshake.
#[derive(Clone)]
pub struct AuthContext {
    pub host: Host,
    pub host_config: Arc<Site>,
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

#[cfg(feature = "webtransport")]
pub(super) fn normalize_connect_path(path: Arc<str>) -> Result<Arc<str>> {
    if path.starts_with('/') {
        Ok(path)
    } else {
        Err(Error::msg(format!("CONNECT path must start with '/': {path}")))
    }
}
