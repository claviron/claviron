//! Configured execution owners, independent of route controls and hooks.
//! What such an owner implements is `claviron_pipeline`; this is how a host
//! declaration selects it.

use std::sync::Arc;

use claviron_http::{
    config::ListenerConfig, ext::PathPrefix, policy::HttpPolicy,
};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;
use serde::de::DeserializeOwned;

use crate::{BackendHandler, Site};

/// Effective input borrowed while preparing one listener/host/prefix backend.
/// No registry lookup or configuration decoding is performed by dispatch.
pub struct BackendContext<'a> {
    pub state: &'a AppState,
    /// The canonical host name, when the site is served as a virtual host.
    pub host: Option<&'a Host>,
    pub site: &'a Site,
    pub route: &'a Site,
    pub prefix: &'a PathPrefix,
    pub policy: HttpPolicy,
    /// What the connection's terms allow an owner to take over.
    pub transport: &'a claviron_http::config::transport::TransportPolicy,
    pub protocol: Option<RouteProtocol>,
    pub listener: Option<&'a ListenerConfig>,
}

/// Backend position within an admitted scope, after Rust contributions/router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendPlacement {
    BeforeFiles,
    AfterFiles,
}

#[derive(Default)]
pub struct PreparedBackends {
    pub before_files: Vec<Arc<dyn BackendHandler>>,
    pub after_files: Vec<Arc<dyn BackendHandler>>,
}

/// Compile one backend directive into its ordered execution owners.
/// Placement is declared at preparation; native file fallback follows both slots.
/// An inactive declaration returns no owners and installs no request action.
pub trait BackendFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn placement(&self) -> BackendPlacement;
    fn prepare(
        &self,
        context: &BackendContext<'_>,
    ) -> Result<Vec<Arc<dyn BackendHandler>>>;
}

/// Decode a backend's configuration once, then prepare its concrete owners.
/// An absent or null directive prepares no owner.
pub fn factory<C, F>(
    name: &'static str,
    placement: BackendPlacement,
    prepare: F,
) -> Arc<dyn BackendFactory>
where
    C: DeserializeOwned + 'static,
    F: Fn(&BackendContext<'_>, C) -> Result<Vec<Arc<dyn BackendHandler>>>
        + Send
        + Sync
        + 'static,
{
    Arc::new(TypedFactory {
        name,
        placement,
        prepare,
        config: std::marker::PhantomData::<fn() -> C>,
    })
}

struct TypedFactory<C, F> {
    name: &'static str,
    placement: BackendPlacement,
    prepare: F,
    config: std::marker::PhantomData<fn() -> C>,
}

impl<C, F> BackendFactory for TypedFactory<C, F>
where
    C: DeserializeOwned,
    F: Fn(&BackendContext<'_>, C) -> Result<Vec<Arc<dyn BackendHandler>>>
        + Send
        + Sync,
{
    fn name(&self) -> &'static str {
        self.name
    }
    fn placement(&self) -> BackendPlacement {
        self.placement
    }
    fn prepare(
        &self,
        context: &BackendContext<'_>,
    ) -> Result<Vec<Arc<dyn BackendHandler>>> {
        let Some(source) = context
            .route
            .extensions
            .get(self.name)
            .filter(|source| !source.is_null())
        else {
            return Ok(Vec::new());
        };
        let config =
            serde_yaml::from_value(source.clone()).map_err(|error| {
                Error::msg(format!(
                    "backend '{}' at '{}': {error}",
                    self.name, context.prefix
                ))
            })?;
        (self.prepare)(context, config)
    }
}

/// Listener protocol family used while preparing host contributions.
/// HTTP and HTTPS are distinct even when both carry HTTP/1.1.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RouteProtocol {
    /// Cleartext HTTP over TCP.
    Http,
    /// HTTP/1.1 or HTTP/2 over TLS/TCP.
    Https,
    /// HTTP/3 over QUIC, including its TLS handshake.
    Quic,
}

impl From<&claviron_http::config::ListenerConfig> for RouteProtocol {
    fn from(listener: &claviron_http::config::ListenerConfig) -> Self {
        use claviron_http::config::{ListenerSettings, TcpSecurity};
        match &listener.settings {
            ListenerSettings::Tcp(tcp) => match tcp.security {
                TcpSecurity::Cleartext { .. } => Self::Http,
                TcpSecurity::Tls { .. } => Self::Https,
            },
            ListenerSettings::Quic(_) => Self::Quic,
        }
    }
}

// An unbound plan retains only contributions valid on every protocol family.
pub fn matches_protocol(
    protocol: Option<RouteProtocol>,
    matches: impl Fn(RouteProtocol) -> bool,
) -> bool {
    match protocol {
        Some(protocol) => matches(protocol),
        None => {
            [RouteProtocol::Http, RouteProtocol::Https, RouteProtocol::Quic]
                .into_iter()
                .all(matches)
        }
    }
}
