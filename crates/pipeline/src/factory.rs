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
pub(crate) struct PreparedBackends {
    pub before_files: Vec<Arc<dyn BackendHandler>>,
    pub after_files: Vec<Arc<dyn BackendHandler>>,
    /// Which registrations prepared an owner here.
    pub owners: Vec<&'static str>,
}

impl PreparedBackends {
    pub(crate) fn add(
        &mut self,
        factory: &dyn BackendFactory,
        context: &BackendContext<'_>,
    ) -> Result<()> {
        let prepared = factory.prepare(context)?;
        if prepared.is_empty() {
            return Ok(());
        }
        self.owners.push(factory.name());
        match factory.placement() {
            BackendPlacement::BeforeFiles => {
                self.before_files.extend(prepared);
            }
            BackendPlacement::AfterFiles => self.after_files.extend(prepared),
        }
        Ok(())
    }
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

    /// Whether this owner serves a scope only when nothing there has one of its
    /// own. A route's explicit owner replaces it.
    fn inherited(&self) -> bool {
        false
    }
}

/// Where a backend registered from code serves. It names its place explicitly,
/// so that a provider registered once does not install itself on every host.
#[derive(Clone, Debug, Default)]
pub struct BackendScope {
    host: Option<Host>,
    prefix: Option<PathPrefix>,
}

impl BackendScope {
    /// Everything this registry's site serves, wherever no scope has an owner
    /// of its own.
    pub fn site() -> Self {
        Self::default()
    }

    /// The same, on one host of those a registry shared by many selects.
    pub fn host(host: Host) -> Self {
        Self { host: Some(host), prefix: None }
    }

    /// Only the route at `prefix`, and no route below it.
    pub fn at(
        mut self,
        prefix: PathPrefix,
    ) -> Self {
        self.prefix = Some(prefix);
        self
    }

    fn selects(
        &self,
        context: &BackendContext<'_>,
    ) -> bool {
        self.host.as_ref().is_none_or(|host| context.host == Some(host))
            && self
                .prefix
                .as_ref()
                .is_none_or(|prefix| prefix.as_str() == context.prefix.as_str())
    }
}

/// A backend registered from code: no directive and nothing to decode. `prepare`
/// runs for each scope `scope` selects, and may return no owner for one.
pub fn backend<F>(
    name: &'static str,
    scope: BackendScope,
    placement: BackendPlacement,
    prepare: F,
) -> Arc<dyn BackendFactory>
where
    F: Fn(&BackendContext<'_>) -> Result<Vec<Arc<dyn BackendHandler>>>
        + Send
        + Sync
        + 'static,
{
    Arc::new(CodeBackend { name, scope, placement, prepare })
}

struct CodeBackend<F> {
    name: &'static str,
    scope: BackendScope,
    placement: BackendPlacement,
    prepare: F,
}

impl<F> BackendFactory for CodeBackend<F>
where
    F: Fn(&BackendContext<'_>) -> Result<Vec<Arc<dyn BackendHandler>>>
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
        if self.scope.selects(context) {
            (self.prepare)(context)
        } else {
            Ok(Vec::new())
        }
    }

    fn inherited(&self) -> bool {
        self.scope.prefix.is_none()
    }
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
