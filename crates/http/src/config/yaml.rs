//! Desired HTTP configuration and its validated, immutable runtime snapshot.

mod listeners;
mod proxy;
mod quic;
mod tcp;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

pub use claviron_helpers::overrides as helpers;
pub use helpers::Override;
use listeners::derive_max_connections;
pub use listeners::{
    ConnectionLimits, ListenerConfig, ListenerDefinition, ListenerOverrides,
    ListenerSettings, QuicListenerConfig, TcpListenerConfig, TcpSecurity,
};
pub use proxy::{ProxyConfig, ProxyOverride};
pub use quic::{QuicConfig, QuicOverride};
use serde::Deserialize;
pub use tcp::{
    AcceptRetryConfig, AcceptRetryOverride, H1Config, H1Override, H2Config,
    H2Override, HeaderReadTimeout, TcpConfig, TcpOverride,
};

use crate::ListenerId;
use crate::config::host::HostHeadersConfig;
use crate::listener::ListenerTransport;
#[cfg(feature = "metrics")]
use crate::metrics::HttpMetricsConfig;
pub use crate::policy::{
    BodyNeed, Buffering, Delivery, HttpPolicy, HttpPolicyOverride, Limit,
    RequestPolicy, RequestPolicyOverride, ResponsePolicy,
    ResponsePolicyOverride,
};
pub use crate::tuning::RouterConfig;
use crate::watchdog::WatchdogConfig;

/// Full http.yaml declaration. A key this build has no field for is not
/// read: its fields change with the features compiled in.
#[derive(Default, Deserialize)]
pub struct HttpYaml {
    #[serde(default)]
    pub defaults: HttpDefaults,
    #[serde(default, deserialize_with = "listeners::deserialize_definitions")]
    pub listeners: BTreeMap<ListenerId, ListenerDefinition>,
    #[cfg(feature = "metrics")]
    #[serde(default)]
    pub(crate) metrics: HttpMetricsConfig,
    /// HOL watchdog policy. Read in every build: a process notices its own
    /// wedge whether or not anything is scraping it.
    #[serde(default)]
    pub watchdog: Option<WatchdogConfig>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpDefaults {
    /// Server-wide response defaults; explicit vhost policy wins.
    #[serde(default)]
    pub headers: HostHeadersConfig,
    #[serde(default)]
    pub request: RouterConfig,
    #[serde(default)]
    pub listener: ListenerOverrides,
}

/// One coherent resolved candidate; transport consumers keep its Arc while
/// building their own generation. No YAML inheritance markers remain here.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct HttpSnapshot {
    pub headers: Arc<HostHeadersConfig>,
    pub router: Arc<RouterConfig>,
    pub listeners: BTreeMap<ListenerId, Arc<ListenerConfig>>,
    #[cfg(feature = "metrics")]
    pub metrics: Arc<HttpMetricsConfig>,
    pub watchdog: Option<Arc<WatchdogConfig>>,
}

impl<'de> Deserialize<'de> for HttpSnapshot {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        HttpYaml::deserialize(deserializer)?
            .resolve()
            .map_err(serde::de::Error::custom)
    }
}

/// Transport capabilities installed by the embedding server. Configuration
/// parsing remains independent of the drivers; publication validates the
/// candidate against this immutable composition boundary.
#[derive(Clone, Copy, Debug)]
pub struct ListenerCapabilities {
    pub tls: bool,
    pub h3: bool,
}

impl ListenerCapabilities {
    /// Unrestricted data-only/config consumers may describe every transport.
    pub const ALL: Self = Self { tls: true, h3: true };

    pub fn validate(
        &self,
        snapshot: &HttpSnapshot,
    ) -> Result<(), ConfigError> {
        for listener in snapshot.listeners.values() {
            match &listener.settings {
                ListenerSettings::Tcp(config)
                    if matches!(config.security, TcpSecurity::Tls { .. })
                        && !self.tls =>
                {
                    return Err(ConfigError::listener(
                        &listener.id,
                        "TLS transport is not enabled in this server",
                    ));
                }
                ListenerSettings::Quic(_) if !self.h3 || !self.tls => {
                    return Err(ConfigError::listener(
                        &listener.id,
                        "HTTP/3 requires enabled H3 and TLS transports",
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl HttpYaml {
    pub fn resolve(self) -> Result<HttpSnapshot, ConfigError> {
        if self.defaults.request.max_concurrent_requests == Some(0) {
            return Err(ConfigError::global(
                "defaults.request.max_concurrent_requests must be positive, or left out for none",
            ));
        }
        self.defaults.listener.validate()?;

        // Accepting without a cap leaves memory as the only backpressure, so a
        // listener that declared nothing gets one derived from what this
        // process can actually hold open. The operator still wins both ways:
        // a number is honoured, and an explicit `max_connections: null` stays
        // unlimited. Counting has to happen before the loop consumes the
        // definitions, and the budget is divided across TCP listeners only —
        // see `derive_max_connections`.
        let tcp_listeners = self
            .listeners
            .values()
            .filter(|definition| definition.transport == ListenerTransport::Tcp)
            .count();
        let undeclared: Vec<ListenerId> = self
            .listeners
            .iter()
            .filter(|(_, definition)| {
                definition.transport == ListenerTransport::Tcp
                    && !definition.declares_max_connections()
                    && !self.defaults.listener.declares_max_connections()
            })
            .map(|(id, _)| id.clone())
            .collect();
        let derived = (!undeclared.is_empty())
            .then(|| derive_max_connections(tcp_listeners))
            .flatten();

        let mut listeners: BTreeMap<ListenerId, Arc<ListenerConfig>> =
            BTreeMap::new();
        for (id, definition) in self.listeners {
            let mut listener =
                definition.resolve(id.clone(), &self.defaults.listener)?;
            if let Some(derived) = derived
                && undeclared.contains(&id)
            {
                listener.limits.max_connections = Some(derived);
                tracing::info!(
                    listener = %id,
                    max_connections = derived.get(),
                    tcp_listeners,
                    "no max_connections declared; derived one from RLIMIT_NOFILE"
                );
            }
            // Reuse-port is a worker-family capability, not permission for two
            // logical listeners in one process to compete for the same bind.
            for previous in listeners.values() {
                if previous.transport() == listener.transport() {
                    for addr in &listener.listen {
                        if addr.port() != 0 && previous.listen.contains(addr) {
                            return Err(ConfigError::listener(
                                &id,
                                format!(
                                    "bind {addr} is also declared by listener '{}'",
                                    previous.id
                                ),
                            ));
                        }
                    }
                }
            }
            listeners.insert(id, Arc::new(listener));
        }
        Ok(HttpSnapshot {
            headers: Arc::new(self.defaults.headers),
            router: Arc::new(self.defaults.request),
            listeners,
            #[cfg(feature = "metrics")]
            metrics: Arc::new(self.metrics),
            watchdog: self.watchdog.map(Arc::new),
        })
    }
}

#[derive(Debug)]
pub struct ConfigError {
    listener: Option<ListenerId>,
    message: String,
}

impl ConfigError {
    pub(crate) fn global(message: impl Into<String>) -> Self {
        Self { listener: None, message: message.into() }
    }
    pub(crate) fn listener(
        id: &ListenerId,
        message: impl Into<String>,
    ) -> Self {
        Self { listener: Some(id.clone()), message: message.into() }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match &self.listener {
            Some(id) => {
                write!(f, "http.yaml listener '{id}': {}", self.message)
            }
            None => write!(f, "http.yaml: {}", self.message),
        }
    }
}
impl std::error::Error for ConfigError {}
