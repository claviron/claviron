//! Desired HTTP configuration and its validated, immutable runtime snapshot.

mod execution;
mod listeners;
mod proxy;
mod quic;
mod tcp;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use claviron_helpers::de::deserialize_regex_set;
pub use claviron_helpers::overrides as helpers;
pub use execution::PlainExecution;
pub use helpers::Override;
pub use listeners::{
    ConnectionLimits, ListenerConfig, ListenerDefinition, ListenerOverrides,
    ListenerSettings, QuicListenerConfig, TcpListenerConfig, TcpSecurity,
};
pub use proxy::{ProxyConfig, ProxyOverride};
pub use quic::{QuicConfig, QuicOverride};
use regex::RegexSet;
use serde::Deserialize;
pub use tcp::{
    AcceptRetryConfig, AcceptRetryOverride, H1Config, H1Override, H2Config,
    H2Override, HeaderReadTimeout, TcpConfig, TcpOverride,
};

use crate::ListenerId;
use crate::config::host::HostHeadersConfig;
#[cfg(feature = "metrics")]
use crate::metrics::HttpMetricsConfig;
pub use crate::policy::{
    BodyNeed, Buffering, Delivery, HttpPolicy, HttpPolicyOverride, Limit,
    RequestPolicy, RequestPolicyOverride, ResponsePolicy,
    ResponsePolicyOverride,
};
use crate::watchdog::WatchdogConfig;

/// Full http.yaml declaration. A key this build has no field for is not
/// read: its fields change with the features compiled in.
#[derive(Default, Deserialize)]
pub struct HttpYaml {
    #[serde(default = "execution::default_plain_execution")]
    pub plain_execution: PlainExecution,
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
    /// Path-deny defaults. An empty set installs no host layer.
    #[serde(default, deserialize_with = "deserialize_regex_set")]
    pub deny: RegexSet,
    #[serde(default)]
    pub listener: ListenerOverrides,
}

/// One coherent resolved candidate; transport consumers keep its Arc while
/// building their own generation. No YAML inheritance markers remain here.
#[derive(Clone, Default)]
pub struct HttpSnapshot {
    pub plain_execution: PlainExecution,
    pub headers: Arc<HostHeadersConfig>,
    pub deny: RegexSet,
    pub listeners: BTreeMap<ListenerId, Arc<ListenerConfig>>,
    #[cfg(feature = "metrics")]
    pub metrics: Arc<HttpMetricsConfig>,
    pub watchdog: Option<Arc<WatchdogConfig>>,
}

impl PartialEq for HttpSnapshot {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        #[cfg(feature = "metrics")]
        if self.metrics != other.metrics {
            return false;
        }
        self.plain_execution == other.plain_execution
            && self.headers == other.headers
            && self.deny.patterns() == other.deny.patterns()
            && self.listeners == other.listeners
            && self.watchdog == other.watchdog
    }
}

impl Eq for HttpSnapshot {}

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
        self.defaults.listener.validate()?;

        let mut listeners: BTreeMap<ListenerId, Arc<ListenerConfig>> =
            BTreeMap::new();
        for (id, definition) in self.listeners {
            let listener =
                definition.resolve(id.clone(), &self.defaults.listener)?;
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
            plain_execution: self.plain_execution,
            headers: Arc::new(self.defaults.headers),
            deny: self.defaults.deny,
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

#[cfg(test)]
#[path = "../../tests/config/yaml.rs"]
mod tests;
