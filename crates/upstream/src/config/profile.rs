//! Owns the profile protocol and file-level capacity, connection and health options.

use serde::Deserialize;

use crate::config::origin::OriginConfig;
use crate::config::profile::capacity::CapacityConfig;
use crate::config::profile::connection::ConnectionOverride;
use crate::config::profile::health::HealthOverride;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum UpstreamProtocol {
    H1,
    H2,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProfileConfig {
    pub protocol: UpstreamProtocol,
    #[serde(default)]
    pub resolve: resolve::ResolveConfig,
    // An inactive capability consumes its section without enabling a driver.
    #[serde(default)]
    pub origins: Vec<OriginConfig>,
    #[serde(default)]
    pub authority: Option<String>,
    pub capacity: CapacityConfig,
    /// Removed; read only to say where each field went. It named its fields
    /// by their type rather than by what they bound (`docs/REVIEW.md` §3e).
    #[serde(default)]
    pub timeouts: Option<serde::de::IgnoredAny>,
    #[serde(default)]
    pub connection: ConnectionOverride,
    #[serde(default)]
    pub health: HealthOverride,
    #[serde(default)]
    pub tls: tls::TlsConfig,
    #[serde(default)]
    pub balance: balance::BalanceConfig,
    #[serde(default)]
    pub forward: forward::ForwardConfig,
    #[serde(default)]
    pub retry: retry::RetryConfig,
    /// This upstream application's request policy, narrowing its host's
    /// and its listener's: when a body is held (and so can be retried),
    /// how long a gap in it may last, how long the reply's head may take
    /// (`docs/REVIEW.md` §3e, step 5c).
    #[serde(default)]
    pub requests: claviron_http::policy::RequestPolicyOverride,
    /// This upstream application's reply policy: `body_idle_timeout` is the
    /// longest gap between two pieces of the origin's reply.
    #[serde(default)]
    pub responses: claviron_http::policy::ResponsePolicyOverride,
    #[serde(default)]
    pub outlier: outlier::OutlierConfig,
    #[serde(default)]
    pub breaker: breaker::BreakerConfig,
    #[serde(default)]
    pub lifecycle: lifecycle::LifecycleConfig,
}

pub(crate) mod capacity;
pub(crate) mod connection;
pub(crate) mod health;

pub(crate) mod tls;

pub(crate) mod balance;

pub(crate) mod forward;

pub(crate) mod retry;

pub(crate) mod breaker;
pub(crate) mod outlier;

pub(crate) mod lifecycle;

pub(crate) mod resolve;
