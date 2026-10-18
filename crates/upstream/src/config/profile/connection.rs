//! Owns connection options, sparse overrides and their defaults.

use std::time::Duration;

use claviron_macros::Overridable;
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Overridable)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// Longest to open a connection to an origin — the TCP or Unix dial, and
    /// the L4 health probe's; also the resolver's total budget for its names.
    /// The connection's time, not a request's (the TLS handshake has its own
    /// `tls.handshake_timeout`). Every profile decides it, as a duration or
    /// `off`; the library does not pick one. `off` waits for the operating
    /// system's own connect timeout.
    #[overridable(
        deserialize_with = "crate::config::overrides::duration_or_off"
    )]
    #[serde(
        default,
        deserialize_with = "crate::config::overrides::plain_duration_or_off"
    )]
    pub connect_timeout: Option<Duration>,
    #[serde(default = "default_true")]
    pub tcp_nodelay: bool,
    #[serde(default = "default_idle_timeout", with = "humantime_serde")]
    pub idle_timeout: Duration,
    #[serde(
        default,
        deserialize_with = "claviron_helpers::de::deserialize_optional_duration"
    )]
    pub keep_alive_interval: Option<Duration>,
    #[serde(default = "default_keep_alive_timeout", with = "humantime_serde")]
    pub keep_alive_timeout: Duration,
    #[serde(default = "default_false")]
    pub keep_alive_while_idle: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: None,
            tcp_nodelay: true,
            idle_timeout: default_idle_timeout(),
            keep_alive_interval: None,
            keep_alive_timeout: default_keep_alive_timeout(),
            keep_alive_while_idle: false,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_keep_alive_timeout() -> Duration {
    Duration::from_secs(20)
}

fn default_false() -> bool {
    false
}
