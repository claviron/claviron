//! Owns file-level process-local pool capacity declarations.

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityConfig {
    /// Physical connection ceiling per origin and serving worker; required.
    #[serde(default)]
    pub max_live: Option<usize>,
    /// Local concurrent-stream ceiling on this worker's H2 connection.
    /// Omitted: use the peer limit without an additional configured ceiling.
    #[serde(default)]
    pub max_streams: Option<usize>,
    #[serde(default)]
    pub max_idle: Option<usize>,
    /// Warm floor on live H1 connections per serving worker; leased counts too.
    #[serde(default = "default_min_idle")]
    pub min_idle: Option<usize>,
    /// Prepare connections asynchronously at lifecycle activation. H1 prepares
    /// its min_idle floor; H2 prepares one multiplexed connection.
    #[serde(default = "default_warmup")]
    pub warmup: bool,
    #[serde(default)]
    pub replenish: ReplenishConfig,
}

fn default_min_idle() -> Option<usize> {
    None
}
fn default_warmup() -> bool {
    false
}
/// Event-driven maintenance retry policy, separate from request/probe timeouts.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplenishConfig {
    #[serde(default = "default_initial_backoff", with = "humantime_serde")]
    pub initial_backoff: std::time::Duration,
    #[serde(default = "default_max_backoff", with = "humantime_serde")]
    pub max_backoff: std::time::Duration,
}
impl Default for ReplenishConfig {
    fn default() -> Self {
        Self {
            initial_backoff: default_initial_backoff(),
            max_backoff: default_max_backoff(),
        }
    }
}
fn default_initial_backoff() -> std::time::Duration {
    std::time::Duration::from_millis(100)
}
fn default_max_backoff() -> std::time::Duration {
    std::time::Duration::from_secs(5)
}
