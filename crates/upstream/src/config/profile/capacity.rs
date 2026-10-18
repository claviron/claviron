//! Owns file-level process-local pool capacity declarations.

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityConfig {
    /// H1 connection ceiling. For H2 this remains the compatibility fallback
    /// when `max_streams` is omitted.
    pub max_live: usize,
    /// Concurrent streams admitted on this worker's H2 connection.
    #[serde(default)]
    pub max_streams: Option<usize>,
    #[serde(default)]
    pub max_idle: Option<usize>,
    /// Warm floor on live H1 connections per serving worker; leased counts too.
    #[serde(default = "default_min_idle")]
    pub min_idle: usize,
    #[serde(default)]
    pub replenish: ReplenishConfig,
}

fn default_min_idle() -> usize {
    0
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
