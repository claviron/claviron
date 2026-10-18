//! Owns balancer selection and slow-start declaration.

use std::time::Duration;

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BalanceConfig {
    #[serde(default = "default_algorithm")]
    pub algorithm: Algorithm,
    #[serde(default = "default_scope")]
    pub scope: Scope,
    #[serde(default = "default_decay", with = "humantime_serde")]
    pub decay: Duration,
    #[serde(default = "default_key")]
    pub key: Option<String>,
    #[serde(default = "default_replicas")]
    pub replicas: usize,
    #[serde(default = "default_slow_start")]
    pub slow_start: SlowStartConfig,
}
impl Default for BalanceConfig {
    fn default() -> Self {
        Self {
            algorithm: default_algorithm(),
            scope: default_scope(),
            decay: default_decay(),
            key: default_key(),
            replicas: default_replicas(),
            slow_start: default_slow_start(),
        }
    }
}
fn default_algorithm() -> Algorithm {
    Algorithm::RoundRobin
}
fn default_scope() -> Scope {
    Scope::Worker
}
fn default_decay() -> Duration {
    Duration::from_secs(10)
}
fn default_key() -> Option<String> {
    None
}
fn default_replicas() -> usize {
    160
}
fn default_slow_start() -> SlowStartConfig {
    SlowStartConfig::default()
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    // The hyphenated spellings are accepted too: both are what people write,
    // and a config that fails on a dash is a config written twice.
    #[serde(alias = "round-robin")]
    RoundRobin,
    #[serde(alias = "least-connections")]
    LeastConnections,
    #[serde(alias = "peak-ewma")]
    PeakEwma,
    #[serde(alias = "consistent-hash")]
    ConsistentHash,
    /// Fill-first, HAProxy's `balance first`: the first healthy origin that is
    /// below its limit — the pool's `capacity.max_live`, or the origin's own
    /// `max_live` — so a light load stays on few processes, awake and warm,
    /// and the rest sleep. Each edge worker starts from its own origin, so the
    /// workers fill different processes. When every origin is at its limit,
    /// the least full of them — never a refusal.
    First,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Worker,
    Fleet,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlowStartConfig {
    #[serde(default = "default_window", with = "humantime_serde")]
    pub window: Duration,
    #[serde(default = "default_initial_percent")]
    pub initial_percent: u8,
}
impl Default for SlowStartConfig {
    fn default() -> Self {
        Self {
            window: default_window(),
            initial_percent: default_initial_percent(),
        }
    }
}
fn default_window() -> Duration {
    Duration::ZERO
}
fn default_initial_percent() -> u8 {
    10
}
