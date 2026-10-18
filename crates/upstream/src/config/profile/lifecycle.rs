//! Owns the explicit per-profile stop drain deadline.

use std::time::Duration;

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleConfig {
    #[serde(default = "default_drain_timeout", with = "humantime_serde")]
    pub drain_timeout: Duration,
}
impl Default for LifecycleConfig {
    fn default() -> Self {
        Self { drain_timeout: default_drain_timeout() }
    }
}
fn default_drain_timeout() -> Duration {
    Duration::from_secs(30)
}
