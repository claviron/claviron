//! Owns serving-axis outlier detection thresholds.

use std::time::Duration;

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutlierConfig {
    #[serde(default = "default_scope")]
    pub scope: super::balance::Scope,
    #[serde(default = "default_consecutive_5xx")]
    pub consecutive_5xx: u32,
    #[serde(default = "default_consecutive_gateway_errors")]
    pub consecutive_gateway_errors: u32,
    #[serde(default = "default_interval", with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default = "default_base_ejection_time", with = "humantime_serde")]
    pub base_ejection_time: Duration,
    #[serde(default = "default_max_ejection_percent")]
    pub max_ejection_percent: u8,
}
impl OutlierConfig {
    /// A zero threshold disables that detector; both zero disable the policy.
    pub(crate) fn enabled(&self) -> bool {
        self.consecutive_5xx > 0 || self.consecutive_gateway_errors > 0
    }
}
impl Default for OutlierConfig {
    fn default() -> Self {
        Self {
            scope: default_scope(),
            consecutive_5xx: default_consecutive_5xx(),
            consecutive_gateway_errors: default_consecutive_gateway_errors(),
            interval: default_interval(),
            base_ejection_time: default_base_ejection_time(),
            max_ejection_percent: default_max_ejection_percent(),
        }
    }
}
fn default_scope() -> super::balance::Scope {
    super::balance::Scope::Worker
}
fn default_consecutive_5xx() -> u32 {
    0
}
fn default_consecutive_gateway_errors() -> u32 {
    0
}
fn default_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_base_ejection_time() -> Duration {
    Duration::from_secs(30)
}
fn default_max_ejection_percent() -> u8 {
    50
}
