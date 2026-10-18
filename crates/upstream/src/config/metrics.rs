//! Owns upstream fleet-metrics cadence and freshness options.

use std::time::Duration;

use serde::Deserialize;

#[cfg(feature = "metrics")]
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct UpstreamMetricsConfig {
    #[serde(default = "default_metrics_interval", with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default = "default_metrics_max_age", with = "humantime_serde")]
    pub max_age: Duration,
}

#[cfg(feature = "metrics")]
impl Default for UpstreamMetricsConfig {
    fn default() -> Self {
        Self {
            interval: default_metrics_interval(),
            max_age: default_metrics_max_age(),
        }
    }
}

#[cfg(feature = "metrics")]
fn default_metrics_interval() -> Duration {
    Duration::from_secs(1)
}

#[cfg(feature = "metrics")]
fn default_metrics_max_age() -> Duration {
    Duration::from_secs(5)
}
