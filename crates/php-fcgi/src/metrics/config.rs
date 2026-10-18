use std::time::Duration;

use claviron_helpers::de::deserialize_duration;
use claviron_orbit::placement::DispatchPolicy;
use serde::Deserialize;

/// The `metrics:` section of `fpm.yaml`, as the metrics providers own it.
/// The catalog is `PhpFcgiService`'s: one file, each owner its own section,
/// so this container accepts the sections it does not read.
#[derive(Default, Deserialize, PartialEq)]
pub struct MetricsSection {
    #[serde(default)]
    pub(crate) metrics: Option<FpmMetricsConfig>,
}

#[derive(Clone, Deserialize, PartialEq)]
pub(crate) struct FpmMetricsConfig {
    /// Singleton collector ownership; enabled metrics require explicit timing.
    #[serde(default)]
    pub(crate) dispatch_policy: Option<DispatchPolicy>,
    #[serde(default = "default_metrics_enabled")]
    pub(crate) enabled: bool,
    #[serde(
        default = "default_metrics_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) interval: Duration,
    #[serde(
        default = "default_publish_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) publish_interval: Duration,
    #[serde(
        default = "default_max_age",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) max_age: Duration,
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_publish_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_max_age() -> Duration {
    Duration::from_secs(5)
}
