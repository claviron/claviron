use std::time::Duration;

use claviron_helpers::de::deserialize_duration;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct HttpMetricsConfig {
    #[serde(default = "default_enabled")]
    pub(crate) enabled: bool,

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

impl Default for HttpMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),

            publish_interval: default_publish_interval(),

            max_age: default_max_age(),
        }
    }
}

fn default_enabled() -> bool {
    true
}

fn default_publish_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_max_age() -> Duration {
    Duration::from_secs(5)
}
