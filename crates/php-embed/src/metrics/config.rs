use std::time::Duration;

/// Metrics policy supplied by the embedding application.
///
/// This type does not read configuration files. The application may construct
/// it directly or translate its own configuration format into these values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhpEmbedMetricsConfig {
    /// Publish and render the embedded PHP metrics family. Defaults to true.
    pub enabled: bool,
    /// Orbit publication cadence. Must be positive; defaults to one second.
    pub publish_interval: Duration,
    /// Maximum age of a fleet sample, in whole seconds. Defaults to five seconds.
    pub max_age: Duration,
}

impl PhpEmbedMetricsConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.publish_interval.is_zero() {
            return Err("PHP metrics publish_interval must be positive".into());
        }
        Ok(())
    }
}

impl Default for PhpEmbedMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            publish_interval: default_publish_interval(),
            max_age: default_max_age(),
        }
    }
}

pub(crate) struct PhpEmbedMetricsConfigProvider {
    config: PhpEmbedMetricsConfig,
}

impl PhpEmbedMetricsConfigProvider {
    pub(crate) fn new(config: PhpEmbedMetricsConfig) -> Self {
        Self { config }
    }

    pub(crate) fn config(&self) -> PhpEmbedMetricsConfig {
        self.config
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
