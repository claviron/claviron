use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::config::ConfigError;

/// Per-socket recovery from OS accept resource pressure. A running listener
/// captures this policy until stop/start, independently of request snapshots.
#[derive(
    Clone, Debug, Deserialize, PartialEq, Eq, claviron_macros::Overridable,
)]
#[overridable(helpers = "crate::config::helpers")]
#[serde(deny_unknown_fields)]
pub struct AcceptRetryConfig {
    #[serde(
        default = "default_accept_retry_initial",
        deserialize_with = "crate::config::helpers::required_duration"
    )]
    pub initial: Duration,
    #[serde(
        default = "default_accept_retry_max",
        deserialize_with = "crate::config::helpers::required_duration"
    )]
    pub max: Duration,
}

impl Default for AcceptRetryConfig {
    fn default() -> Self {
        Self {
            initial: default_accept_retry_initial(),
            max: default_accept_retry_max(),
        }
    }
}

impl AcceptRetryConfig {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.initial.is_zero() || self.max.is_zero() {
            return Err(ConfigError::global(
                "tcp.accept_retry durations must be positive",
            ));
        }
        if self.initial > self.max {
            return Err(ConfigError::global(
                "tcp.accept_retry.initial must not exceed max",
            ));
        }
        if Instant::now().checked_add(self.max).is_none() {
            return Err(ConfigError::global(
                "tcp.accept_retry.max exceeds the platform's monotonic clock range",
            ));
        }
        Ok(())
    }
}

fn default_accept_retry_initial() -> Duration {
    Duration::from_millis(50)
}

fn default_accept_retry_max() -> Duration {
    Duration::from_secs(1)
}
