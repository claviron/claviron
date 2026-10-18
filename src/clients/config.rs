use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{OptionsError, PoolOptions};

/// Explicit bounds for a process-local client pool, independent of its manager.
///
/// Capacity values are required. Absent or null durations remain `None`; this
/// declaration never supplies a timeout or retry policy. Code-first callers
/// can continue constructing [`PoolOptions`] directly.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    pub min_idle: usize,
    pub max_idle: usize,
    pub max_live: usize,
    pub max_concurrency: usize,
    pub max_waiting: usize,
    #[serde(default, with = "humantime_serde")]
    pub acquire_timeout: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    pub idle_timeout: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    pub max_lifetime: Option<Duration>,
}

impl PoolConfig {
    /// Check the same bounds required by the runtime pool before booting it.
    /// Pool construction still validates code-first options independently.
    pub fn validate(&self) -> Result<(), OptionsError> {
        if self.max_live == 0 {
            return Err(OptionsError::ZeroMaxLive);
        }
        if self.max_concurrency == 0 {
            return Err(OptionsError::ZeroMaxConcurrency);
        }
        if self.min_idle > self.max_idle || self.max_idle > self.max_live {
            return Err(OptionsError::CapacityOrder {
                min_idle: self.min_idle,
                max_idle: self.max_idle,
                max_live: self.max_live,
            });
        }
        for (name, value) in [
            ("acquire_timeout", self.acquire_timeout),
            ("idle_timeout", self.idle_timeout),
            ("max_lifetime", self.max_lifetime),
        ] {
            if value.is_some_and(|duration| duration.is_zero()) {
                return Err(OptionsError::ZeroDuration(name));
            }
        }
        Ok(())
    }
}

impl From<&PoolConfig> for PoolOptions {
    fn from(config: &PoolConfig) -> Self {
        Self::new(
            config.min_idle,
            config.max_idle,
            config.max_live,
            config.max_concurrency,
            config.max_waiting,
        )
        .acquire_timeout(config.acquire_timeout)
        .idle_timeout(config.idle_timeout)
        .max_lifetime(config.max_lifetime)
    }
}

impl From<PoolConfig> for PoolOptions {
    fn from(config: PoolConfig) -> Self {
        Self::from(&config)
    }
}

#[cfg(test)]
#[path = "../../tests/clients/config.rs"]
mod tests;
