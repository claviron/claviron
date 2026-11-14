use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{OptionsError, PoolOptions};

/// Explicit bounds for a process-local client pool, independent of its manager.
///
/// Capacity values are required. Absent or null durations remain `None`; this
/// declaration never supplies a timeout or retry policy. Code-first callers
/// can continue constructing [`PoolOptions`] directly.
/// Where a pool's admission bounds hold.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PoolScope {
    /// Each process applies the bounds to itself.
    Process,
    /// The fleet shares one request budget (`max_concurrency`) in shared
    /// memory; clients and waiting stay in each process. Refused where no
    /// fleet exists.
    Fleet,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    /// Required: it changes what the bounds mean.
    pub scope: PoolScope,
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

/// Outbound HTTP calls through a client pool: how long a connection and a
/// request may take, how long admission pauses after a rate limit that names
/// no wait, and the pool's bounds. A declaration only: the consumer builds
/// its HTTP client and pool from it, and supplies its own defaults, if any.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpClientConfig {
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    /// Absent: no pause; the next caller sees the server's own answer.
    #[serde(default, with = "humantime_serde")]
    pub rate_limit_pause: Option<Duration>,
    pub pool: PoolConfig,
}

impl HttpClientConfig {
    /// The same checks as [`PoolConfig::validate`], and non-zero timeouts.
    pub fn validate(&self) -> Result<(), OptionsError> {
        for (name, value) in [
            ("connect_timeout", Some(self.connect_timeout)),
            ("request_timeout", Some(self.request_timeout)),
            ("rate_limit_pause", self.rate_limit_pause),
        ] {
            if value.is_some_and(|duration| duration.is_zero()) {
                return Err(OptionsError::ZeroDuration(name));
            }
        }
        self.pool.validate()
    }
}

#[cfg(test)]
#[path = "../../tests/clients/config.rs"]
mod tests;
