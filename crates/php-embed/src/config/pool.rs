//! Request-pool construction and validation.

use std::time::Duration;

use super::default_instances;

/// Capacity and maintenance policy for ordinary request-scoped PHP execution.
///
/// Each instance owns one OS thread and one ZTS/TSRM context. PHP request
/// startup and shutdown still run for every request; reusing a thread does not
/// preserve the application's PHP objects between requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestPool {
    pub(crate) min_idle: u8,
    pub(crate) max_size: u8,
    pub(crate) max_idle: u8,
    pub(crate) idle_timeout: Duration,
    pub(crate) max_life: Option<Duration>,
    pub(crate) max_requests: Option<u64>,
}

impl RequestPool {
    /// Start with one warm instance. Set a larger maximum to enable growth.
    ///
    /// Explicit capacity settings are validated together when the service is
    /// registered; setters never silently clamp a supplied value.
    pub fn elastic() -> Self {
        Self::fixed(default_instances())
    }

    /// Keep exactly `instances` request-execution threads alive.
    pub fn fixed(instances: u8) -> Self {
        Self {
            min_idle: instances,
            max_size: instances,
            max_idle: instances,
            idle_timeout: default_idle_timeout(),
            max_life: None,
            max_requests: None,
        }
    }

    /// Minimum number of request threads retained by the pool.
    pub fn min_idle(
        mut self,
        instances: u8,
    ) -> Self {
        self.min_idle = instances;
        self
    }

    /// Maximum number of request threads, including growth under load.
    pub fn max_size(
        mut self,
        instances: u8,
    ) -> Self {
        self.max_size = instances;
        self
    }

    /// Warm idle capacity retained after a burst. Must be between the idle
    /// floor and the live ceiling.
    pub fn max_idle(
        mut self,
        instances: u8,
    ) -> Self {
        self.max_idle = instances;
        self
    }

    /// How long an excess thread must stay idle before it can be removed.
    /// Defaults to 60 seconds; zero permits removal on the next culler pass.
    pub fn idle_timeout(
        mut self,
        timeout: Duration,
    ) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Renew or remove an idle thread once its lifetime reaches this bound.
    /// Active requests are never interrupted. `None` disables age-based renewal.
    pub fn max_life(
        mut self,
        lifetime: Option<Duration>,
    ) -> Self {
        self.max_life = lifetime;
        self
    }

    /// Renew an idle thread after this many completed requests.
    /// `None` disables request-count rotation.
    pub fn max_requests(
        mut self,
        requests: Option<u64>,
    ) -> Self {
        self.max_requests = requests;
        self
    }

    /// Check the capacity contract without starting PHP or changing state.
    pub fn validate(&self) -> Result<(), String> {
        if self.min_idle == 0 {
            return Err("PHP request pool min_idle must be at least 1".into());
        }
        if self.max_size < self.min_idle {
            return Err("PHP request pool max_size must be >= min_idle".into());
        }
        if self.max_idle < self.min_idle || self.max_idle > self.max_size {
            return Err("PHP request pool must satisfy min_idle <= max_idle <= max_size".into());
        }
        if self.max_life.is_some_and(|duration| duration.is_zero()) {
            return Err(
                "PHP request pool max_life must be positive when configured"
                    .into(),
            );
        }
        if self.max_requests == Some(0) {
            return Err(
                "PHP request pool max_requests must be at least 1".into()
            );
        }
        Ok(())
    }
}

impl Default for RequestPool {
    fn default() -> Self {
        Self::fixed(default_instances())
    }
}

fn default_idle_timeout() -> Duration {
    Duration::from_secs(60)
}
