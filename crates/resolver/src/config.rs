use std::net::SocketAddr;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::ResolveError;

/// Resolver policy only; origin boot/failure/refresh policy belongs to consumers.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolverConfig {
    /// Empty uses system configuration; otherwise UDP + TCP at each address.
    #[serde(default)]
    pub nameservers: Vec<SocketAddr>,
    /// Per DNS transport request, not a total lookup/boot/HTTP deadline.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    /// Hickory attempts after lookup failure, before giving up.
    #[serde(default = "default_attempts")]
    pub attempts: usize,
    /// Maximum responses the driver may cache; zero, the default, disables
    /// driver caching. The consumer already keeps each answer with its expiry
    /// and asks again only when it lapses, so a second cache below it would
    /// only hide how old an answer really is.
    #[serde(default = "default_cache_size")]
    pub cache_size: u64,
    #[serde(default = "default_min_ttl", with = "humantime_serde")]
    pub min_ttl: Duration,
    #[serde(default = "default_max_ttl", with = "humantime_serde")]
    pub max_ttl: Duration,
}
// Named defaults are operator policy, not protocol constants.
fn default_timeout() -> Duration {
    Duration::from_secs(5)
}
fn default_attempts() -> usize {
    2
}
fn default_cache_size() -> u64 {
    0
}
fn default_min_ttl() -> Duration {
    Duration::from_secs(5)
}
fn default_max_ttl() -> Duration {
    Duration::from_secs(300)
}
impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            nameservers: vec![],
            timeout: default_timeout(),
            attempts: default_attempts(),
            cache_size: default_cache_size(),
            min_ttl: default_min_ttl(),
            max_ttl: default_max_ttl(),
        }
    }
}
impl ResolverConfig {
    pub fn validate(&self) -> Result<(), ResolveError> {
        if self.timeout.is_zero() {
            return Err(ResolveError::config("timeout must be positive"));
        }
        if self.attempts == 0 {
            return Err(ResolveError::config("attempts must be positive"));
        }
        if self.max_ttl.is_zero() || self.min_ttl > self.max_ttl {
            return Err(ResolveError::config(
                "TTL bounds require 0 <= min_ttl <= max_ttl and max_ttl > 0",
            ));
        }
        for (field, duration) in
            [("timeout", self.timeout), ("max_ttl", self.max_ttl)]
        {
            if Instant::now().checked_add(duration).is_none() {
                return Err(ResolveError::config(format!(
                    "{field} exceeds the monotonic clock range"
                )));
            }
        }
        for addr in &self.nameservers {
            if addr.port() == 0 {
                return Err(ResolveError::config(
                    "nameserver port must be positive",
                ));
            }
        }
        Ok(())
    }
}
