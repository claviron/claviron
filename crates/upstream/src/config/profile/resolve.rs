//! DNS transport policy is shared; membership and scheduling belong to upstream.
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use claviron_resolver::{ResolveError, ResolveErrorKind, ResolverConfig};
use serde::{Deserialize, Serialize};

/// Cold upstream membership policy; no lookup is performed by parsing this type.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResolveConfig {
    pub nameservers: Vec<SocketAddr>,
    /// Per DNS transport request. The coordinator owns the total lookup budget.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub attempts: usize,
    pub cache_size: u64,
    #[serde(with = "humantime_serde")]
    pub min_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub max_ttl: Duration,
    /// Zero follows the existing answer expiry; nonzero is a refresh cadence.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Retry after errors, occupied owner duty, or an already expired answer.
    /// Also bounds cold native-retirement receipt checks, without a drain deadline
    /// or rescheduling DNS queries / refreshing cached TTL.
    #[serde(with = "humantime_serde")]
    pub retry_interval: Duration,
    pub max_addresses: usize,
    pub family: Family,
    pub on_failure: OnFailure,
    pub boot: Boot,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Family {
    #[default]
    Any,
    Ipv4,
    Ipv6,
}
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum OnFailure {
    #[default]
    Keep,
    Empty,
}
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Boot {
    #[default]
    Require,
    AllowEmpty,
}

fn default_resolve_interval() -> Duration {
    Duration::ZERO
}
// Errors and expired/zero-TTL cache reads must not become a busy query loop.
fn default_resolve_retry_interval() -> Duration {
    Duration::from_secs(5)
}
fn default_resolve_max_addresses() -> usize {
    16
}
impl Default for ResolveConfig {
    fn default() -> Self {
        let shared = ResolverConfig::default();
        Self {
            nameservers: shared.nameservers,
            timeout: shared.timeout,
            attempts: shared.attempts,
            cache_size: shared.cache_size,
            min_ttl: shared.min_ttl,
            max_ttl: shared.max_ttl,
            interval: default_resolve_interval(),
            retry_interval: default_resolve_retry_interval(),
            max_addresses: default_resolve_max_addresses(),
            family: Family::default(),
            on_failure: OnFailure::default(),
            boot: Boot::default(),
        }
    }
}
impl ResolveConfig {
    /// Delegate DNS transport/cache validation to its owning shared crate.
    pub fn backend(&self) -> ResolverConfig {
        ResolverConfig {
            nameservers: self.nameservers.clone(),
            timeout: self.timeout,
            attempts: self.attempts,
            cache_size: self.cache_size,
            min_ttl: self.min_ttl,
            max_ttl: self.max_ttl,
        }
    }
    pub fn validate(&self) -> Result<(), ResolveError> {
        self.backend().validate()?;
        // OriginId is a u16 protocol identity; at least one slot is required.
        if self.max_addresses == 0
            || self.max_addresses > usize::from(u16::MAX) + 1
        {
            return Err(ResolveError::new(
                ResolveErrorKind::Config,
                "max_addresses must fit the nonempty OriginId domain",
            ));
        }
        if self.retry_interval.is_zero()
            || Instant::now().checked_add(self.interval).is_none()
            || Instant::now().checked_add(self.retry_interval).is_none()
        {
            return Err(ResolveError::new(
                ResolveErrorKind::Config,
                "intervals must fit the monotonic clock and retry_interval must be positive",
            ));
        }
        Ok(())
    }
}
