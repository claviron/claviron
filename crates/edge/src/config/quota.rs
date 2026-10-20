//! IP admission declared by a host or an explicit route. Not a route extension.

use std::num::NonZeroU64;

use claviron_http::rate_limit::{IpKey, KeyProvider};
use claviron_orbit::quota::QuotaConfig;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Key namespace inside this definition's independent store.
    pub namespace: String,
    pub cost: NonZeroU64,
    /// Successful replies report this limiter's admission snapshot. Off by default.
    #[serde(default)]
    pub headers: bool,
    pub quota: QuotaConfig,
}

impl RateLimitConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.quota.validate().map_err(|error| error.to_string())?;
        let key_bound = IpKey.max_key_bytes();
        if self.namespace.is_empty()
            || self
                .namespace
                .len()
                .checked_add(key_bound)
                .is_none_or(|len| len > self.quota.table.key_bytes.get())
            || self.cost > self.quota.burst
        {
            return Err(
                "rate_limit namespace, IP key or cost cannot fit its quota"
                    .into(),
            );
        }
        Ok(())
    }
}
