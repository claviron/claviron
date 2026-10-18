use std::time::Duration;

use claviron_helpers::de::deserialize_duration;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct RateLimitConfig {
    #[serde(deserialize_with = "deserialize_duration")]
    pub duration: Duration,

    pub max_attempts: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self { duration: Duration::from_secs(60), max_attempts: 100 }
    }
}
