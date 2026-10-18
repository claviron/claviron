//! Optional worker/profile-wide limits; unset means no additional breaker.
use serde::Deserialize;
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BreakerConfig {
    #[serde(default = "default_limit")]
    pub max_connections: Option<usize>,
    #[serde(default = "default_limit")]
    pub max_pending: Option<usize>,
    #[serde(default = "default_limit")]
    pub max_requests: Option<usize>,
    #[serde(default = "default_limit")]
    pub max_retries: Option<usize>,
}
fn default_limit() -> Option<usize> {
    None
}
impl BreakerConfig {
    pub(crate) fn enabled(&self) -> bool {
        self.max_connections.is_some()
            || self.max_pending.is_some()
            || self.max_requests.is_some()
            || self.max_retries.is_some()
    }
}
