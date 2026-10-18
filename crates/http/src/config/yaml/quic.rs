use std::time::Duration;

use serde::Deserialize;

use super::ConfigError;

/// QUIC connection policy, separate from the H3 request-body adapter.
#[derive(
    Clone, Debug, Eq, PartialEq, Deserialize, claviron_macros::Overridable,
)]
#[overridable(helpers = "crate::config::helpers")]
#[serde(deny_unknown_fields)]
pub struct QuicConfig {
    #[serde(default = "default_streams")]
    pub max_concurrent_bidi_streams: u32,
    #[serde(default = "default_streams")]
    pub max_concurrent_uni_streams: u32,
    #[serde(
        default = "default_idle_timeout",
        deserialize_with = "super::helpers::required_duration"
    )]
    pub idle_timeout: Duration,
    #[serde(
        default,
        deserialize_with = "claviron_helpers::de::deserialize_optional_duration"
    )]
    pub keep_alive_interval: Option<Duration>,
    #[serde(default)]
    pub enable_early_data: bool,
    /// Largest request head accepted over HTTP/3, sent to the client as
    /// `SETTINGS_MAX_FIELD_SECTION_SIZE`. 16 KiB, the same as HTTP/2's
    /// `max_header_list_size`: h3 on its own accepts any size, so without this
    /// one protocol would let in heads the other two refuse.
    #[serde(default = "default_max_field_section_size")]
    pub max_field_section_size: u64,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            max_concurrent_bidi_streams: default_streams(),
            max_concurrent_uni_streams: default_streams(),
            idle_timeout: default_idle_timeout(),
            keep_alive_interval: None,
            enable_early_data: false,
            max_field_section_size: default_max_field_section_size(),
        }
    }
}

pub(crate) fn validate(quic: &QuicConfig) -> Result<(), ConfigError> {
    if quic.max_concurrent_bidi_streams == 0
        || quic.max_concurrent_uni_streams == 0
    {
        return Err(ConfigError::global("QUIC stream limits must be positive"));
    }
    if quic.idle_timeout.is_zero() {
        return Err(ConfigError::global("quic.idle_timeout must be positive"));
    }
    if quic.keep_alive_interval.is_some_and(|interval| {
        interval.is_zero() || interval >= quic.idle_timeout
    }) {
        return Err(ConfigError::global(
            "quic.keep_alive_interval must be positive and shorter than idle_timeout, or null",
        ));
    }
    if quic.max_field_section_size == 0 {
        return Err(ConfigError::global(
            "quic.max_field_section_size must be positive",
        ));
    }
    Ok(())
}

fn default_max_field_section_size() -> u64 {
    1024 * 16
}

fn default_streams() -> u32 {
    100
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(60)
}
