use std::num::NonZeroU32;
use std::time::Duration;

use claviron_helpers::de::deserialize_duration;
use serde::{Deserialize, Deserializer};

mod accept;
pub use accept::{AcceptRetryConfig, AcceptRetryOverride};

#[derive(
    Debug, Deserialize, Clone, PartialEq, Eq, claviron_macros::Overridable,
)]
#[overridable(helpers = "crate::config::helpers")]
#[serde(deny_unknown_fields)]
pub struct TcpConfig {
    /// Pending connections per listening socket, before accept. Defaults to
    /// 1024; the OS may cap it. This is not an active-connection limit.
    /// Changing it requires restarting the listener, not a policy reload.
    #[serde(default = "default_tcp_backlog")]
    pub backlog: NonZeroU32,

    #[serde(default = "default_true")]
    pub nodelay: bool,

    #[serde(
        default = "default_tcp_keepalive",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "claviron_helpers::de::deserialize_optional_duration"
    )]
    pub keepalive: Option<Duration>,

    #[serde(default)]
    pub accept_retry: AcceptRetryConfig,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            backlog: default_tcp_backlog(),
            nodelay: true,
            keepalive: default_tcp_keepalive(),
            accept_retry: AcceptRetryConfig::default(),
        }
    }
}

impl TcpConfig {
    pub(crate) fn validate(&self) -> Result<(), super::ConfigError> {
        if i32::try_from(self.backlog.get()).is_err() {
            return Err(super::ConfigError::global(
                "tcp.backlog must be at most 2147483647",
            ));
        }
        self.accept_retry.validate()?;
        if self.keepalive.is_some_and(|value| value.is_zero()) {
            return Err(super::ConfigError::global(
                "tcp.keepalive must be positive or null",
            ));
        }
        Ok(())
    }
}

/// Preserve the existing socket queue default in the public TCP configuration.
fn default_tcp_backlog() -> NonZeroU32 {
    NonZeroU32::new(1024).expect("the default TCP backlog is positive")
}

/// Resolved H1 header deadline, distinct from declaration inheritance.
///
/// An entirely omitted setting retains the driver's own default. Only an
/// explicit null disables the timer; transports must not collapse the two.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HeaderReadTimeout {
    #[default]
    DriverDefault,
    Disabled,
    After(Duration),
}

impl HeaderReadTimeout {
    fn explicit(value: Option<Duration>) -> Self {
        match value {
            Some(duration) => Self::After(duration),
            None => Self::Disabled,
        }
    }
}

impl<'de> Deserialize<'de> for HeaderReadTimeout {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        claviron_helpers::de::deserialize_optional_duration(deserializer)
            .map(Self::explicit)
    }
}

#[derive(
    Debug, Deserialize, Clone, PartialEq, Eq, claviron_macros::Overridable,
)]
#[overridable(helpers = "crate::config::helpers")]
#[serde(deny_unknown_fields)]
pub struct H1Config {
    /// The one field whose override is not its own type: an explicit value has
    /// to stay distinguishable from an inherited one after resolution.
    #[serde(default)]
    #[overridable(
        sparse = "Option<Duration>",
        resolve = "HeaderReadTimeout::explicit"
    )]
    pub header_read_timeout: HeaderReadTimeout,

    #[serde(default = "default_h1_max_headers")]
    pub max_headers: usize,

    /// The largest request head HTTP/1.1 lets in, start line included; a
    /// larger one is answered 431 and the connection is closed. It caps
    /// chunked trailers too. 16 KiB, the same as HTTP/2's
    /// `max_header_list_size` and HTTP/3's `max_field_section_size`.
    #[serde(default = "default_h1_max_header_size")]
    pub max_header_size: usize,

    /// Hyper's connection read and write buffer. Unset leaves hyper's own
    /// size. It used to stand in for the head limit, which is
    /// `max_header_size` now, so it no longer has to be small.
    #[serde(default)]
    pub max_buf_size: Option<usize>,

    #[serde(default = "default_true")]
    pub keep_alive: bool,

    #[serde(default = "default_true")]
    pub half_close: bool,

    #[serde(default)]
    pub pipeline_flush: bool,
}

impl Default for H1Config {
    fn default() -> Self {
        Self {
            header_read_timeout: HeaderReadTimeout::default(),
            max_headers: default_h1_max_headers(),
            max_header_size: default_h1_max_header_size(),
            max_buf_size: None,
            keep_alive: default_true(),
            half_close: default_true(),
            pipeline_flush: false,
        }
    }
}

#[derive(
    Debug, Deserialize, Clone, PartialEq, Eq, claviron_macros::Overridable,
)]
#[overridable(helpers = "crate::config::helpers")]
#[serde(deny_unknown_fields)]
pub struct H2Config {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_duration"
    )]
    pub keep_alive_interval: Option<Duration>,

    #[serde(
        default = "default_h2_keep_alive_timeout",
        deserialize_with = "super::helpers::required_duration"
    )]
    pub keep_alive_timeout: Duration,

    #[serde(default = "default_h2_concurrent_streams")]
    pub max_concurrent_streams: u32,

    #[serde(default = "default_h2_send_buf_size")]
    pub max_send_buf_size: usize,

    /// SETTINGS_MAX_FRAME_SIZE advertised to the peer. The protocol fixes the
    /// range at 16 KiB–16 MiB; larger frames trade latency for fewer frame
    /// headers on big responses.
    #[serde(default = "default_h2_max_frame_size")]
    pub max_frame_size: u32,

    /// Largest header block this server will accept. 16 KiB is deliberately
    /// tighter than most edges; raise it for deployments that carry large
    /// cookies or bearer tokens rather than letting them fail as 431.
    #[serde(default = "default_h2_max_header_list_size")]
    pub max_header_list_size: u32,

    /// Rapid Reset (CVE-2023-44487) cap: how many peer-reset streams may sit
    /// pending accept before the connection is treated as abusive.
    #[serde(default = "default_h2_max_pending_accept_reset_streams")]
    pub max_pending_accept_reset_streams: usize,

    /// How many streams this server may reset locally before it stops
    /// tracking them. The companion to the cap above, for errors we raise.
    #[serde(default = "default_h2_max_local_error_reset_streams")]
    pub max_local_error_reset_streams: usize,
}

impl Default for H2Config {
    fn default() -> Self {
        Self {
            keep_alive_interval: None,
            keep_alive_timeout: default_h2_keep_alive_timeout(),
            max_concurrent_streams: default_h2_concurrent_streams(),
            max_send_buf_size: default_h2_send_buf_size(),
            max_frame_size: default_h2_max_frame_size(),
            max_header_list_size: default_h2_max_header_list_size(),
            max_pending_accept_reset_streams:
                default_h2_max_pending_accept_reset_streams(),
            max_local_error_reset_streams:
                default_h2_max_local_error_reset_streams(),
        }
    }
}

fn default_tcp_keepalive() -> Option<Duration> {
    Some(Duration::from_secs(60))
}

fn default_h1_max_headers() -> usize {
    100
}

fn default_h1_max_header_size() -> usize {
    1024 * 16
}

fn default_h2_keep_alive_timeout() -> Duration {
    Duration::from_secs(20)
}

fn default_h2_concurrent_streams() -> u32 {
    200
}

fn default_h2_send_buf_size() -> usize {
    409_600
}

/// The four below carry forward what `make_h2_builder` hardcoded before they
/// became config, so an untouched deployment behaves identically.
fn default_h2_max_frame_size() -> u32 {
    1024 * 16
}

fn default_h2_max_header_list_size() -> u32 {
    1024 * 16
}

fn default_h2_max_pending_accept_reset_streams() -> usize {
    20
}

fn default_h2_max_local_error_reset_streams() -> usize {
    1024
}

fn default_true() -> bool {
    true
}

use super::ConfigError;

pub(crate) fn validate(
    h1: &H1Config,
    h2: &H2Config,
) -> Result<(), ConfigError> {
    if let HeaderReadTimeout::After(timeout) = h1.header_read_timeout {
        if timeout.is_zero() {
            return Err(ConfigError::global(
                "h1.header_read_timeout must be positive or null",
            ));
        }
        if std::time::Instant::now().checked_add(timeout).is_none() {
            return Err(ConfigError::global(
                "h1.header_read_timeout exceeds the platform's monotonic clock range",
            ));
        }
    }
    if h1.max_headers == 0 {
        return Err(ConfigError::global("h1.max_headers must be positive"));
    }
    if h1.max_header_size == 0 {
        return Err(ConfigError::global("h1.max_header_size must be positive"));
    }
    if let Some(max_buf_size) = h1.max_buf_size {
        // Hyper panics below its protocol/parser minimum; reject before publish.
        if max_buf_size < 8192 {
            return Err(ConfigError::global(
                "h1.max_buf_size must be at least 8192 bytes",
            ));
        }
        // A buffer smaller than the head limit would answer 431 first, and
        // `max_header_size` would not be the limit it says it is.
        if max_buf_size < h1.max_header_size {
            return Err(ConfigError::global(
                "h1.max_buf_size must not be smaller than h1.max_header_size",
            ));
        }
    }
    if h2.max_concurrent_streams == 0 || h2.max_send_buf_size == 0 {
        return Err(ConfigError::global(
            "h2 stream and send-buffer limits must be positive",
        ));
    }
    // Hyper's H2 builder asserts this bound before starting a connection.
    if h2.max_send_buf_size > u32::MAX as usize {
        return Err(ConfigError::global(
            "h2.max_send_buf_size must fit in u32",
        ));
    }
    if h2.keep_alive_interval.is_some_and(|interval| interval.is_zero()) {
        return Err(ConfigError::global(
            "h2.keep_alive_interval must be positive or null",
        ));
    }
    if h2.keep_alive_timeout.is_zero() {
        return Err(ConfigError::global(
            "h2.keep_alive_timeout must be positive",
        ));
    }
    // RFC 9113 §6.5.2 fixes SETTINGS_MAX_FRAME_SIZE; hyper forwards the value
    // to h2, which rejects the connection rather than the config.
    if !(16_384..=16_777_215).contains(&h2.max_frame_size) {
        return Err(ConfigError::global(
            "h2.max_frame_size must be between 16384 and 16777215 bytes",
        ));
    }
    if h2.max_header_list_size == 0 {
        return Err(ConfigError::global(
            "h2.max_header_list_size must be positive",
        ));
    }
    Ok(())
}
