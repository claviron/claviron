//! Owns the four health policy groups, overrides and defaults.

use std::time::Duration;

use claviron_macros::Overridable;
use l7::L7Override;
// ---------- helpers ----------
use serde::Deserialize;

/// Top-level health configuration for an upstream profile.
///
/// The four sections correspond to four orthogonal mechanisms:
///
/// - `passive` — request-driven failure counting. No background tasks.
///   When threshold is crossed the origin is marked Down.
/// - `drop_probe` — one-shot reachability check fired when a connection
///   driver task ends with an error. Distinguishes "session lifecycle
///   event" from "origin is gone". Protocol-aware default: H2 = enabled,
///   H1 = disabled (multi-conn pools rarely need it).
/// - `recovery_probe` — background loop that starts when an origin
///   transitions to Down and keeps probing with exponential backoff until
///   the origin comes back. Was named `validator` previously.
/// - `active` — periodic probe that runs even while the origin is Healthy.
///   Catches origin death during idle periods when no request would
///   otherwise notice. Off by default; opt-in feature.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Overridable)]
#[serde(deny_unknown_fields)]
pub struct HealthConfig {
    #[serde(default)]
    #[overridable(nested)]
    pub passive: PassiveHealthConfig,
    #[serde(default)]
    #[overridable(nested)]
    pub drop_probe: DropProbeConfig,
    #[serde(default)]
    #[overridable(nested)]
    pub recovery_probe: RecoveryProbeConfig,
    #[serde(default)]
    #[overridable(nested)]
    pub active: ActiveHealthConfig,
    #[serde(default)]
    #[overridable(nested)]
    pub l7: l7::L7Config,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Overridable)]
#[serde(deny_unknown_fields)]
pub struct PassiveHealthConfig {
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
}

impl Default for PassiveHealthConfig {
    fn default() -> Self {
        Self { failure_threshold: default_failure_threshold() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Overridable)]
#[serde(deny_unknown_fields)]
pub struct DropProbeConfig {
    /// Whether to probe origin reachability after a driver task ends with
    /// an error. The built-in default is `false`; compile substitutes
    /// the protocol-aware default (`true` for H2, `false` for H1) when the
    /// operator has not explicitly set this field.
    #[serde(default = "default_drop_probe_enabled")]
    pub enabled: bool,
    #[serde(default = "default_drop_probe_timeout", with = "humantime_serde")]
    pub probe_timeout: Duration,
}

impl Default for DropProbeConfig {
    fn default() -> Self {
        Self {
            enabled: default_drop_probe_enabled(),
            probe_timeout: default_drop_probe_timeout(),
        }
    }
}

impl DropProbeConfig {
    pub fn h1_default() -> Self {
        Self { enabled: false, probe_timeout: default_drop_probe_timeout() }
    }

    pub fn h2_default() -> Self {
        Self { enabled: true, probe_timeout: default_drop_probe_timeout() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Overridable)]
#[serde(deny_unknown_fields)]
pub struct RecoveryProbeConfig {
    #[serde(default = "default_recovery_enabled")]
    pub enabled: bool,
    #[serde(
        default = "default_recovery_initial_backoff",
        with = "humantime_serde"
    )]
    pub initial_backoff: Duration,
    /// Ceiling on the exponential recovery delay. `null` removes the ceiling:
    /// the delay keeps doubling for as long as the origin stays Down.
    #[serde(
        default = "default_recovery_max_backoff",
        deserialize_with = "claviron_helpers::de::deserialize_optional_duration"
    )]
    pub max_backoff: Option<Duration>,
    #[serde(default = "default_recovery_jitter", with = "humantime_serde")]
    pub jitter: Duration,
    #[serde(
        default = "default_recovery_probe_timeout",
        with = "humantime_serde"
    )]
    pub probe_timeout: Duration,
    #[serde(default = "default_recovery_max_ttl", with = "humantime_serde")]
    pub max_ttl: Duration,
}

impl Default for RecoveryProbeConfig {
    fn default() -> Self {
        Self {
            enabled: default_recovery_enabled(),
            initial_backoff: default_recovery_initial_backoff(),
            max_backoff: default_recovery_max_backoff(),
            jitter: default_recovery_jitter(),
            probe_timeout: default_recovery_probe_timeout(),
            max_ttl: default_recovery_max_ttl(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Overridable)]
#[serde(deny_unknown_fields)]
pub struct ActiveHealthConfig {
    /// Off by default. Set to true to enable periodic background reachability
    /// probes regardless of whether the origin is currently Healthy or Down.
    /// Closes the "idle origin gap" — origin death during idle periods when
    /// no request would otherwise notice.
    #[serde(default = "default_active_enabled")]
    pub enabled: bool,
    #[serde(default = "default_active_interval", with = "humantime_serde")]
    pub interval: Duration,
    #[serde(
        default = "default_active_probe_timeout",
        with = "humantime_serde"
    )]
    pub probe_timeout: Duration,
}

impl Default for ActiveHealthConfig {
    fn default() -> Self {
        Self {
            enabled: default_active_enabled(),
            interval: default_active_interval(),
            probe_timeout: default_active_probe_timeout(),
        }
    }
}

fn default_failure_threshold() -> u32 {
    3
}

fn default_drop_probe_enabled() -> bool {
    // The protocol-aware default is applied at compile via
    // DropProbeConfig::h1_default / h2_default. A bare `DropProbeConfig::default()`
    // falls back to conservative `false`.
    false
}

fn default_drop_probe_timeout() -> Duration {
    Duration::from_millis(500)
}

fn default_recovery_enabled() -> bool {
    true
}

fn default_recovery_initial_backoff() -> Duration {
    Duration::from_secs(2)
}

fn default_recovery_max_backoff() -> Option<Duration> {
    Some(Duration::from_secs(60))
}

fn default_recovery_jitter() -> Duration {
    Duration::from_secs(1)
}

fn default_recovery_probe_timeout() -> Duration {
    Duration::from_millis(500)
}

fn default_recovery_max_ttl() -> Duration {
    Duration::from_secs(10 * 60)
}

fn default_active_enabled() -> bool {
    false
}

fn default_active_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_active_probe_timeout() -> Duration {
    Duration::from_millis(500)
}

pub(crate) mod l7;
