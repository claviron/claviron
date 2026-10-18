//! Typed supervisor policies. Observation, recovery and lifecycle deadlines are independent.
use std::path::PathBuf;
use std::time::Duration;

use claviron_helpers::de::deserialize_duration;
use claviron_runtime::AppState;
use serde::{Deserialize, Serialize};

use crate::Error;

fn serialize_duration<S: serde::Serializer>(
    value: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&humantime::format_duration(*value))
}

fn serialize_optional_duration<S: serde::Serializer>(
    value: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => serialize_duration(value, serializer),
        None => serializer.serialize_none(),
    }
}

/// Complete typed supervisor policy, loaded and validated as one atomic snapshot.
/// Each group belongs to a distinct consumer; this data type runs no lifecycle
/// actions and keeps observation cadence separate from operation deadlines.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct SupervisorConfig {
    pub snapshot: WorkerSnapshotConfig,
    pub heartbeat: HeartbeatConfig,
    pub monitor: MonitorConfig,
    pub startup: StartupConfig,
    pub reload: ReloadConfig,
    pub recovery: RecoveryConfig,
    pub rotation: RotationConfig,
    pub shutdown: ShutdownConfig,
    pub metrics: MetricsConfig,
    /// Which signal asks for a reload, a log reopen and a worker restart:
    /// `hup`, `usr1` or `usr2`, each for one role. Unwritten, the binary's
    /// choice (HUP, USR1, USR2 unless it chose otherwise). Read when the
    /// process starts, before any provider boots, because the listeners are
    /// installed then so a stuck startup can still be interrupted — a change
    /// here takes effect on the next restart, not on a reload.
    pub signals: Option<claviron_sys::signal::ServerSignals>,
}

/// Worker pulse cadence and the master's accepted heartbeat age. Controls vital
/// liveness evidence independently of metrics freshness and boot confirmation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatConfig {
    /// Orbit heartbeat publication cadence.
    #[serde(
        default = "default_heartbeat_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub interval: Duration,
    /// Heartbeat timeout for hung worker detection through Orbit.
    #[serde(
        default = "default_heartbeat_timeout",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub timeout: Duration,
}
impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: default_heartbeat_interval(),
            timeout: default_heartbeat_timeout(),
        }
    }
}

/// Cadence for master-side health and rotation policy evaluation, not publication
/// frequency or a deadline for shutting down workers.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MonitorConfig {
    #[serde(
        default = "default_monitor_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub interval: Duration,
}
impl Default for MonitorConfig {
    fn default() -> Self {
        Self { interval: default_monitor_interval() }
    }
}

/// Initial fleet boot-acknowledgement deadline, separate from ongoing heartbeat
/// freshness and worker reload completion.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StartupConfig {
    /// Maximum wait for the initially spawned workers to acknowledge boot.
    /// Defaults to 30 seconds; independent of heartbeat and reload timeouts.
    #[serde(
        default = "default_startup_timeout",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub timeout: Duration,
}
impl Default for StartupConfig {
    fn default() -> Self {
        Self { timeout: default_startup_timeout() }
    }
}

/// Deadline for correlated worker reload acknowledgements. Expiry describes an
/// incomplete reload; it does not authorize process termination.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReloadConfig {
    /// Maximum time the master waits for worker reload completion events.
    #[serde(
        default = "default_reload_timeout",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub timeout: Duration,
}
impl Default for ReloadConfig {
    fn default() -> Self {
        Self { timeout: default_reload_timeout() }
    }
}

/// Failure-recovery budget, healthy-uptime reset window and retry backoff.
/// Applies to failed workers, not ordinary operator restart or policy rotation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// How long a worker must stay up before its restart history is forgiven.
    ///
    /// Without this the attempt count is a lifetime budget: a worker that
    /// crashes once a week is abandoned permanently after
    /// `restart_max_attempts` weeks, which is not what an operator setting a
    /// retry limit is asking for. They mean "stop retrying a worker that keeps
    /// failing", and "keeps" is a rate. systemd spells the same idea
    /// `StartLimitIntervalSec`.
    #[serde(
        default = "default_restart_window",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub window: Duration,
    #[serde(default)]
    pub backoff: BackoffConfig,
}
impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            window: default_restart_window(),
            backoff: BackoffConfig::default(),
        }
    }
}

/// Optional process RSS and runtime-age triggers for worker rotation. Uses
/// process facts, never exported metrics; absent triggers disable rotation.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RotationConfig {
    /// Worker Rotation Policies (all optional, rotation disabled if all None)
    /// Rotate worker when RSS exceeds this threshold. Example: "300 MB", "1.5 GB", "512 MiB"
    #[serde(default)]
    pub rss_threshold: Option<byte_unit::Byte>,
    /// Rotate worker after running for this duration
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub max_age: Option<Duration>,
}

/// Operator-selected worker-drain escalation policy. An absent deadline adds no
/// kill authority; whether a process joins the fleet exit barrier also depends
/// on its spawned/adopted origin, as decided by the shutdown use case.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ShutdownConfig {
    /// [GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION][graceful-ownership]
    /// policy for worker drains.
    ///
    /// `None` is an explicit unbounded policy, not a missing value: runtime
    /// code must wait indefinitely and must not synthesize a fallback.
    /// `Some(d)` authorizes SIGKILL escalation after `d`.
    ///
    /// [graceful-ownership]: https://github.com/claviron/claviron/blob/main/docs/design/GRACEFUL_OWNERSHIP.md
    #[serde(
        default,
        serialize_with = "serialize_optional_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub timeout: Option<Duration>,
}

/// Independent publication cadence and read-time freshness for optional process
/// telemetry. Neither setting influences heartbeat or lifecycle decisions.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(
        default = "default_metrics_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub publish_interval: Duration,
    #[serde(
        default = "default_metrics_max_age",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub max_age: Duration,
}
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            publish_interval: default_metrics_interval(),
            max_age: default_metrics_max_age(),
        }
    }
}

/// Bounded exponential delay between failure-recovery attempts. Delays only the
/// affected recovery operation; it is not a drain deadline or a monitor pause.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackoffConfig {
    #[serde(
        default = "default_restart_backoff",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub initial: Duration,
    #[serde(default = "default_restart_backoff_multiplier")]
    pub multiplier: u32,
    /// Ceiling on the exponential backoff between restarts.
    ///
    /// Without one the wait is `restart_backoff * multiplier^attempts`, which
    /// reaches ~17 minutes by the tenth attempt on the defaults and overflows
    /// outright on a larger multiplier — `u32::pow` and `Duration * u32` both
    /// panic, and the thing that panics is the master.
    #[serde(
        default = "default_restart_backoff_max",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub max: Duration,
}
impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: default_restart_backoff(),
            multiplier: default_restart_backoff_multiplier(),
            max: default_restart_backoff_max(),
        }
    }
}

/// Operator-facing cadence for the always-available worker status snapshot.
/// The master publishes this freshness policy with its read model so clients
/// need no independently loaded copy of supervisor configuration.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerSnapshotConfig {
    #[serde(
        default = "default_snapshot_publish_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub publish_interval: Duration,

    #[serde(
        default = "default_snapshot_max_age",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub max_age: Duration,
}

impl Default for WorkerSnapshotConfig {
    fn default() -> Self {
        Self {
            publish_interval: default_snapshot_publish_interval(),
            max_age: default_snapshot_max_age(),
        }
    }
}

pub(crate) fn config_path(state: &AppState) -> PathBuf {
    state.config_dir().join("supervisor.yaml")
}

/// Where the supervisor reads its policy from, when not from a file.
///
/// An embedder that already holds this policy — because it keeps its own
/// single configuration file, or builds the policy in code — registers a
/// source and binds it, the way `ShmActivitySource` is bound:
///
/// ```ignore
/// state.registry_ref().insert(Arc::new(MyPolicy::new(config)))?;
/// state.registry_ref().bind_dyn::<dyn SupervisorConfigSource, MyPolicy>(|source| source)?;
/// ```
///
/// `supervisor.yaml` is then never looked for. It is asked again on every
/// reload, so an embedder that can re-read its own file still reloads like
/// one that reads ours.
pub trait SupervisorConfigSource: Send + Sync {
    /// The policy as it stands now, or why it cannot be had.
    fn supervisor_config(
        &self
    ) -> std::result::Result<SupervisorConfig, String>;
}

/// The signals the operator configured, read before the listeners are
/// installed: from a registered [`SupervisorConfigSource`], otherwise from
/// `supervisor.yaml`. `None` when neither says — the binary's choice stands.
/// A file that is missing here is left to the supervisor's configure phase to
/// report; one that does not parse, or signals that do not validate, stop the
/// start now.
pub fn configured_signals(
    state: &AppState
) -> Result<Option<claviron_sys::signal::ServerSignals>, Error> {
    let cfg = match state
        .registry_ref()
        .resolve_dyn::<dyn SupervisorConfigSource>()
    {
        Some(source) => source.supervisor_config().map_err(Error::service)?,
        None => {
            let path = config_path(state);
            let yaml = match std::fs::read_to_string(&path) {
                Ok(yaml) => yaml,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(error) => {
                    return Err(Error::service(format!(
                        "read {}: {error}",
                        path.display()
                    )));
                }
            };
            serde_yaml::from_str::<SupervisorConfig>(&yaml).map_err(|e| {
                Error::service(format!("parse {}: {e}", path.display()))
            })?
        }
    };
    if let Some(signals) = &cfg.signals {
        signals
            .validate()
            .map_err(|error| Error::service(format!("signals: {error}")))?;
    }
    Ok(cfg.signals)
}

/// The same arithmetic runs on whatever the policy came from: a value that
/// would make a deadline meaningless is refused from a registered source
/// exactly as it is from a file.
pub(crate) fn validate(
    cfg: SupervisorConfig
) -> Result<SupervisorConfig, Error> {
    for (name, value) in [
        ("heartbeat.interval", cfg.heartbeat.interval),
        ("heartbeat.timeout", cfg.heartbeat.timeout),
        ("monitor.interval", cfg.monitor.interval),
        ("metrics.publish_interval", cfg.metrics.publish_interval),
        ("metrics.max_age", cfg.metrics.max_age),
    ] {
        if value.is_zero() {
            return Err(Error::service(format!(
                "{name} must be greater than zero"
            )));
        }
    }
    if cfg.snapshot.publish_interval.is_zero() {
        return Err(Error::service(
            "worker snapshot publish_interval must be greater than zero",
        ));
    }
    if cfg.snapshot.max_age.is_zero() {
        return Err(Error::service(
            "worker snapshot max_age must be greater than zero",
        ));
    }
    // These three feed `restart_backoff * multiplier^attempts`, which is
    // arithmetic on operator input. Rejecting the impossible values here is
    // what keeps that expression from being the master's last act.
    if cfg.recovery.backoff.initial.is_zero() {
        return Err(Error::service(
            "recovery.backoff.initial must be greater than zero",
        ));
    }
    if cfg.recovery.backoff.multiplier == 0 {
        return Err(Error::service(
            "recovery.backoff.multiplier must be at least 1",
        ));
    }
    if cfg.recovery.window.is_zero() {
        return Err(Error::service(
            "recovery.window must be greater than zero",
        ));
    }
    if cfg.recovery.backoff.max < cfg.recovery.backoff.initial {
        return Err(Error::service(
            "recovery.backoff.max must not be smaller than recovery.backoff.initial",
        ));
    }
    Ok(cfg)
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(1)
}
fn default_snapshot_publish_interval() -> Duration {
    Duration::from_secs(1)
}
fn default_snapshot_max_age() -> Duration {
    Duration::from_secs(5)
}
fn default_heartbeat_timeout() -> Duration {
    Duration::from_secs(10)
}
fn default_metrics_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_metrics_max_age() -> Duration {
    Duration::from_secs(30)
}

fn default_monitor_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_reload_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_startup_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_max_attempts() -> u32 {
    10
}

fn default_restart_backoff_multiplier() -> u32 {
    2
}

fn default_restart_backoff() -> Duration {
    Duration::from_secs(2)
}

/// Ten minutes: comfortably longer than a crash loop takes to exhaust its
/// attempts under the capped backoff, so a genuine loop still gives up, while a
/// worker that has been serving for ten minutes starts clean.
fn default_restart_window() -> Duration {
    Duration::from_secs(600)
}

/// A minute. Past this the backoff is no longer telling the operator anything
/// they do not already know from the attempt count, and every extra second is
/// capacity the fleet is not serving with.
fn default_restart_backoff_max() -> Duration {
    Duration::from_secs(60)
}

pub(crate) mod provider;
