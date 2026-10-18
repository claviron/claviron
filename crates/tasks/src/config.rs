use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::Duration;

use claviron_helpers::de::deserialize_duration;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};

use crate::executor::ScriptSpec;
use crate::process::CommandSpec;
use crate::registry::{TaskDispatch, TaskScope};

pub type TaskTable = HashMap<String, Arc<TaskConfig>>;

/// A section another build reads: accepted whatever it holds, and equal to
/// itself, so it never counts as a change of this configuration.
#[cfg(not(feature = "metrics"))]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Ignored;

#[cfg(not(feature = "metrics"))]
impl<'de> serde::Deserialize<'de> for Ignored {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(deserializer).map(|_| Self)
    }
}

/// Top-level shape of `etc/claviron/tasks.yaml`.
///
/// Service-level knobs live at the root (`graceful_timeout`, …); the
/// per-task definitions are nested under `tasks:` so the file has room
/// to grow without colliding with task names.
#[derive(Clone, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskServiceConfig {
    /// Cadence of the automatically installed cron scheduler task.
    #[serde(default)]
    pub scheduler: SchedulerConfig,
    /// Service-wide budget for letting in-flight task ticks finish during
    /// shutdown. `None` waits indefinitely. Distinct from `TaskConfig
    /// .graceful_timeout`, which bounds a single task's stop.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_duration"
    )]
    pub graceful_timeout: Option<Duration>,

    /// Fleet task-status publication and freshness policy.
    #[serde(default)]
    pub snapshot: TaskSnapshotConfig,

    /// Task metrics knobs, validated and used only with the `metrics` feature.
    #[cfg(feature = "metrics")]
    #[serde(default)]
    pub metrics: TaskMetricsConfig,
    #[cfg(not(feature = "metrics"))]
    #[serde(default, rename = "metrics")]
    _metrics: Option<Ignored>,

    /// Fleet-shared daemon pool coordination. One lease duration applies to
    /// every `dispatch: pool` task; controllers renew at one third of it.
    #[serde(default)]
    pub pool: TaskPoolConfig,

    /// Service-wide defaults for external `command:` tasks.
    #[serde(default)]
    pub command: TaskCommandConfig,

    #[serde(default)]
    pub tasks: TaskTable,
}

/// Service-wide defaults applied to every external `command:` task.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskCommandConfig {
    /// `PATH` handed to a spawned command that does not set its own.
    ///
    /// A spawned command gets a cleared environment, so this is the whole
    /// answer to "where are this deployment's executables" — operational
    /// policy, not an implementation detail, which is why it is here rather
    /// than a literal in the spawn path. A task's own `env: { PATH: ... }`
    /// still wins; this is what applies when it is absent.
    ///
    /// `$VAR` and `${VAR}` are expanded against the **supervisor's** own
    /// environment when the child is spawned, and a segment naming a variable
    /// that is not set is dropped rather than left half-resolved. The default
    /// carries `$HOME/.local/bin` unresolved on purpose: which home holds a
    /// deployment's tools is the operator's answer, not the library's. A unit
    /// that runs as one account while pointing `HOME` at the one that owns the
    /// toolchain is the ordinary case, and a home baked in at compile time
    /// would get it wrong in exactly that case.
    #[serde(default = "default_command_path")]
    pub path: String,
}

impl Default for TaskCommandConfig {
    fn default() -> Self {
        Self { path: default_command_path() }
    }
}

/// Executable search path for spawned commands, before expansion.
///
/// System locations plus the per-user `bin` directory that language
/// toolchains install into (deno, uv, pipx, `cargo install`). Homebrew's
/// prefix is on the macOS list for the same reason: it is where that
/// platform's tools actually land.
#[cfg(target_os = "macos")]
pub(crate) fn default_command_path() -> String {
    "$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin".to_owned()
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn default_command_path() -> String {
    "$HOME/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_owned()
}

/// Operator-facing cadence for the always-available management snapshot.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskSnapshotConfig {
    /// How often each fleet member publishes its complete local task view.
    #[serde(
        default = "default_snapshot_publish_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub publish_interval: Duration,

    /// Maximum accepted age of a member snapshot returned by `TaskManager`.
    #[serde(
        default = "default_snapshot_max_age",
        deserialize_with = "deserialize_duration"
    )]
    pub max_age: Duration,
}

impl Default for TaskSnapshotConfig {
    fn default() -> Self {
        Self {
            publish_interval: default_snapshot_publish_interval(),
            max_age: default_snapshot_max_age(),
        }
    }
}

fn default_snapshot_publish_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_snapshot_max_age() -> Duration {
    Duration::from_secs(5)
}

/// Operator-facing coordination policy for fleet-shared daemon pools.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskPoolConfig {
    /// Duration for which a pool slot remains owned without a renewal.
    /// Shorter values fail over faster but write more renewal frames.
    #[serde(
        default = "default_pool_lease_ttl",
        deserialize_with = "deserialize_duration"
    )]
    pub lease_ttl: Duration,
}

impl Default for TaskPoolConfig {
    fn default() -> Self {
        Self { lease_ttl: default_pool_lease_ttl() }
    }
}

fn default_pool_lease_ttl() -> Duration {
    Duration::from_secs(15)
}

/// Operator-facing knobs for the task metrics subsystem.
///
/// Compile-time gate (`feature = "metrics"`) decides whether the
/// instrumentation hooks fire at all; this struct is the runtime
/// surface for tuning *what* gets emitted when the feature is on.
///
/// The task metrics family and its publisher read this policy on every
/// collection tick, so reloads change cadence and freshness without restart.
#[cfg(feature = "metrics")]
#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskMetricsConfig {
    /// Master switch for the whole task metrics surface. Default
    /// `true` — when the `metrics` feature is on, metrics are on.
    /// Set `false` to keep the feature compiled in but silenced
    /// (useful for benchmarking or one-off troubleshooting where
    /// every counter increment looks like noise).
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Whether the dedicated external-process RSS sampler should
    /// run when at least one `command:` task exists. Defaults
    /// `true` on platforms supported by `claviron-sys::rss` for external
    /// process lookup; no-op elsewhere regardless of this flag.
    #[cfg_attr(
        not(any(target_os = "linux", target_os = "freebsd")),
        allow(dead_code)
    )]
    #[serde(default = "default_true")]
    pub rss_sampler: bool,

    /// How often the RSS sampler reads each alive child of a `command:` task.
    /// Smaller = more responsive leak detection, more syscalls. Default: 5s.
    #[serde(
        default = "default_rss_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub rss_sample_interval: Duration,

    /// Orbit publication cadence for task snapshots. Default: 1s.
    #[serde(
        default = "default_publish_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub publish_interval: Duration,

    /// Maximum Orbit sample age accepted by a worker scrape. Default: 5s.
    #[serde(
        default = "default_max_age",
        deserialize_with = "deserialize_duration"
    )]
    pub max_age: Duration,
}

#[cfg(feature = "metrics")]
impl Default for TaskMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rss_sampler: true,
            rss_sample_interval: default_rss_interval(),
            publish_interval: default_publish_interval(),
            max_age: default_max_age(),
        }
    }
}

#[cfg(feature = "metrics")]
fn default_rss_interval() -> Duration {
    Duration::from_secs(5)
}

#[cfg(feature = "metrics")]
fn default_publish_interval() -> Duration {
    Duration::from_secs(1)
}

#[cfg(feature = "metrics")]
fn default_max_age() -> Duration {
    Duration::from_secs(5)
}

#[derive(Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SchedulerConfig {
    #[serde(
        default = "default_scheduler_poll_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub poll_interval: Duration,
}

/// Scheduled overlap is explicit; missed occurrences never form a backlog.
#[derive(Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskOverlap {
    #[default]
    Skip,
    Allow,
}

impl TaskOverlap {
    pub(crate) fn reserve(
        self,
        limit: &Arc<tokio::sync::Semaphore>,
    ) -> Result<
        Option<tokio::sync::OwnedSemaphorePermit>,
        tokio::sync::TryAcquireError,
    > {
        match self {
            Self::Skip => Arc::clone(limit).try_acquire_owned().map(Some),
            Self::Allow => Ok(None),
        }
    }
}

fn default_scheduler_poll_interval() -> Duration {
    Duration::from_secs(60)
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self { poll_interval: default_scheduler_poll_interval() }
    }
}

#[derive(Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskConfig {
    /// Materialized from the service-level scheduler policy, never task YAML.
    #[serde(skip)]
    pub(crate) scheduler: Option<SchedulerConfig>,
    /// Run this task by spawning a child process. Mutually exclusive with
    /// `php` — see [`TaskConfig::executor`].
    #[serde(default)]
    pub command: Option<CommandSpec>,

    /// Run this task on the embedded PHP engine. Mutually exclusive with the
    /// other two — see [`TaskConfig::executor`].
    #[serde(default)]
    pub php: Option<ScriptSpec>,

    /// Run this task on the embedded JavaScript engine. Same block shape as
    /// `php:`; the key is what names the engine.
    #[serde(default)]
    pub js: Option<ScriptSpec>,

    /// Run this task as a Lua script in a fresh sandboxed state. Same block
    /// shape as `php:`; the key is what names the engine.
    #[serde(default)]
    pub lua: Option<ScriptSpec>,

    /// Eligible runtime roles.
    #[serde(default)]
    pub scope: Option<TaskScope>,

    /// Execution distribution. Rust task annotations provide the default;
    /// command tasks default to `each`.
    #[serde(default)]
    pub dispatch: Option<TaskDispatch>,

    /// Trigger and execution-budget policy. Omission selects manual timing,
    /// leaving the task available for explicit `run-task` invocations without
    /// starting a scheduler.
    #[serde(default)]
    pub timing: TaskTiming,

    #[serde(default = "default_one")]
    pub concurrency: u8,

    /// Maximum total attempts for one execution. Accepts a positive integer
    /// or the explicit `unlimited` keyword; zero is invalid. Omission means
    /// one attempt for every timing mode. Unlimited retry is never implicit.
    ///
    /// For pool dispatch, one execution is the current slot ownership
    /// session. Exhaustion releases the slot for re-contest, where a new
    /// ownership session starts again at attempt 1.
    #[serde(default)]
    pub attempts: AttemptLimit,

    #[serde(
        default = "default_one_second",
        deserialize_with = "deserialize_duration"
    )]
    pub backoff: Duration,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_duration"
    )]
    pub graceful_timeout: Option<Duration>,

    /// Suppress per-attempt info logs when nothing is wrong.
    ///
    /// When `false` (default), keep the existing `🚀 Task '...' #N attempt M`
    /// and `Task '...' exits` chatter — useful for low-frequency admin tasks
    /// where each run is a noteworthy event.
    ///
    /// When `true`, silence those start/exit lines for chatty / sub-second tasks
    /// (heartbeat, watcher loops, metrics scrapers). Errors, retries, and
    /// skip-still-running messages are still emitted at their normal levels.
    #[serde(default)]
    pub silent: bool,
}

impl TaskConfig {
    /// A task's own configuration, written in the same words as a
    /// `tasks.yaml` entry. This is how a builtin task carries its defaults in
    /// code: the entry it would otherwise need in the file.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        serde_yaml::from_str(yaml)
            .map_err(|error| format!("task config: {error}"))
    }

    /// The one place this task's executor is decided.
    ///
    /// Both blocks written at once is refused when the configuration is loaded,
    /// so that case cannot reach a running task; it resolves here to `Command`
    /// only so that the accessor stays total.
    pub fn executor(&self) -> TaskExecutor<'_> {
        match (&self.command, &self.php, &self.js, &self.lua) {
            (Some(command), ..) => TaskExecutor::Command(command),
            (None, Some(php), ..) => TaskExecutor::Php(php),
            (None, None, Some(js), _) => TaskExecutor::Js(js),
            (None, None, None, Some(lua)) => TaskExecutor::Lua(lua),
            (None, None, None, None) => TaskExecutor::Builtin,
        }
    }

    pub fn executor_kind(&self) -> TaskExecutorKind {
        self.executor().kind()
    }

    /// Whether both executor blocks are written, which no task may do.
    /// Whether more than one executor block is written, which no task may do.
    pub const fn has_conflicting_executors(&self) -> bool {
        let declared = self.command.is_some() as u8
            + self.php.is_some() as u8
            + self.js.is_some() as u8
            + self.lua.is_some() as u8;
        declared > 1
    }
}

/// Which executor runs this task's body.
///
/// `builtin` names no block because its body is not declared anywhere: it is
/// compiled into this binary and registered by `#[task]`, and configuration can
/// only refer to it by name. The other two declare theirs, and exactly one may
/// be written — a task carrying both is refused at load rather than resolved by
/// precedence.
///
/// They do not name languages, even where a name happens to be one. They name
/// where a task's body comes from and what runs it: the binary itself, a
/// program spawned as a child, or a script handed to an engine this process
/// already carries. `php:` and `js:` are the same block shape for that reason —
/// the key is the engine, not the syntax.
///
/// This type exists so that the question is asked in one place. It used to be
/// asked as `command.is_some()` at eight sites, each supplying its own `else`,
/// and that `else` is `SEMANTICS.md` S14: a task with no block and no
/// registered runner was not rejected, it was silently taken to be a built-in
/// task that does not exist. `Builtin` is now a variant one has to handle
/// rather than a fallthrough one gets by default.
pub enum TaskExecutor<'a> {
    Builtin,
    Command(&'a CommandSpec),
    Php(&'a ScriptSpec),
    Js(&'a ScriptSpec),
    Lua(&'a ScriptSpec),
}

/// [`TaskExecutor`] without the borrow, for comparing two configurations and
/// for keying the installed engines.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TaskExecutorKind {
    Builtin,
    Command,
    Php,
    Js,
    Lua,
}

impl TaskExecutor<'_> {
    pub const fn kind(&self) -> TaskExecutorKind {
        match self {
            Self::Builtin => TaskExecutorKind::Builtin,
            Self::Command(_) => TaskExecutorKind::Command,
            Self::Php(_) => TaskExecutorKind::Php,
            Self::Js(_) => TaskExecutorKind::Js,
            Self::Lua(_) => TaskExecutorKind::Lua,
        }
    }
}

impl TaskExecutorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Command => "command",
            Self::Php => "php",
            Self::Js => "js",
            Self::Lua => "lua",
        }
    }

    /// Where a task of this kind may run when the configuration does not say.
    ///
    /// `None` means ask the registered runner, which only a Rust task has. A
    /// command or hosted-language task defaults to workers, never implicitly
    /// to the master. Master or all-role execution must be requested explicitly.
    pub const fn default_scope(self) -> Option<TaskScope> {
        match self {
            Self::Builtin => None,
            // The engines live on serving processes, not on the master.
            Self::Command | Self::Php | Self::Js | Self::Lua => {
                Some(TaskScope::Worker)
            }
        }
    }

    pub const fn default_dispatch(self) -> Option<TaskDispatch> {
        match self {
            Self::Builtin => None,
            Self::Command | Self::Php | Self::Js | Self::Lua => {
                Some(TaskDispatch::Each)
            }
        }
    }

    /// Whether the executor drains itself once cancellation is observed.
    ///
    /// A Rust body is a future the runner owns and may bound. The other two are
    /// not: a child process handles the signal it was sent, and a PHP program
    /// answers `claviron_is_cancelled()` on its own loop, with the slot drain
    /// owning the grace. Imposing a second deadline on either would end work the
    /// executor was still finishing.
    pub const fn owns_shutdown(self) -> bool {
        !matches!(self, Self::Builtin)
    }
}

/// How a task body is triggered and how long one bounded occurrence may run.
///
/// YAML uses an explicit tagged object when an automatic trigger is wanted:
///
/// ```yaml
/// # omitted: manual runs only, with the default execution timeout
/// timing: { kind: daemon, enabled: true }
/// timing: { kind: interval, enabled: true, every: 30s, execution_timeout: 20s }
/// timing: { kind: cron, enabled: true, expression: "0 0 * * * *", execution_timeout: 2m }
/// timing: { kind: manual, execution_timeout: 30s }
/// ```
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum TaskTiming {
    Manual {
        /// Whole-run budget, including retries and backoff.
        #[serde(
            default = "default_execution_timeout",
            alias = "timeout",
            deserialize_with = "deserialize_duration"
        )]
        execution_timeout: Duration,
    },
    Daemon {
        /// Whether the lifecycle-managed daemon trigger is active.
        #[serde(default = "default_true")]
        enabled: bool,
    },
    Interval {
        /// Whether a new occurrence may start while an earlier one runs.
        #[serde(default)]
        overlap: TaskOverlap,
        /// Whether the interval scheduler is active.
        #[serde(default = "default_true")]
        enabled: bool,
        #[serde(deserialize_with = "deserialize_duration")]
        every: Duration,
        /// Skip the initial run at service startup; normal scheduling resumes
        /// after one full interval.
        #[serde(default = "default_true")]
        skip_on_boot: bool,
        /// Whole-run budget, including retries and backoff.
        #[serde(
            default = "default_execution_timeout",
            alias = "timeout",
            deserialize_with = "deserialize_duration"
        )]
        execution_timeout: Duration,
    },
    Cron {
        /// Whether a new occurrence may start while an earlier one runs.
        #[serde(default)]
        overlap: TaskOverlap,
        /// Whether the cron scheduler is active.
        #[serde(default = "default_true")]
        enabled: bool,
        expression: String,
        /// Whole-run budget, including retries and backoff.
        #[serde(
            default = "default_execution_timeout",
            alias = "timeout",
            deserialize_with = "deserialize_duration"
        )]
        execution_timeout: Duration,
    },
}

impl Default for TaskTiming {
    fn default() -> Self {
        Self::Manual { execution_timeout: default_execution_timeout() }
    }
}

impl TaskTiming {
    pub(crate) fn overlap(&self) -> Option<TaskOverlap> {
        match self {
            Self::Cron { overlap, .. } | Self::Interval { overlap, .. } => {
                Some(*overlap)
            }
            Self::Manual { .. } | Self::Daemon { .. } => None,
        }
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Manual { .. } => "manual",
            Self::Daemon { .. } => "daemon",
            Self::Interval { .. } => "interval",
            Self::Cron { .. } => "cron",
        }
    }

    pub(crate) fn is_daemon(&self) -> bool {
        matches!(self, Self::Daemon { .. })
    }

    pub(crate) fn is_scheduled(&self) -> bool {
        matches!(self, Self::Interval { .. } | Self::Cron { .. })
    }

    /// Current desired state of an automatic trigger. Manual tasks have no
    /// trigger and therefore cannot be enabled or disabled.
    pub(crate) const fn trigger_enabled(&self) -> Option<bool> {
        match self {
            Self::Manual { .. } => None,
            Self::Daemon { enabled }
            | Self::Interval { enabled, .. }
            | Self::Cron { enabled, .. } => Some(*enabled),
        }
    }

    /// Mutate the current desired state of an automatic trigger.
    ///
    /// Returns `false` for manual timing, which deliberately has no managed
    /// trigger state.
    pub(crate) fn set_trigger_enabled(
        &mut self,
        value: bool,
    ) -> bool {
        match self {
            Self::Manual { .. } => false,
            Self::Daemon { enabled }
            | Self::Interval { enabled, .. }
            | Self::Cron { enabled, .. } => {
                *enabled = value;
                true
            }
        }
    }

    /// Whole-run budget for bounded timing modes. Daemons are lifecycle-bound
    /// and stop only through disable, reload, or runtime shutdown.
    pub(crate) const fn execution_timeout(&self) -> Option<Duration> {
        match self {
            Self::Manual { execution_timeout }
            | Self::Interval { execution_timeout, .. }
            | Self::Cron { execution_timeout, .. } => Some(*execution_timeout),
            Self::Daemon { .. } => None,
        }
    }
}

/// Explicit attempt budget. Zero is deliberately not a sentinel: operators
/// write `unlimited`, while finite budgets are non-zero by construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptLimit {
    Limited(NonZeroU8),
    Unlimited,
}

impl Default for AttemptLimit {
    fn default() -> Self {
        Self::Limited(NonZeroU8::MIN)
    }
}

impl AttemptLimit {
    pub(crate) fn exhausted_by(
        self,
        attempt: u32,
    ) -> bool {
        match self {
            Self::Limited(limit) => attempt >= u32::from(limit.get()),
            Self::Unlimited => false,
        }
    }
}

impl<'de> Deserialize<'de> for AttemptLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AttemptLimitVisitor;

        impl Visitor<'_> for AttemptLimitVisitor {
            type Value = AttemptLimit;

            fn expecting(
                &self,
                formatter: &mut fmt::Formatter<'_>,
            ) -> fmt::Result {
                formatter.write_str("a positive integer or 'unlimited'")
            }

            fn visit_i64<E>(
                self,
                value: i64,
            ) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value = u64::try_from(value).map_err(|_| {
                    E::custom("attempts must be positive or 'unlimited'")
                })?;
                self.visit_u64(value)
            }

            fn visit_u64<E>(
                self,
                value: u64,
            ) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value = u8::try_from(value).map_err(|_| {
                    E::custom("attempts must fit in an unsigned 8-bit integer")
                })?;
                NonZeroU8::new(value).map(AttemptLimit::Limited).ok_or_else(
                    || E::custom("attempts must be positive or 'unlimited'"),
                )
            }

            fn visit_str<E>(
                self,
                value: &str,
            ) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "unlimited" {
                    Ok(AttemptLimit::Unlimited)
                } else {
                    Err(E::custom(
                        "attempts must be a positive integer or 'unlimited'",
                    ))
                }
            }
        }

        deserializer.deserialize_any(AttemptLimitVisitor)
    }
}

fn default_one_second() -> Duration {
    Duration::from_secs(1)
}

fn default_execution_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_one() -> u8 {
    1
}

#[inline]
fn default_true() -> bool {
    true
}
