//! Typed PHP runtime options and library-owned configuration loading.
//!
//! The builder accepts resolved values; validation checks that program paths
//! exist without reading their contents. The file adapter loads php.yaml and
//! preserves explicit env-file paths and inline values for execution.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

mod application;
pub use application::PhpApplication;
pub(crate) mod file;
mod pool;
pub use pool::RequestPool;
#[cfg(feature = "tasks")]
mod task;
#[cfg(feature = "tasks")]
pub(crate) use task::TaskProgram;

/// Bounded backlog for each PHP execution's application-event subscription.
///
/// Only subscribed topics addressed to that execution consume slots. This is
/// not a topic count, delivery quota or retained-history replay. Tokio rounds
/// storage up to the next power of two; overflow remains a receive error.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct PhpEventConfig {
    #[serde(default = "default_event_capacity")]
    pub capacity: usize,
}

fn default_event_capacity() -> usize {
    // Preserve the existing process bus default without changing other users.
    64
}

impl Default for PhpEventConfig {
    fn default() -> Self {
        Self { capacity: default_event_capacity() }
    }
}

impl PhpEventConfig {
    pub(crate) fn validate(self) -> Result<(), String> {
        if self.capacity == 0 || self.capacity > usize::MAX / 2 {
            return Err("PHP events.capacity must be positive and at most usize::MAX / 2".into());
        }
        Ok(())
    }
}

/// Fleet publication and freshness policy for named PHP resources.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct PhpManagementConfig {
    #[serde(deserialize_with = "claviron_helpers::de::deserialize_duration")]
    pub publish_interval: Duration,
    #[serde(deserialize_with = "claviron_helpers::de::deserialize_duration")]
    pub max_age: Duration,
}

impl Default for PhpManagementConfig {
    fn default() -> Self {
        Self {
            publish_interval: Duration::from_secs(1),
            max_age: Duration::from_secs(5),
        }
    }
}

impl PhpManagementConfig {
    pub(crate) fn validate(self) -> Result<(), String> {
        if self.publish_interval.is_zero() {
            return Err(
                "PHP management publish_interval must be greater than zero"
                    .into(),
            );
        }
        if self.max_age.is_zero() {
            return Err(
                "PHP management max_age must be greater than zero".into()
            );
        }
        Ok(())
    }
}

/// Policy for programs admitted by the invocation bridge: one boot per
/// invocation, on a thread of its own.
///
/// `execution_timeout` is per invocation and is the `--timeout` a CLI worker got
/// from `pcntl_alarm`: `None` falls back to `lifecycle.execution_timeout`, and
/// an invocation that outlives it is interrupted, not retried. `max_in_flight`
/// is a ceiling on memory -- every admitted invocation is a PHP context and a
/// framework boot -- rather than a throughput target.
/// With `tasks` enabled, `log` routes invocation stdout/stderr using the task
/// output policy. Relative file targets resolve against each invocation's cwd;
/// file targets append and the default is `inherit`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct PhpLaunchConfig {
    pub max_in_flight: usize,
    #[serde(
        deserialize_with = "claviron_helpers::de::deserialize_optional_duration"
    )]
    pub execution_timeout: Option<Duration>,
    #[serde(deserialize_with = "claviron_helpers::de::deserialize_duration")]
    pub drain_timeout: Duration,
    #[cfg(feature = "tasks")]
    pub log: claviron_tasks::LogConfig,
}

impl Default for PhpLaunchConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 8,
            execution_timeout: None,
            drain_timeout: Duration::from_secs(10),
            #[cfg(feature = "tasks")]
            log: claviron_tasks::LogConfig::default(),
        }
    }
}

impl PhpLaunchConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.max_in_flight == 0 {
            return Err(
                "PHP launch max_in_flight must be greater than zero".into()
            );
        }
        if self.execution_timeout.is_some_and(|budget| budget.is_zero()) {
            return Err("PHP launch execution_timeout must be greater than zero when set".into());
        }
        if self.drain_timeout.is_zero() {
            return Err(
                "PHP launch drain_timeout must be greater than zero".into()
            );
        }
        Ok(())
    }
}

fn default_instances() -> u8 {
    1
}

/// Preserve fleet invalidation unless the operator explicitly disables it.
pub(crate) fn default_cache_invalidation() -> bool {
    true
}

fn validate_program(
    kind: &str,
    name: &str,
    script_path: &Path,
    args: &[String],
    environment: &BTreeMap<String, String>,
) -> Result<(), String> {
    if name.trim().is_empty() || name.contains('\0') {
        return Err(format!(
            "{kind} name must be nonempty and contain no NUL bytes"
        ));
    }
    if args.len() >= i32::MAX as usize
        || args.iter().any(|arg| arg.contains('\0'))
    {
        return Err(format!(
            "{kind} {name:?} arguments must fit argc and contain no NUL bytes"
        ));
    }
    if script_path.as_os_str().is_empty()
        || script_path.as_os_str().as_encoded_bytes().contains(&0)
    {
        return Err(format!(
            "{kind} {name:?} script_path must be nonempty and contain no NUL bytes"
        ));
    }
    if !script_path.exists() {
        return Err(format!(
            "{kind} {name:?} script_path {} file not exist",
            script_path.to_string_lossy()
        ));
    }
    for (key, value) in environment {
        if key.is_empty() || key.contains(['\0', '=']) {
            return Err(format!(
                "{kind} {name:?} environment keys must be nonempty and contain neither NUL nor '='"
            ));
        }
        if value.contains('\0') {
            return Err(format!(
                "{kind} {name:?} environment values must contain no NUL bytes"
            ));
        }
    }
    Ok(())
}
