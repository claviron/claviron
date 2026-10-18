use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::output::LogConfig;

#[derive(Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommandSpec {
    pub(crate) program: String,

    #[serde(default)]
    pub(crate) args: Vec<String>,

    #[serde(default)]
    pub(crate) env: HashMap<String, String>,

    /// Environment source declaration, retained without reading the file.
    /// The execution owner resolves it with inline `env:` values taking
    /// precedence; the task config snapshot never stores the merged result.
    pub(crate) env_file: Option<PathBuf>,

    #[serde(default, alias = "dir")]
    pub(crate) cwd: Option<String>,

    /// Which natural process completions enter the task runner's retry loop.
    #[serde(default)]
    pub(crate) restart: RestartPolicy,

    /// What happens to a long-running command when Claviron exits.
    #[serde(default)]
    pub(crate) exit_action: ExitAction,

    /// What happens to an unchanged long-running command on task reload.
    #[serde(default)]
    pub(crate) reload_action: Option<ReloadAction>,

    #[serde(default)]
    pub(crate) log: LogConfig,

    /// The service-level `command.path`, stamped in when `tasks.yaml` is
    /// loaded so the spawn path has one place to read it from. Not an
    /// operator field of its own: `command:` blocks cannot set it, and
    /// `env: { PATH: ... }` is how a single task overrides the result.
    /// It takes part in equality deliberately — changing the service path
    /// changes what a command would run, so reload must see it as a change.
    #[serde(skip, default = "crate::config::default_command_path")]
    pub(crate) default_path: String,
}

/// Retry classification for an external command's natural completion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RestartPolicy {
    Never,
    #[default]
    OnFailure,
    Always,
}

/// Command-process behavior when Claviron exits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ExitAction {
    /// SIGTERM, wait for `graceful_timeout`, then SIGKILL and reap if needed.
    #[default]
    Terminate,
    /// Send one signal and relinquish supervision without waiting for exit.
    Signal(ProcessSignal),
    /// Leave the process running and relinquish supervision.
    KeepRunning,
}

/// Explicit command-process behavior when TaskService reloads.
///
/// Absence means that reload does not touch an unchanged command. Structural
/// task-plan changes are handled independently by TaskService.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReloadAction {
    /// Stop and reap the current process, then run the candidate task plan.
    Restart,
    /// Send one signal to the current process. Except for KILL, this keeps an
    /// unchanged daemon's supervision and pool ownership without a stop transition.
    Signal(ProcessSignal),
}

/// Portable, intentionally bounded signals accepted by process actions — the
/// same set, and the same words, as every other signal an operator chooses.
pub(crate) use claviron_sys::signal::Signal as ProcessSignal;

impl ExitAction {
    pub(crate) const fn is_terminating(self) -> bool {
        matches!(self, Self::Terminate | Self::Signal(ProcessSignal::Kill))
    }
}

impl ReloadAction {
    pub(crate) const fn is_terminating(self) -> bool {
        self.in_place_signal().is_none()
    }

    /// Signals delivered without requesting a supervisor transition. This is
    /// not a claim about the signal's effect: the child may still exit.
    pub(crate) const fn in_place_signal(self) -> Option<ProcessSignal> {
        match self {
            Self::Restart | Self::Signal(ProcessSignal::Kill) => None,
            Self::Signal(signal) => Some(signal),
        }
    }
}

fn deserialize_signal_map<'de, A>(mut map: A) -> Result<ProcessSignal, A::Error>
where
    A: MapAccess<'de>,
{
    let Some(key) = map.next_key::<String>()? else {
        return Err(de::Error::custom(
            "process action signal map cannot be empty",
        ));
    };
    if key != "signal" {
        return Err(de::Error::unknown_field(&key, &["signal"]));
    }
    let signal = map.next_value::<ProcessSignal>()?;
    if let Some(extra) = map.next_key::<String>()? {
        return Err(de::Error::unknown_field(&extra, &["signal"]));
    }
    Ok(signal)
}

impl<'de> Deserialize<'de> for ExitAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ExitActionVisitor;

        impl<'de> Visitor<'de> for ExitActionVisitor {
            type Value = ExitAction;

            fn expecting(
                &self,
                formatter: &mut fmt::Formatter<'_>,
            ) -> fmt::Result {
                formatter.write_str(
                    "'terminate', 'keep-running', or a map containing signal",
                )
            }

            fn visit_str<E>(
                self,
                value: &str,
            ) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "terminate" => Ok(ExitAction::Terminate),
                    "keep-running" => Ok(ExitAction::KeepRunning),
                    other => Err(E::unknown_variant(
                        other,
                        &["terminate", "keep-running"],
                    )),
                }
            }

            fn visit_map<A>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                deserialize_signal_map(map).map(ExitAction::Signal)
            }
        }

        deserializer.deserialize_any(ExitActionVisitor)
    }
}

impl<'de> Deserialize<'de> for ReloadAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ReloadActionVisitor;

        impl<'de> Visitor<'de> for ReloadActionVisitor {
            type Value = ReloadAction;

            fn expecting(
                &self,
                formatter: &mut fmt::Formatter<'_>,
            ) -> fmt::Result {
                formatter.write_str("'restart' or a map containing signal")
            }

            fn visit_str<E>(
                self,
                value: &str,
            ) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "restart" => Ok(ReloadAction::Restart),
                    other => Err(E::unknown_variant(other, &["restart"])),
                }
            }

            fn visit_map<A>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                deserialize_signal_map(map).map(ReloadAction::Signal)
            }
        }

        deserializer.deserialize_any(ReloadActionVisitor)
    }
}
