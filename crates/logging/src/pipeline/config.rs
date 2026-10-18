//! `logging.yaml`: the channels an operator declared.
//!
//! Congif prepares the file before boot and in the reload batch; this module
//! is its shape and its rules, and reads nothing itself.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The whole file. A channel exists only if it is written here; an absent
/// file is the empty document — no channels — until one is declared.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// The system log's block, read by [`crate::SystemLog`] before the
    /// logger exists; not the pipeline's.
    #[serde(default, rename = "system_log")]
    pub _system_log: Option<Ignored>,
    /// The channel HTTP access records go to. Unwritten, none are made.
    #[serde(default)]
    pub access_log: Option<String>,
    pub channels: BTreeMap<String, ChannelConfig>,
}

/// A section another reader owns: accepted whatever it holds, and equal to
/// itself, so its content never counts as a change of this configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Ignored;

impl<'de> Deserialize<'de> for Ignored {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(deserializer).map(|_| Self)
    }
}

/// Where one channel's records go.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "adapter", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChannelConfig {
    /// Appended to one file. A relative path is the working directory's.
    File {
        path: PathBuf,
        format: FileFormat,
        /// Clear on the writer's first startup, not on reload or reopen.
        #[serde(default, alias = "truncate")]
        truncate_on_startup: bool,
    },
    /// Sent as OpenTelemetry logs over OTLP/HTTP (the `otlp` feature).
    Otlp {
        /// The logs URL itself, used as written — a collector's is usually
        /// `http://127.0.0.1:4318/v1/logs`. `http` only in this build.
        endpoint: String,
        /// Records held for sending; when the endpoint falls behind and this
        /// is full, further records are dropped and counted, and nothing else
        /// waits. The operator's number: no default.
        queue: usize,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// One JSON object per line.
    Json,
    /// One human-readable line per record.
    Line,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{path}: more channels than a record can name ({count})")]
    TooManyChannels { path: PathBuf, count: usize },
    #[error(
        "{path}: access_log names '{channel}', which is not a declared channel"
    )]
    UnknownAccessChannel { path: PathBuf, channel: String },
}

impl LoggingConfig {
    /// What the file must satisfy beyond its shape: `access_log` names a
    /// channel it declares. The id budget is the table's to check, since ids
    /// outlive the channels that first took them.
    pub fn validate(
        &self,
        path: &Path,
    ) -> Result<(), ConfigError> {
        if let Some(channel) = &self.access_log
            && !self.channels.contains_key(channel)
        {
            return Err(ConfigError::UnknownAccessChannel {
                path: path.to_owned(),
                channel: channel.clone(),
            });
        }
        Ok(())
    }
}
