//! The system log: a server process's own diagnostics, written to a file by
//! the process itself.
//!
//! Every process of a family — master and each worker — opens the file on
//! its own with `O_APPEND` and writes each event with one `write`, so their
//! lines land whole at the end of one file without an fd handed from one to
//! another and without depending on who started the master or where its
//! stdout goes. Nothing here touches Orbit: the lines that matter most are
//! the ones written while something below the server has failed.
//!
//! Declared in `logging.yaml`, beside the pipeline's channels:
//!
//! ```yaml
//! system_log:
//!   path: ${SYSTEM_LOG_PATH}
//!   format: line
//!   truncate_on_startup: false # `truncate` is an alias.
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use claviron_runtime::RunMode;
use serde::{Deserialize, Serialize};

/// What `logging.yaml` says about the system log. The rest of the file is
/// the pipeline's and is not read here.
#[derive(Deserialize)]
struct LoggingFile {
    system_log: Option<SystemLogConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SystemLogConfig {
    /// A relative path is the working directory's.
    pub path: PathBuf,
    #[serde(default)]
    pub format: SystemLogFormat,
    /// Reset only on a new master or standalone launch, never in workers.
    #[serde(default, alias = "truncate")]
    pub truncate_on_startup: bool,
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum SystemLogFormat {
    /// The console's line — time, level, target, role, message — without
    /// colour.
    #[default]
    Line,
}

impl SystemLogConfig {
    pub fn validate(&self) -> Result<(), crate::Error> {
        if self.path.as_os_str().is_empty() {
            return Err(crate::Error::Config(
                "system_log.path is empty".into(),
            ));
        }
        Ok(())
    }

    /// Resolve boot-time variables using the same rules as `logging.yaml`.
    pub fn resolve(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, crate::Error> {
        let raw = serde_yaml::to_string(self)
            .map_err(|error| crate::Error::Config(error.to_string()))?;
        let raw = claviron_helpers::yaml::expand_env_with(&raw, lookup)
            .map_err(|error| crate::Error::Config(error.to_string()))?;
        serde_yaml::from_str(&raw)
            .map_err(|error| crate::Error::Config(error.to_string()))
    }
}

/// An opened system log.
pub struct SystemLog {
    open: Arc<OpenFile>,
}

/// The file every event is written to, and where it lives, so it can be
/// opened again after a rotation moved it.
struct OpenFile {
    path: PathBuf,
    file: RwLock<File>,
}

/// This process's system log, once the logger holds it: what a log reopen
/// reaches.
static OPEN: OnceLock<Arc<OpenFile>> = OnceLock::new();

/// Open the system log file again, at its configured path. Writers take the
/// new file from their next line on; a line is never split between the two.
pub(crate) fn reopen() {
    let Some(open) = OPEN.get() else { return };
    match append(&open.path) {
        Ok(file) => {
            *open.file.write().unwrap_or_else(|poison| poison.into_inner()) =
                file
        }
        Err(error) => {
            tracing::error!(path = %open.path.display(), %error, "system log could not be reopened")
        }
    }
}

fn append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// What `logging.yaml` in `config_dir` says about the system log; `None`
/// when there is no file or it declares none.
///
/// Read directly rather than through Congif: this runs before the runtime
/// and its reader exist, with the environment the process booted with as
/// `lookup`, because the system log must be open before anything that could
/// fail to report through it.
fn read(
    config_dir: &Path,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<SystemLogConfig>, crate::Error> {
    let path = config_dir.join("logging.yaml");
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => {
            return Err(crate::Error::Config(format!(
                "read {}: {error}",
                path.display()
            )));
        }
    };
    let text = claviron_helpers::yaml::expand_env_with(&raw, lookup).map_err(
        |error| crate::Error::Config(format!("{}: {error}", path.display())),
    )?;
    let file: LoggingFile = serde_yaml::from_str(&text).map_err(|error| {
        crate::Error::Config(format!("parse {}: {error}", path.display()))
    })?;
    let Some(config) = file.system_log else { return Ok(None) };
    config.validate()?;
    Ok(Some(config))
}

impl SystemLog {
    /// The system log `logging.yaml` in `config_dir` declares, opened for
    /// appending; `None` when there is no file or it declares none.
    pub fn from_config_dir(
        config_dir: &Path,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<Self>, crate::Error> {
        Self::open(config_dir, lookup, false)
    }

    /// Open at process startup. An opted-in reset belongs only to the master
    /// or standalone process; workers always append, including replacements.
    pub fn from_config_dir_for_startup(
        config_dir: &Path,
        lookup: impl Fn(&str) -> Option<String>,
        run_mode: RunMode,
    ) -> Result<Option<Self>, crate::Error> {
        Self::open(config_dir, lookup, !matches!(run_mode, RunMode::Worker(_)))
    }

    fn open(
        config_dir: &Path,
        lookup: impl Fn(&str) -> Option<String>,
        startup_reset_allowed: bool,
    ) -> Result<Option<Self>, crate::Error> {
        let Some(config) = read(config_dir, lookup)? else { return Ok(None) };
        Self::open_configuration(config, startup_reset_allowed).map(Some)
    }

    /// Open an explicitly configured system log without reading another file.
    pub fn from_configuration(
        config: SystemLogConfig,
        run_mode: RunMode,
    ) -> Result<Self, crate::Error> {
        Self::open_configuration(
            config,
            !matches!(run_mode, RunMode::Worker(_)),
        )
    }

    fn open_configuration(
        config: SystemLogConfig,
        startup_reset_allowed: bool,
    ) -> Result<Self, crate::Error> {
        config.validate()?;
        let SystemLogFormat::Line = config.format;
        if let Some(parent) =
            config.path.parent().filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                crate::Error::Config(format!(
                    "system log {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let file = append(&config.path)
            .and_then(|file| {
                if startup_reset_allowed && config.truncate_on_startup {
                    // Keep O_APPEND for every subsequent write from this process.
                    file.set_len(0)?;
                }
                Ok(file)
            })
            .map_err(|error| {
                crate::Error::Config(format!(
                    "system log {}: {error}",
                    config.path.display()
                ))
            })?;
        Ok(Self {
            open: Arc::new(OpenFile {
                path: config.path,
                file: RwLock::new(file),
            }),
        })
    }

    /// What [`Self::from_config_dir`] reads, checked and nothing opened: for
    /// a configuration check, which may run as another user than the
    /// service and must not create, or depend on writing, the service's
    /// files.
    pub fn check_config_dir(
        config_dir: &Path,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<(), crate::Error> {
        read(config_dir, lookup).map(|_| ())
    }

    pub(crate) fn writer(
        self
    ) -> impl Fn() -> AppendWriter + Send + Sync + 'static {
        let _ = OPEN.set(Arc::clone(&self.open));
        let open = self.open;
        move || AppendWriter(Arc::clone(&open))
    }
}

/// One event's writer. The formatter hands it the whole line at once.
pub(crate) struct AppendWriter(Arc<OpenFile>);

impl Write for AppendWriter {
    fn write(
        &mut self,
        buf: &[u8],
    ) -> io::Result<usize> {
        let file =
            self.0.file.read().unwrap_or_else(|poison| poison.into_inner());
        (&*file).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let file =
            self.0.file.read().unwrap_or_else(|poison| poison.into_inner());
        (&*file).flush()
    }
}

#[cfg(test)]
#[path = "../tests/system.rs"]
mod tests;
