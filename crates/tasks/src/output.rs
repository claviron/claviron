//! Executor-independent task output policy.
//!
//! `command:` applies this policy to child-process `Stdio`; embedded engines
//! apply it to their own task-local console bridge. The YAML vocabulary is the
//! same because the operator is answering the same question in both cases:
//! where this task's stdout and stderr belong.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use tracing::{error, warn};

/// How one task's stdout/stderr should be routed.
///
/// String form applies one target to both streams:
/// `log: worker.log`, `log: inherit`, `log: console` or `log: null`.
/// `inherit` (also written `journal`, and the default) is the server's own
/// log — the system log file, journald or the console, wherever the server
/// writes — one line per event, stamped with the task. `console` is the
/// process's raw stdout/stderr, passed through untouched: a command task's
/// child writes to the server's descriptors directly. A path appends to that
/// file. `log: { channel: queue }` sends each line as a record to a channel
/// `logging.yaml` declares.
/// Map form addresses them independently:
/// `log: { stdout: worker.log, stderr: worker-error.log }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogConfig {
    Single(LogTarget),
    Split { stdout: LogTarget, stderr: LogTarget },
}

impl Default for LogConfig {
    fn default() -> Self {
        Self::Single(LogTarget::Inherit)
    }
}

impl<'de> Deserialize<'de> for LogConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LogConfigVisitor;

        impl<'de> Visitor<'de> for LogConfigVisitor {
            type Value = LogConfig;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                formatter.write_str(
                    "a log target string or a map with stdout/stderr",
                )
            }

            fn visit_str<E>(
                self,
                value: &str,
            ) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(LogConfig::Single(parse_log_target(value)))
            }

            fn visit_string<E>(
                self,
                value: String,
            ) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }

            fn visit_map<A>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut stdout: Option<LogTarget> = None;
                let mut stderr: Option<LogTarget> = None;
                let mut channel: Option<String> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "stdout" | "stdout_log" => {
                            stdout = Some(parse_optional_log_target(
                                map.next_value()?,
                            ));
                        }
                        "stderr" | "stderr_log" => {
                            stderr = Some(parse_optional_log_target(
                                map.next_value()?,
                            ));
                        }
                        "channel" => channel = Some(map.next_value()?),
                        other => {
                            return Err(de::Error::unknown_field(
                                other,
                                &["stdout", "stderr", "channel"],
                            ));
                        }
                    }
                }

                if let Some(channel) = channel {
                    if stdout.is_some() || stderr.is_some() {
                        return Err(de::Error::custom(
                            "a task log names a channel or its streams, not both",
                        ));
                    }
                    return Ok(LogConfig::Single(LogTarget::Channel(channel)));
                }

                Ok(LogConfig::Split {
                    stdout: stdout.unwrap_or(LogTarget::Inherit),
                    stderr: stderr.unwrap_or(LogTarget::Inherit),
                })
            }
        }

        deserializer.deserialize_any(LogConfigVisitor)
    }
}

/// One resolved stream destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogTarget {
    Path(PathBuf),
    Null,
    /// The server's own log.
    Inherit,
    /// The process's raw stdout/stderr.
    Console,
    /// A declared log channel.
    Channel(String),
}

fn parse_optional_log_target(value: Option<String>) -> LogTarget {
    match value {
        None => LogTarget::Inherit,
        Some(value) if value.is_empty() => LogTarget::Inherit,
        Some(value) => parse_log_target(&value),
    }
}

fn parse_log_target(value: &str) -> LogTarget {
    match value.to_ascii_lowercase().as_str() {
        "/dev/null" | "none" | "null" => LogTarget::Null,
        "inherit" | "journal" => LogTarget::Inherit,
        "console" => LogTarget::Console,
        _ => LogTarget::Path(PathBuf::from(value)),
    }
}

pub(crate) fn resolve_path(
    path: &Path,
    cwd: Option<&Path>,
) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    if let Some(cwd) = cwd {
        return cwd.join(path);
    }
    std::env::current_dir()
        .map(|current| current.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

/// Open task-local writers for an embedded executor.
///
/// Unlike process `Stdio`, these handles can safely coexist in one process:
/// every task execution owns its routing object and same-path split streams
/// share one locked append handle.
#[derive(Clone, Debug)]
pub struct TaskOutput {
    stdout: OutputSink,
    stderr: OutputSink,
}

#[derive(Clone, Debug)]
enum OutputSink {
    Null,
    Console(Stream),
    Lines(Arc<LineWriter>),
    File(Arc<Mutex<File>>),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    #[cfg(feature = "log-pipeline")]
    fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

/// Where one line of a task's output goes when it goes line by line.
#[derive(Debug)]
pub(crate) enum LineSink {
    /// An event in the server's own log.
    SystemLog { task: Arc<str>, stream: Stream },
    /// A record in a declared channel.
    #[cfg(feature = "log-pipeline")]
    Channel {
        channels: Arc<claviron_logging::pipeline::LogChannels>,
        channel: claviron_logging::pipeline::ChannelId,
        task: Arc<str>,
        stream: Stream,
    },
}

impl LineSink {
    /// The sink a target names. A channel that is not declared, or a build
    /// without the pipeline, is said and goes to the server's log instead.
    pub(crate) fn for_target(
        target: &LogTarget,
        task: &str,
        stream: Stream,
    ) -> Self {
        let task: Arc<str> = Arc::from(task);
        if let LogTarget::Channel(name) = target {
            #[cfg(feature = "log-pipeline")]
            if let Some(channels) = claviron_logging::pipeline::installed()
                && let Some(channel) = channels.channel(name)
            {
                return Self::Channel { channels, channel, task, stream };
            }
            error!(
                task = %task,
                channel = %name,
                "task log channel is not declared; writing to the server's log"
            );
        }
        Self::SystemLog { task, stream }
    }

    pub(crate) fn emit(
        &self,
        line: &str,
    ) {
        match self {
            Self::SystemLog { task, stream: Stream::Stdout } => {
                tracing::info!(target: "task", task = %task, stream = "stdout", "{line}")
            }
            Self::SystemLog { task, stream: Stream::Stderr } => {
                tracing::warn!(target: "task", task = %task, stream = "stderr", "{line}")
            }
            #[cfg(feature = "log-pipeline")]
            Self::Channel { channels, channel, task, stream } => {
                use claviron_logging::pipeline::{Level, Record, Value};
                let level = match stream {
                    Stream::Stdout => Level::Info,
                    Stream::Stderr => Level::Warn,
                };
                let fields = [("stream", Value::Str(stream.as_str()))];
                let record = Record {
                    level,
                    target: task,
                    message: line,
                    fields: &fields,
                };
                if let Err(error) = channels.emit(*channel, &record) {
                    tracing::debug!(%error, "task log record not written");
                }
            }
        }
    }
}

/// An embedded task's writes, cut into lines: output arrives in whatever
/// pieces the program writes, and a record is a line.
#[derive(Debug)]
pub(crate) struct LineWriter {
    sink: LineSink,
    pending: Mutex<Vec<u8>>,
}

impl LineWriter {
    fn new(sink: LineSink) -> Arc<Self> {
        Arc::new(Self { sink, pending: Mutex::new(Vec::new()) })
    }

    fn write(
        &self,
        bytes: &[u8],
    ) {
        let mut pending =
            self.pending.lock().unwrap_or_else(|poison| poison.into_inner());
        pending.extend_from_slice(bytes);
        while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = pending.drain(..=end).collect();
            self.sink.emit(
                String::from_utf8_lossy(&line).trim_end_matches(['\r', '\n']),
            );
        }
    }

    /// What is left without a newline goes as a line of its own.
    fn flush(&self) {
        let mut pending =
            self.pending.lock().unwrap_or_else(|poison| poison.into_inner());
        if !pending.is_empty() {
            let line = std::mem::take(&mut *pending);
            self.sink.emit(
                String::from_utf8_lossy(&line).trim_end_matches(['\r', '\n']),
            );
        }
    }
}

impl LogConfig {
    pub fn open(
        &self,
        cwd: Option<&Path>,
        task: &str,
    ) -> TaskOutput {
        match self {
            Self::Single(LogTarget::Path(path)) => {
                let path = resolve_path(path, cwd);
                match open_append(&path) {
                    Ok(file) => {
                        let sink = OutputSink::File(Arc::new(Mutex::new(file)));
                        TaskOutput { stdout: sink.clone(), stderr: sink }
                    }
                    Err(error) => {
                        warn!(
                            path = %path.display(),
                            %error,
                            "failed to open task log file; writing to the server's log"
                        );
                        TaskOutput::system_log(task)
                    }
                }
            }
            Self::Single(target) => TaskOutput {
                stdout: open_sink(target, cwd, task, Stream::Stdout),
                stderr: open_sink(target, cwd, task, Stream::Stderr),
            },
            Self::Split { stdout, stderr } => {
                if let (LogTarget::Path(stdout), LogTarget::Path(stderr)) =
                    (stdout, stderr)
                    && stdout == stderr
                {
                    let path = resolve_path(stdout, cwd);
                    return match open_append(&path) {
                        Ok(file) => {
                            let sink =
                                OutputSink::File(Arc::new(Mutex::new(file)));
                            TaskOutput { stdout: sink.clone(), stderr: sink }
                        }
                        Err(error) => {
                            warn!(
                                path = %path.display(),
                                %error,
                                "failed to open task log file; writing to the server's log"
                            );
                            TaskOutput::system_log(task)
                        }
                    };
                }
                TaskOutput {
                    stdout: open_sink(stdout, cwd, task, Stream::Stdout),
                    stderr: open_sink(stderr, cwd, task, Stream::Stderr),
                }
            }
        }
    }
}

impl TaskOutput {
    /// Both streams to the server's own log.
    pub fn system_log(task: &str) -> Self {
        Self {
            stdout: OutputSink::Lines(LineWriter::new(LineSink::SystemLog {
                task: Arc::from(task),
                stream: Stream::Stdout,
            })),
            stderr: OutputSink::Lines(LineWriter::new(LineSink::SystemLog {
                task: Arc::from(task),
                stream: Stream::Stderr,
            })),
        }
    }

    pub fn write_stdout(
        &self,
        bytes: &[u8],
    ) -> bool {
        self.stdout.write(bytes)
    }

    pub fn write_stderr(
        &self,
        bytes: &[u8],
    ) -> bool {
        self.stderr.write(bytes)
    }

    pub fn flush_stdout(&self) -> bool {
        self.stdout.flush()
    }

    pub fn flush_stderr(&self) -> bool {
        self.stderr.flush()
    }
}

impl OutputSink {
    fn write(
        &self,
        bytes: &[u8],
    ) -> bool {
        match self {
            Self::Null => true,
            Self::Console(Stream::Stdout) => {
                std::io::stdout().lock().write_all(bytes).is_ok()
            }
            Self::Console(Stream::Stderr) => {
                std::io::stderr().lock().write_all(bytes).is_ok()
            }
            Self::Lines(lines) => {
                lines.write(bytes);
                true
            }
            Self::File(file) => {
                file.lock().is_ok_and(|mut file| file.write_all(bytes).is_ok())
            }
        }
    }

    fn flush(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Console(Stream::Stdout) => {
                std::io::stdout().lock().flush().is_ok()
            }
            Self::Console(Stream::Stderr) => {
                std::io::stderr().lock().flush().is_ok()
            }
            Self::Lines(lines) => {
                lines.flush();
                true
            }
            Self::File(file) => {
                file.lock().is_ok_and(|mut file| file.flush().is_ok())
            }
        }
    }
}

fn open_sink(
    target: &LogTarget,
    cwd: Option<&Path>,
    task: &str,
    stream: Stream,
) -> OutputSink {
    match target {
        LogTarget::Null => OutputSink::Null,
        LogTarget::Console => OutputSink::Console(stream),
        LogTarget::Inherit | LogTarget::Channel(_) => OutputSink::Lines(
            LineWriter::new(LineSink::for_target(target, task, stream)),
        ),
        LogTarget::Path(path) => {
            let path = resolve_path(path, cwd);
            match open_append(&path) {
                Ok(file) => OutputSink::File(Arc::new(Mutex::new(file))),
                Err(error) => {
                    match stream {
                        Stream::Stdout => error!(
                            path = %path.display(),
                            %error,
                            "failed to open stdout log file; writing to the server's log"
                        ),
                        Stream::Stderr => warn!(
                            path = %path.display(),
                            %error,
                            "failed to open stderr log file; writing to the server's log"
                        ),
                    }
                    OutputSink::Lines(LineWriter::new(LineSink::SystemLog {
                        task: Arc::from(task),
                        stream,
                    }))
                }
            }
        }
    }
}

pub(crate) fn open_append(path: &Path) -> std::io::Result<File> {
    if let Some(directory) = path.parent() {
        std::fs::create_dir_all(directory)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

#[cfg(test)]
#[path = "../tests/output.rs"]
mod tests;
