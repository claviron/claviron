//! The one writer of a fleet: drains every lane, formats, writes each
//! channel's records to its adapter.
//!
//! Woken by producers' doorbells and nothing else — under load one wake
//! carries many records, when traffic is light each is written as it comes.
//!
//! Reading the ring and writing the files happen on one thread of its own,
//! never on the async runtime's workers, and at the pace the files take: the
//! ring is the only buffer. A writer slower than its producers lets the ring
//! overwrite, and counts and reports what was lost; nothing queues in memory
//! behind a slow disk.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::registry::{self};
use claviron_runtime::{AppState, Component, RunContext, Runnable};
use orbitive::Fleet;
use orbitive::fleet::FleetLaneCursor;

use super::channels::LogChannels;
use super::config::{ChannelConfig, FileFormat};
use super::record::{self, Decoded, OwnedValue};
use super::ring::{FRAME_KIND_PART, FRAME_KIND_RECORD, LogFrame};

/// Registered everywhere, run in the master or in a standalone process: one
/// writer per fleet. It writes the channels the shared table declares and
/// reopens its sinks when that table changes.
pub(crate) struct LogWriter {
    channels: Arc<LogChannels>,
    /// A managed service restart is not a new process startup.
    sinks_initialized: AtomicBool,
}

impl LogWriter {
    pub(crate) fn new(channels: Arc<LogChannels>) -> Self {
        Self { channels, sinks_initialized: AtomicBool::new(false) }
    }
}

impl Component<AppState> for LogWriter {
    type Deps = (Arc<claviron_orbit::FleetProvider>,);

    fn name(&self) -> &'static str {
        "LogWriter"
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}

impl Scoped for LogWriter {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

/// What the async side tells the writing thread.
enum Nudge {
    /// The ring has something; read it.
    Work,
    /// The files were moved (a log rotation): open them again at their
    /// paths.
    Reopen,
    /// The declared channels changed: write what was read to the channels as
    /// they were, then open sinks for the channels as they are.
    Reconfigure,
    /// The producers are stopping or stopped: read what is left and end.
    Stop,
}

#[async_trait]
impl Runnable<AppState, (Arc<claviron_orbit::FleetProvider>,)> for LogWriter {
    async fn run(
        self: Arc<Self>,
        (fleet,): (Arc<claviron_orbit::FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = super::shared_fleet(&fleet)?;
        let sinks = Sinks::open(
            &self.channels.table().channels,
            state.name(),
            !self.sinks_initialized.load(Ordering::Relaxed),
        )?;
        self.sinks_initialized.store(true, Ordering::Relaxed);
        // Taken before the first readiness, so nothing published after this
        // point is skipped; what was there before belongs to an earlier run.
        let cursor = fleet.lane_cursor_at_head::<LogFrame>();
        let pending = Arc::new(AtomicBool::new(false));
        let (nudges, receiver) = mpsc::channel::<Nudge>();
        let writing = {
            let fleet = Arc::clone(&fleet);
            let channels = Arc::clone(&self.channels);
            let service = state.name().to_owned();
            let pending = Arc::clone(&pending);
            std::thread::Builder::new()
                .name("log-writer".to_owned())
                .spawn(move || {
                    write_loop(
                        &fleet, &channels, &service, cursor, sinks, &pending,
                        receiver,
                    )
                })
                .map_err(failure)?
        };

        let reopen = state.events().subscribe::<claviron_runtime::ReopenLogs>();
        let result = wait_loop(
            &fleet,
            &self.channels,
            &pending,
            &nudges,
            reopen,
            &context,
        )
        .await;
        let _ = nudges.send(Nudge::Stop);
        join(writing).await;
        result
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn wait_loop(
    fleet: &Fleet,
    channels: &LogChannels,
    pending: &AtomicBool,
    nudges: &mpsc::Sender<Nudge>,
    mut reopen: tokio::sync::broadcast::Receiver<claviron_runtime::ReopenLogs>,
    context: &RunContext,
) -> registry::Result<()> {
    let ready = fleet
        .ring_event_fd_parked::<LogFrame>()
        .and_then(|event_fd| {
            // SAFETY: ParkedRingEventFd owns its stable descriptor until drop;
            // the writer only drains it and never replaces the inner value.
            unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
                .map_err(std::io::Error::from)
        })
        .map_err(|error| failure(format!("log ring readiness: {error}")))?;
    loop {
        tokio::select! {
            biased;
            () = context.cancelled() => break,
            requested = reopen.recv() => {
                // A lagged receiver still means a reopen was asked for.
                if matches!(requested, Err(tokio::sync::broadcast::error::RecvError::Closed)) {
                    continue;
                }
                if nudges.send(Nudge::Reopen).is_err() {
                    return Err(failure("the log writer thread has stopped"));
                }
            }
            () = channels.changed() => {
                if nudges.send(Nudge::Reconfigure).is_err() {
                    return Err(failure("the log writer thread has stopped"));
                }
            }
            guard = ready.readable() => {
                // Clear before draining: a signal that lands after the drain
                // is the only one the parked driver sends until the next
                // drain, and clearing after would erase it.
                let mut guard = guard.map_err(failure)?;
                guard.clear_ready();
                ready.get_ref().drain().map_err(failure)?;
                // One nudge in flight at a time; the thread reads everything
                // there is when it takes it.
                if !pending.swap(true, Ordering::AcqRel) && nudges.send(Nudge::Work).is_err() {
                    return Err(failure("the log writer thread has stopped"));
                }
            }
        }
    }
    Ok(())
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "macos"
)))]
async fn wait_loop(
    _fleet: &Fleet,
    _channels: &LogChannels,
    _pending: &AtomicBool,
    _nudges: &mpsc::Sender<Nudge>,
    _reopen: tokio::sync::broadcast::Receiver<claviron_runtime::ReopenLogs>,
    _context: &RunContext,
) -> registry::Result<()> {
    Err(failure(
        "the log pipeline needs native Orbit readiness on this platform",
    ))
}

fn write_loop(
    fleet: &Fleet,
    channels: &LogChannels,
    service: &str,
    mut cursor: FleetLaneCursor,
    mut sinks: Sinks,
    pending: &AtomicBool,
    nudges: mpsc::Receiver<Nudge>,
) {
    let mut table = channels.table();
    let mut lines: Vec<Vec<u8>> = vec![Vec::new(); table.channels.len()];
    let mut parts = Parts::default();
    for nudge in nudges {
        match nudge {
            Nudge::Reopen => {
                // What was read before the move goes to the old file; the
                // rest to the new one.
                while drain(
                    fleet,
                    &table.channels,
                    &mut cursor,
                    &mut parts,
                    &sinks,
                    &mut lines,
                ) {
                    sinks.write(&mut lines);
                }
                sinks.reopen();
                continue;
            }
            Nudge::Reconfigure => {
                // What was read goes to the channels as they were declared.
                while drain(
                    fleet,
                    &table.channels,
                    &mut cursor,
                    &mut parts,
                    &sinks,
                    &mut lines,
                ) {
                    sinks.write(&mut lines);
                }
                table = channels.table();
                sinks.reconfigure(&table.channels, service);
                lines.resize(table.channels.len(), Vec::new());
                continue;
            }
            Nudge::Work | Nudge::Stop => {}
        }
        // Cleared before reading: a publish after this read nudges again.
        pending.store(false, Ordering::Release);
        while drain(
            fleet,
            &table.channels,
            &mut cursor,
            &mut parts,
            &sinks,
            &mut lines,
        ) {
            sinks.write(&mut lines);
        }
        if matches!(nudge, Nudge::Stop) {
            sinks.shutdown();
            break;
        }
    }
}

/// Records being put back together from their parts, by the node that
/// wrote them.
#[derive(Default)]
struct Parts {
    open: std::collections::HashMap<u16, Assembly>,
}

struct Assembly {
    count: u8,
    next: u8,
    bytes: Vec<u8>,
}

impl Parts {
    /// Take one part; the whole record once its last part arrives. A part
    /// out of order ends the record it belongs to, which is then lost.
    fn take(
        &mut self,
        node: u16,
        payload: &[u8],
    ) -> Option<Vec<u8>> {
        let [index, count, bytes @ ..] = payload else {
            tracing::warn!(node, "a log record part is too short");
            return None;
        };
        if *index == 0 {
            if self.open.remove(&node).is_some() {
                tracing::warn!(
                    node,
                    "a log record was lost with some of its parts"
                );
            }
            self.open.insert(
                node,
                Assembly { count: *count, next: 0, bytes: Vec::new() },
            );
        }
        let Some(assembly) = self.open.get_mut(&node) else {
            tracing::warn!(
                node,
                "a log record was lost with some of its parts"
            );
            return None;
        };
        if assembly.next != *index || assembly.count != *count {
            self.open.remove(&node);
            tracing::warn!(
                node,
                "a log record was lost with some of its parts"
            );
            return None;
        }
        assembly.bytes.extend_from_slice(bytes);
        assembly.next += 1;
        if assembly.next < assembly.count {
            return None;
        }
        self.open.remove(&node).map(|assembly| assembly.bytes)
    }

    /// A whole record from `node` ends any record it left unfinished.
    fn close(
        &mut self,
        node: u16,
    ) {
        if self.open.remove(&node).is_some() {
            tracing::warn!(
                node,
                "a log record was lost with some of its parts"
            );
        }
    }
}

/// Read what the lanes hold into `lines`; `false` when there was nothing.
fn drain(
    fleet: &Fleet,
    channels: &[Option<(String, ChannelConfig)>],
    cursor: &mut FleetLaneCursor,
    parts: &mut Parts,
    sinks: &Sinks,
    lines: &mut [Vec<u8>],
) -> bool {
    let poll = fleet.poll_lanes::<LogFrame>(cursor);
    if poll.loss.overwritten > 0 || poll.loss.unavailable > 0 {
        tracing::warn!(
            overwritten = poll.loss.overwritten,
            unavailable = poll.loss.unavailable,
            "log records were lost before the writer read them"
        );
    }
    if poll.frames.is_empty() {
        return false;
    }
    for frame in poll.frames {
        let node = frame.id.node();
        let whole = match frame.kind {
            FRAME_KIND_RECORD => {
                parts.close(node);
                frame.payload.to_vec()
            }
            FRAME_KIND_PART => match parts.take(node, &frame.payload) {
                Some(whole) => whole,
                None => continue,
            },
            _ => continue,
        };
        let Some(decoded) = record::decode(&whole) else {
            tracing::warn!("a log record could not be decoded");
            continue;
        };
        let index = usize::from(decoded.channel.0);
        let Some(Some((name, config))) = channels.get(index) else {
            tracing::warn!(
                channel = index,
                "a log record names no declared channel"
            );
            continue;
        };
        match config {
            ChannelConfig::File { format: FileFormat::Json, .. } => {
                json_line(&mut lines[index], name, node, &decoded)
            }
            ChannelConfig::File { format: FileFormat::Line, .. } => {
                text_line(&mut lines[index], name, node, &decoded)
            }
            ChannelConfig::Otlp { .. } => {
                sinks.emit(index, name, node, &decoded)
            }
        }
    }
    true
}

fn json_line(
    out: &mut Vec<u8>,
    channel: &str,
    node: u16,
    record: &Decoded,
) {
    let mut object = serde_json::Map::new();
    object.insert("time".into(), rfc3339(record.time).into());
    object.insert("level".into(), record.level.as_str().into());
    object.insert("channel".into(), channel.into());
    object.insert("node".into(), node.into());
    if !record.target.is_empty() {
        object.insert("target".into(), record.target.clone().into());
    }
    if !record.message.is_empty() {
        object.insert("message".into(), record.message.clone().into());
    }
    let mut collisions = serde_json::Map::new();
    for (key, value) in &record.fields {
        if matches!(
            key.as_str(),
            "time"
                | "level"
                | "channel"
                | "node"
                | "target"
                | "message"
                | "truncated"
                | "fields"
        ) {
            collisions.insert(key.clone(), json_value(value));
        } else {
            object.insert(key.clone(), json_value(value));
        }
    }
    if !collisions.is_empty() {
        object.insert("fields".into(), collisions.into());
    }
    if record.truncated {
        object.insert("truncated".into(), true.into());
    }
    if serde_json::to_writer(&mut *out, &object).is_ok() {
        out.push(b'\n');
    }
}

fn json_value(value: &OwnedValue) -> serde_json::Value {
    match value {
        OwnedValue::Str(text) => text.clone().into(),
        OwnedValue::U64(number) => (*number).into(),
        OwnedValue::I64(number) => (*number).into(),
        OwnedValue::F64(number) => serde_json::Number::from_f64(*number)
            .map_or(serde_json::Value::Null, Into::into),
        OwnedValue::Bool(flag) => (*flag).into(),
    }
}

fn text_line(
    out: &mut Vec<u8>,
    channel: &str,
    node: u16,
    record: &Decoded,
) {
    use std::fmt::Write as _;

    let mut line = format!(
        "{} {:<5} {channel} [node {node}]",
        rfc3339(record.time),
        record.level.as_str().to_ascii_uppercase()
    );
    if !record.target.is_empty() {
        let _ = write!(line, " {}:", record.target);
    }
    if !record.message.is_empty() {
        let _ = write!(line, " {}", record.message);
    }
    for (key, value) in &record.fields {
        let _ = match value {
            OwnedValue::Str(text) => write!(line, " {key}={text:?}"),
            OwnedValue::U64(number) => write!(line, " {key}={number}"),
            OwnedValue::I64(number) => write!(line, " {key}={number}"),
            OwnedValue::F64(number) => write!(line, " {key}={number}"),
            OwnedValue::Bool(flag) => write!(line, " {key}={flag}"),
        };
    }
    if record.truncated {
        line.push_str(" [truncated]");
    }
    line.push('\n');
    out.extend_from_slice(line.as_bytes());
}

fn rfc3339(unix_nanos: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(unix_nanos))
        .ok()
        .and_then(|at| {
            at.format(&time::format_description::well_known::Rfc3339).ok()
        })
        .unwrap_or_else(|| unix_nanos.to_string())
}

/// The opened destinations, by channel id.
struct Sinks {
    /// By channel id; `None` where no channel is declared.
    sinks: Vec<Option<Opened>>,
}

/// A sink and the declaration it was opened for, so a reconfiguration can
/// tell an unchanged channel from a changed one.
struct Opened {
    config: ChannelConfig,
    sink: Sink,
}

enum Sink {
    File {
        name: String,
        path: std::path::PathBuf,
        file: File,
    },
    #[cfg(feature = "otlp")]
    Otlp(super::otlp::OtlpSink),
}

impl Sinks {
    fn open(
        channels: &[Option<(String, ChannelConfig)>],
        service: &str,
        startup: bool,
    ) -> registry::Result<Self> {
        let mut sinks = Vec::with_capacity(channels.len());
        for channel in channels {
            sinks.push(match channel {
                Some((name, config)) => Some(Opened {
                    config: config.clone(),
                    sink: open_one(name, config, service, startup)?,
                }),
                None => None,
            });
        }
        Ok(Self { sinks })
    }

    /// Match the sinks to the channels as they are now declared: an
    /// unchanged channel keeps its sink, a changed or new one is opened, a
    /// withdrawn one is closed. A sink that cannot be opened is said and
    /// left absent; its records are counted as naming no channel.
    fn reconfigure(
        &mut self,
        channels: &[Option<(String, ChannelConfig)>],
        service: &str,
    ) {
        self.sinks.resize_with(channels.len(), || None);
        for (index, declared) in channels.iter().enumerate() {
            let current = &mut self.sinks[index];
            match declared {
                Some((name, config)) => {
                    if current
                        .as_ref()
                        .is_some_and(|opened| opened.config == *config)
                    {
                        continue;
                    }
                    if let Some(previous) = current.take() {
                        previous.sink.shutdown();
                    }
                    match open_one(name, config, service, false) {
                        Ok(sink) => {
                            *current =
                                Some(Opened { config: config.clone(), sink })
                        }
                        Err(error) => {
                            tracing::error!(channel = %name, %error, "log channel could not be opened")
                        }
                    }
                }
                None => {
                    if let Some(previous) = current.take() {
                        previous.sink.shutdown();
                    }
                }
            }
        }
    }

    /// Hand one record to a channel that takes records one by one.
    fn emit(
        &self,
        index: usize,
        name: &str,
        node: u16,
        record: &Decoded,
    ) {
        #[cfg(feature = "otlp")]
        if let Some(Some(Opened { sink: Sink::Otlp(sink), .. })) =
            self.sinks.get(index)
        {
            sink.emit(name, node, record);
        }
        #[cfg(not(feature = "otlp"))]
        let _ = (index, name, node, record);
    }

    /// Open every file again at its path; one that fails keeps the old.
    // Irrefutable when the build has no network sink.
    #[allow(irrefutable_let_patterns)]
    fn reopen(&mut self) {
        for opened in self.sinks.iter_mut().flatten() {
            if let Sink::File { name, path, file } = &mut opened.sink {
                match OpenOptions::new().create(true).append(true).open(&*path)
                {
                    Ok(reopened) => *file = reopened,
                    Err(error) => {
                        tracing::error!(channel = %name, path = %path.display(), %error, "log channel could not be reopened")
                    }
                }
            }
        }
    }

    /// One write per file channel for what was read; the buffers are kept.
    // Irrefutable when the build has no network sink.
    #[allow(irrefutable_let_patterns)]
    fn write(
        &mut self,
        lines: &mut [Vec<u8>],
    ) {
        for (index, bytes) in lines.iter_mut().enumerate() {
            if bytes.is_empty() {
                continue;
            }
            if let Some(Some(Opened {
                sink: Sink::File { name, file, .. },
                ..
            })) = self.sinks.get_mut(index)
                && let Err(error) = file.write_all(bytes)
            {
                tracing::error!(channel = %name, %error, "log channel write failed");
            }
            bytes.clear();
        }
    }

    /// Send what the network channels still hold.
    fn shutdown(&self) {
        for opened in self.sinks.iter().flatten() {
            opened.sink.shutdown();
        }
    }
}

impl Sink {
    /// Send what a network channel still holds; a file needs nothing.
    fn shutdown(&self) {
        #[cfg(feature = "otlp")]
        if let Self::Otlp(sink) = self {
            sink.shutdown();
        }
    }
}

fn open_one(
    name: &str,
    config: &ChannelConfig,
    service: &str,
    startup: bool,
) -> registry::Result<Sink> {
    match config {
        ChannelConfig::File { path, truncate_on_startup, .. } => {
            if let Some(parent) =
                path.parent().filter(|p| !p.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent).map_err(|error| {
                    failure(format!(
                        "log channel '{name}': {}: {error}",
                        parent.display()
                    ))
                })?;
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .and_then(|file| {
                    if startup && *truncate_on_startup {
                        file.set_len(0)?;
                    }
                    Ok(file)
                })
                .map_err(|error| {
                    failure(format!(
                        "log channel '{name}': {}: {error}",
                        path.display()
                    ))
                })?;
            Ok(Sink::File { name: name.to_owned(), path: path.clone(), file })
        }
        #[cfg(feature = "otlp")]
        ChannelConfig::Otlp { endpoint, queue } => {
            super::otlp::OtlpSink::open(endpoint, *queue, service)
                .map(Sink::Otlp)
                .map_err(|error| {
                    failure(format!("log channel '{name}': {error}"))
                })
        }
        #[cfg(not(feature = "otlp"))]
        ChannelConfig::Otlp { .. } => {
            let _ = service;
            Err(failure(format!(
                "log channel '{name}' is OTLP, and this build has no OTLP adapter (feature `otlp`)"
            )))
        }
    }
}

async fn join(files: JoinHandle<()>) {
    let _ = tokio::task::spawn_blocking(move || files.join()).await;
}

fn failure(error: impl std::fmt::Display) -> registry::Error {
    registry::Error::msg(error.to_string())
}

#[cfg(test)]
#[path = "../../tests/pipeline/writer.rs"]
mod tests;
