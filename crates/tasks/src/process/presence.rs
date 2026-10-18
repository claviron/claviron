//! Durable, same-host observations of recoverable command processes.
//!
//! The child owns `.instance`; its current Rust controller owns a separate
//! close-on-exec `.controller` lock. Neither a PID, a stale record, nor an OS
//! parent relationship grants control. `.presence` is an atomic read model,
//! not termination authority. Enumeration never signals, spawns or adopts.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use claviron_runtime::{AppState, RunMode};
use claviron_sys::process::{ProcessIdentity, identity};
use serde::{Deserialize, Serialize};

use super::instance;
use crate::{TaskDispatch, TaskRuntime};

const VERSION: u8 = 1;
const MAX_RECORD_BYTES: u64 = 16 * 1024;
static NEXT_WRITE: AtomicU64 = AtomicU64::new(0);

/// The exact controlling runtime, not the child's kernel PPID. Worker slot
/// numbers survive replacement; `process` distinguishes their incarnations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessOwner {
    Master(ProcessIdentity),
    Worker { worker_id: u8, process: ProcessIdentity },
    Standalone(ProcessIdentity),
}

impl ProcessOwner {
    pub fn process(self) -> ProcessIdentity {
        match self {
            Self::Master(process)
            | Self::Standalone(process)
            | Self::Worker { process, .. } => process,
        }
    }

    fn current(runtime: TaskRuntime) -> io::Result<Self> {
        let process = identity(std::process::id())
            .ok_or_else(|| invalid("controller identity unavailable"))?;
        if runtime.process_id != process.pid() {
            return Err(invalid(
                "task runtime does not identify the current controller process",
            ));
        }
        Ok(match runtime.run_mode {
            RunMode::Master => Self::Master(process),
            RunMode::Worker(id) => {
                Self::Worker { worker_id: id.raw(), process }
            }
            RunMode::Standalone => Self::Standalone(process),
        })
    }
}

/// Whether ownership can currently be observed. Legacy/missing/inconsistent
/// metadata is Unknown, never silently counted as managed or abandoned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskProcessControl {
    Managed,
    Detached,
    Unknown,
}

/// A matching kernel identity proves liveness at capture time. Unverified is
/// not an exit acknowledgement and never authorizes replacing or killing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskProcessLiveness {
    Alive,
    Unverified,
}

/// Last controller-published action, not an acknowledgement from PHP.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskProcessPhase {
    Running,
    Stopping,
}

/// One durable command identity, independent of current worker membership or
/// tasks.yaml. Missing logical fields explicitly identify pre-presence records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskProcessStatus {
    pub instance_key: String,
    pub task: Option<String>,
    pub dispatch: Option<TaskDispatch>,
    pub slot: Option<u8>,
    pub process: ProcessIdentity,
    pub spawned_by: Option<ProcessOwner>,
    pub controller: Option<ProcessOwner>,
    pub last_controller: Option<ProcessOwner>,
    pub control: TaskProcessControl,
    pub liveness: TaskProcessLiveness,
    pub last_phase: Option<TaskProcessPhase>,
    pub recorded_at: Option<SystemTime>,
}

/// A partial scan is explicit: corrupt or unreadable records are retained as
/// issues, not converted into an empty/healthy inventory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskProcessIssue {
    pub instance_key: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskProcessSnapshot {
    pub captured_at: SystemTime,
    pub processes: Vec<TaskProcessStatus>,
    pub issues: Vec<TaskProcessIssue>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireProcess {
    pid: u32,
    started_at: u64,
}

impl From<ProcessIdentity> for WireProcess {
    fn from(value: ProcessIdentity) -> Self {
        Self { pid: value.pid(), started_at: value.started_at() }
    }
}
impl WireProcess {
    fn identity(self) -> io::Result<ProcessIdentity> {
        if self.pid == 0 {
            return Err(invalid("zero process PID"));
        }
        Ok(ProcessIdentity::new(self.pid, self.started_at))
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "kebab-case", deny_unknown_fields)]
enum WireOwner {
    Master { process: WireProcess },
    Worker { worker_id: u8, process: WireProcess },
    Standalone { process: WireProcess },
}

impl From<ProcessOwner> for WireOwner {
    fn from(owner: ProcessOwner) -> Self {
        match owner {
            ProcessOwner::Master(process) => {
                Self::Master { process: process.into() }
            }
            ProcessOwner::Worker { worker_id, process } => {
                Self::Worker { worker_id, process: process.into() }
            }
            ProcessOwner::Standalone(process) => {
                Self::Standalone { process: process.into() }
            }
        }
    }
}
impl WireOwner {
    fn owner(self) -> io::Result<ProcessOwner> {
        Ok(match self {
            Self::Master { process } => {
                ProcessOwner::Master(process.identity()?)
            }
            Self::Standalone { process } => {
                ProcessOwner::Standalone(process.identity()?)
            }
            Self::Worker { worker_id, process } if worker_id != 0 => {
                ProcessOwner::Worker { worker_id, process: process.identity()? }
            }
            Self::Worker { .. } => return Err(invalid("zero worker ID")),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u8,
    task: String,
    dispatch: TaskDispatch,
    slot: u8,
    process: WireProcess,
    spawned_by: Option<WireOwner>,
    controller: WireOwner,
    phase: TaskProcessPhase,
    recorded_at_ms: u64,
}

impl Record {
    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let record: Self = serde_yaml::from_slice(bytes).map_err(invalid)?;
        if record.version != VERSION
            || record.task.is_empty()
            || record.slot == 0
            || !matches!(
                record.dispatch,
                TaskDispatch::Each | TaskDispatch::One | TaskDispatch::Pool
            )
        {
            return Err(invalid(
                "invalid command presence version or logical identity",
            ));
        }
        record.process.identity()?;
        record.controller.owner()?;
        record.spawned_by.map(WireOwner::owner).transpose()?;
        Ok(record)
    }
}

/// Exclusive supervision tenure. The child does NOT inherit this lock. It
/// survives every attach->spawn transition of one task body and releases on
/// cancellation, completed drain, panic or controller-process death.
pub(super) struct Controller {
    _lock: File,
    path: PathBuf,
    owner: ProcessOwner,
    runtime: TaskRuntime,
}

impl Controller {
    pub(super) async fn acquire(
        state: &AppState,
        runtime: TaskRuntime,
    ) -> io::Result<Self> {
        let owner = ProcessOwner::current(runtime)?;
        let path = instance::instance_path(state, runtime)?;
        let lock =
            instance::open_instance_file(&path.with_extension("controller"))?;
        validate_file(&lock)?;
        // An inventory probe can briefly hold this lock too. Give publication
        // and read-side probes the same bounded rendezvous window as the
        // instance record protocol, without charging a task retry for a probe.
        let deadline =
            tokio::time::Instant::now() + super::instance::RECORD_READY_TIMEOUT;
        loop {
            match instance::try_lock(lock.as_raw_fd()) {
                Ok(()) => break,
                Err(error)
                    if instance::is_lock_busy(&error)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(super::instance::RECORD_RETRY_INTERVAL)
                        .await;
                }
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "task '{}' slot {} already controlled or inaccessible: {error}",
                            runtime.task_name,
                            runtime.slot()
                        ),
                    ));
                }
            }
        }
        Ok(Self { _lock: lock, path, owner, runtime })
    }

    pub(super) fn publish(
        &self,
        process: ProcessIdentity,
        attached: bool,
        phase: TaskProcessPhase,
    ) -> io::Result<()> {
        let path = self.path.with_extension("presence");
        let previous = match read_presence(&path) {
            Ok(record) => Some(record),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let spawned_by = if let Some(previous) = previous
            .filter(|record| record.process.identity().ok() == Some(process))
        {
            previous.spawned_by
        } else if attached {
            // v1 instance records cannot reconstruct the original owner.
            None
        } else {
            Some(self.owner.into())
        };
        let record = Record {
            version: VERSION,
            task: self.runtime.task_name.to_owned(),
            dispatch: self.runtime.task_dispatch,
            slot: self.runtime.slot(),
            process: process.into(),
            spawned_by,
            controller: self.owner.into(),
            phase,
            recorded_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(invalid)?
                .as_millis()
                .try_into()
                .map_err(invalid)?,
        };
        write_atomic(
            &path,
            serde_yaml::to_string(&record).map_err(invalid)?.as_bytes(),
        )
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        // A concurrently forked child may briefly hold a duplicate before exec.
        // Explicit unlock ends this tenure even before that duplicate closes.
        if unsafe {
            nix::libc::flock(self._lock.as_raw_fd(), nix::libc::LOCK_UN)
        } == -1
        {
            tracing::warn!(error = %io::Error::last_os_error(), "failed to release task controller lock");
        }
    }
}

/// Read the whole application namespace, never just the configured task or
/// current worker lanes. This is deliberately independent of Orbit freshness.
pub(crate) fn inspect(state: &AppState) -> io::Result<TaskProcessSnapshot> {
    let mut snapshot = TaskProcessSnapshot {
        captured_at: SystemTime::now(),
        processes: vec![],
        issues: vec![],
    };
    let dir = instance::instance_directory(state)?;
    match fs::symlink_metadata(&dir) {
        Ok(metadata)
            if !metadata.is_dir()
                || metadata.uid() != unsafe { nix::libc::geteuid() }
                || metadata.mode() & 0o077 != 0 =>
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsafe task instance directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(snapshot);
        }
        Err(error) => return Err(error),
    }
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(snapshot);
        }
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "instance") {
            continue;
        }
        let key =
            path.file_stem().unwrap_or_default().to_string_lossy().into_owned();
        match inspect_one(&path, key.clone()) {
            Ok((status, issue)) => {
                snapshot.processes.push(status);
                if let Some(error) = issue {
                    snapshot
                        .issues
                        .push(TaskProcessIssue { instance_key: key, error });
                }
            }
            Err(error) => snapshot.issues.push(TaskProcessIssue {
                instance_key: key,
                error: error.to_string(),
            }),
        }
    }
    snapshot.processes.sort_by(|a, b| a.instance_key.cmp(&b.instance_key));
    snapshot.issues.sort_by(|a, b| a.instance_key.cmp(&b.instance_key));
    Ok(snapshot)
}

fn inspect_one(
    path: &Path,
    instance_key: String,
) -> io::Result<(TaskProcessStatus, Option<String>)> {
    let record = instance::read_record(&mut open_read(path)?)?;
    let mut status = TaskProcessStatus {
        instance_key,
        process: record.identity,
        task: None,
        dispatch: None,
        slot: None,
        spawned_by: None,
        controller: None,
        last_controller: None,
        control: TaskProcessControl::Unknown,
        liveness: if record.identity.is_alive() {
            TaskProcessLiveness::Alive
        } else {
            TaskProcessLiveness::Unverified
        },
        last_phase: None,
        recorded_at: None,
    };
    let presence = match read_presence(&path.with_extension("presence")) {
        Ok(record) if record.process.identity()? == status.process => record,
        Ok(_) => {
            return Ok((
                status,
                Some(
                    "presence identifies a different command incarnation"
                        .into(),
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok((status, None));
        }
        Err(error) => return Ok((status, Some(error.to_string()))),
    };
    let controller = presence.controller.owner()?;
    status.task = Some(presence.task);
    status.dispatch = Some(presence.dispatch);
    status.slot = Some(presence.slot);
    status.spawned_by =
        presence.spawned_by.map(WireOwner::owner).transpose()?;
    status.last_controller = Some(controller);
    status.last_phase = Some(presence.phase);
    status.recorded_at =
        UNIX_EPOCH.checked_add(Duration::from_millis(presence.recorded_at_ms));
    let file = match open_read(&path.with_extension("controller")) {
        Ok(file) => file,
        Err(error) => return Ok((status, Some(error.to_string()))),
    };
    match instance::try_lock(file.as_raw_fd()) {
        Ok(()) => status.control = TaskProcessControl::Detached,
        Err(error) if instance::is_lock_busy(&error) => {
            if controller.process().is_alive() {
                status.controller = Some(controller);
                status.control = TaskProcessControl::Managed;
            }
        }
        Err(error) => return Ok((status, Some(error.to_string()))),
    }
    Ok((status, None))
}

fn validate_file(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { nix::libc::geteuid() }
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe task presence file",
        ));
    }
    Ok(())
}

fn open_read(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)?;
    validate_file(&file)?;
    Ok(file)
}

fn read_presence(path: &Path) -> io::Result<Record> {
    let mut bytes = Vec::new();
    open_read(path)?.take(MAX_RECORD_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(invalid("task presence record too large"));
    }
    Record::decode(&bytes)
}

fn write_atomic(
    path: &Path,
    bytes: &[u8],
) -> io::Result<()> {
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(invalid("task presence record too large"));
    }
    let seq = NEXT_WRITE.fetch_add(1, Ordering::Relaxed);
    let temporary = path
        .with_extension(format!("presence-{}-{seq}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
#[path = "../../tests/process/presence.rs"]
mod tests;
