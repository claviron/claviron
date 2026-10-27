use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use claviron_runtime::{AppState, RunMode};
use claviron_sys::process::{ProcessHandle, ProcessIdentity, identity};
use nix::libc;
use tokio::time::sleep;

use super::CommandSpec;
use crate::runner::TaskRuntime;

const INSTANCE_RECORD_VERSION: &str = "v1";
pub(super) const RECORD_READY_TIMEOUT: Duration = Duration::from_millis(250);
pub(super) const RECORD_RETRY_INTERVAL: Duration = Duration::from_millis(5);

pub(super) enum CommandInstance {
    Spawn(SpawnReservation),
    Attach(AttachedInstance),
}

pub(super) struct SpawnReservation {
    file: File,
    path: PathBuf,
    fingerprint: [u8; 32],
}

impl SpawnReservation {
    pub(super) fn raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    pub(super) fn publish(
        mut self,
        pid: u32,
    ) -> io::Result<ProcessIdentity> {
        let identity = identity(pid).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("spawned command PID {pid} has no process identity"),
            )
        })?;
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        writeln!(
            self.file,
            "{} {} {} {}",
            INSTANCE_RECORD_VERSION,
            identity.pid(),
            identity.started_at(),
            encode_hex(&self.fingerprint)
        )?;
        self.file.sync_data().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "sync command instance record {}: {error}",
                    self.path.display()
                ),
            )
        })?;
        // Closing the parent's descriptor must leave the lock held by the
        // exec'd command. A foreground command that closes inherited file
        // descriptors cannot participate in persistent supervision safely.
        drop(self.file);
        let probe = open_instance_file(&self.path)?;
        match try_lock(probe.as_raw_fd()) {
            Err(error) if is_lock_busy(&error) => {}
            Err(error) => return Err(error),
            Ok(()) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "spawned command did not retain its instance lock descriptor",
                ));
            }
        }
        Ok(identity)
    }
}

pub(super) struct AttachedInstance {
    handle: ProcessHandle,
}

impl AttachedInstance {
    pub(super) fn identity(&self) -> ProcessIdentity {
        self.handle.identity()
    }

    pub(super) fn pid(&self) -> u32 {
        self.handle.identity().pid()
    }

    pub(super) async fn wait(&self) -> io::Result<()> {
        self.handle.wait().await
    }

    pub(super) fn signal(
        &self,
        signal: libc::c_int,
    ) -> io::Result<()> {
        self.handle.signal(signal)
    }
}

/// Resolve one stable command instance. The lock is taken before spawn and
/// inherited by the child; a later Claviron incarnation sees the busy lock,
/// validates the PID/start stamp record, and attaches instead of duplicating.
pub(super) async fn resolve(
    state: &AppState,
    runtime: &TaskRuntime,
    spec: &CommandSpec,
) -> io::Result<CommandInstance> {
    let path = instance_path(state, runtime)?;
    let fingerprint = fingerprint(spec);
    let deadline = Instant::now() + RECORD_READY_TIMEOUT;

    loop {
        let mut file = open_instance_file(&path)?;
        match try_lock(file.as_raw_fd()) {
            Ok(()) => {
                return Ok(CommandInstance::Spawn(SpawnReservation {
                    file,
                    path,
                    fingerprint,
                }));
            }
            Err(error) if is_lock_busy(&error) => {}
            Err(error) => return Err(error),
        }

        let record = read_record(&mut file);
        match record {
            Ok(record) if record.fingerprint != fingerprint => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "live command instance {} was started from a different command specification",
                        path.display()
                    ),
                ));
            }
            Ok(record) => match ProcessHandle::open(record.identity) {
                Ok(handle) => {
                    return Ok(CommandInstance::Attach(AttachedInstance {
                        handle,
                    }));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            },
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ) => {}
            Err(error) => return Err(error),
        }

        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "command instance lock {} is held but its live process record is unavailable",
                    path.display()
                ),
            ));
        }
        sleep(RECORD_RETRY_INTERVAL).await;
    }
}

/// Clear close-on-exec in the forked child so the command owns the instance
/// lock after Claviron drops its copy of the descriptor.
pub(super) unsafe fn inherit_lock(raw_fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(raw_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) }
        == -1
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(super) fn instance_path(
    state: &AppState,
    runtime: &TaskRuntime,
) -> io::Result<PathBuf> {
    let dir = instance_directory(state)?;
    let subject = match runtime.task_dispatch {
        crate::TaskDispatch::Each => {
            let lane = match runtime.run_mode {
                RunMode::Master => "master".to_string(),
                RunMode::Worker(worker_id) => {
                    format!("worker-{}", worker_id.raw())
                }
                RunMode::Standalone => "standalone".to_string(),
            };
            format!(
                "each:{lane}:{}:{}",
                runtime.task_name, runtime.task_worker_number
            )
        }
        crate::TaskDispatch::Pool => {
            format!("pool:{}:{}", runtime.task_name, runtime.task_worker_number)
        }
        crate::TaskDispatch::One => format!("one:{}", runtime.task_name),
    };
    create_private_dir(&dir)?;
    Ok(dir.join(format!("{}.instance", short_hash(subject.as_bytes()))))
}

/// Resolve the shared instance namespace without creating or changing it.
pub(super) fn instance_directory(state: &AppState) -> io::Result<PathBuf> {
    let base = state
        .env_snapshot()
        .get("RUNTIME_DIRECTORY")
        .or_else(|| state.env_snapshot().get("TMPDIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    let config_dir = std::fs::canonicalize(state.config_dir())
        .unwrap_or_else(|_| state.config_dir().to_path_buf());
    let app_identity = format!("{}:{}", state.name(), config_dir.display());
    let app_hash = short_hash(app_identity.as_bytes());
    let uid = unsafe { libc::geteuid() };
    let dir = base.join(format!("claviron-tasks-{uid}-{app_hash}"));
    Ok(dir)
}

fn create_private_dir(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_symlink() || !metadata.is_dir() =>
        {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "command instance path is not a directory",
            ))
        }
        Ok(metadata)
            if metadata.uid() != unsafe { libc::geteuid() }
                || metadata.mode() & 0o077 != 0 =>
        {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "command instance directory must be owned by the current user with mode 0700",
            ))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true).mode(0o700).create(path)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn open_instance_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(path)
}

pub(super) fn try_lock(raw_fd: RawFd) -> io::Result<()> {
    let result = unsafe { libc::flock(raw_fd, libc::LOCK_EX | libc::LOCK_NB) };
    if result == -1 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

pub(super) fn is_lock_busy(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::EWOULDBLOCK)
}

pub(super) struct InstanceRecord {
    pub(super) identity: ProcessIdentity,
    fingerprint: [u8; 32],
}

pub(super) fn read_record(file: &mut File) -> io::Result<InstanceRecord> {
    file.seek(SeekFrom::Start(0))?;
    let mut raw = String::new();
    file.take(1025).read_to_string(&mut raw)?;
    if raw.len() > 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "instance record too large",
        ));
    }
    let mut fields = raw.split_whitespace();
    if fields.next() != Some(INSTANCE_RECORD_VERSION) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid instance record version",
        ));
    }
    let pid = parse_field(fields.next(), "pid")?;
    let started_at = parse_field(fields.next(), "process start identity")?;
    let fingerprint = decode_hex(fields.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "missing command fingerprint",
        )
    })?)?;
    if pid == 0 || fields.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid instance record fields",
        ));
    }
    Ok(InstanceRecord {
        identity: ProcessIdentity::new(pid, started_at),
        fingerprint,
    })
}

fn parse_field<T: std::str::FromStr>(
    field: Option<&str>,
    name: &str,
) -> io::Result<T> {
    field
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("missing {name}"),
            )
        })?
        .parse()
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid {name}"),
            )
        })
}

fn fingerprint(spec: &CommandSpec) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hash_field(&mut hasher, b"claviron-command-instance-v1");
    hash_field(&mut hasher, spec.program.as_bytes());
    hash_count(&mut hasher, spec.args.len());
    for arg in &spec.args {
        hash_field(&mut hasher, arg.as_bytes());
    }
    hash_field(&mut hasher, spec.cwd.as_deref().unwrap_or_default().as_bytes());
    let mut env = spec.env.iter().collect::<Vec<_>>();
    env.sort_unstable_by(|left, right| left.0.cmp(right.0));
    hash_count(&mut hasher, env.len());
    for (key, value) in env {
        hash_field(&mut hasher, key.as_bytes());
        hash_field(&mut hasher, value.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn hash_count(
    hasher: &mut blake3::Hasher,
    count: usize,
) {
    hasher.update(&(count as u64).to_be_bytes());
}

fn hash_field(
    hasher: &mut blake3::Hasher,
    bytes: &[u8],
) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn short_hash(bytes: &[u8]) -> String {
    encode_hex(&blake3::hash(bytes).as_bytes()[..8])
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(value: &str) -> io::Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid command fingerprint",
        ));
    }
    let mut decoded = [0u8; 32];
    for (index, byte) in decoded.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16).map_err(
            |_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid command fingerprint",
                )
            },
        )?;
    }
    Ok(decoded)
}

#[cfg(test)]
pub(super) fn recorded_identity(
    state: &AppState,
    runtime: &TaskRuntime,
) -> io::Result<ProcessIdentity> {
    let path = instance_path(state, runtime)?;
    let mut file = open_instance_file(&path)?;
    read_record(&mut file).map(|record| record.identity)
}

#[cfg(test)]
pub(super) fn remove_instance_dir(
    state: &AppState,
    runtime: &TaskRuntime,
) {
    if let Ok(path) = instance_path(state, runtime)
        && let Some(dir) = path.parent()
    {
        let _ = std::fs::remove_dir_all(dir);
    }
}
