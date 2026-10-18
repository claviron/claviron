//! Host-local, non-blocking named process exclusion.

use std::fs::{self, File, OpenOptions, Permissions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// An exclusive OS lock held until this guard is dropped or the process exits.
///
/// Names share one local filesystem namespace, independent of UID, cwd,
/// configuration and `TMPDIR`. The backing file is intentionally retained:
/// unlinking a live lock would allow another process to lock a different inode.
/// File existence or contents are never evidence of a running process.
///
/// This is cooperative exclusion, not a security boundary or orphan-process
/// detector. Do not delete its files while any participating process is alive.
/// Separate container/private-tmp namespaces are separate lock domains.
/// Descriptors are close-on-exec; re-executed children do not retain ownership.
#[derive(Debug)]
#[must_use = "dropping the guard releases process exclusion"]
pub struct ProcessLock {
    _file: File,
}

impl ProcessLock {
    /// Try once, returning `WouldBlock` when this name is already locked.
    ///
    /// Uses a fixed host-local namespace on the supported Unix platforms.
    /// Unlike `temp_dir()`, this must not change with the caller's environment
    /// or user: doing so would silently allow duplicate named processes.
    pub fn try_acquire(name: &str) -> io::Result<Self> {
        Self::at_path(&lock_path(name)?)
    }

    fn at_path(path: &Path) -> io::Result<Self> {
        let flags = libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o644)
            .custom_flags(flags)
            .open(path)
        {
            Ok(file) => {
                // Empty rendezvous inode, not a secret or PID file. Permit
                // other OS users to participate even with a restrictive umask.
                file.set_permissions(Permissions::from_mode(0o644))?;
                file
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // flock on a local Unix filesystem needs no write permission.
                // Never truncate, chmod or write an existing caller-owned file.
                OpenOptions::new().read(true).custom_flags(flags).open(path)?
            }
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process lock must be a regular file with exactly one link",
            ));
        }

        loop {
            // SAFETY: file owns a valid descriptor throughout this call.
            if unsafe {
                libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB)
            } == 0
            {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }

        // Refuse replacement/unlink races observed during acquisition. The
        // namespace must remain intact for the entire guard lifetime.
        let named = fs::symlink_metadata(path)?;
        if !named.is_file()
            || named.nlink() != 1
            || named.dev() != metadata.dev()
            || named.ino() != metadata.ino()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process lock path changed",
            ));
        }
        Ok(Self { _file: file })
    }
}

fn lock_path(name: &str) -> io::Result<PathBuf> {
    if name.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process lock name is empty",
        ));
    }
    // Fixed-size stable encoding: names cannot escape the namespace, collide
    // through punctuation sanitization or exceed a filesystem component limit.
    let key = blake3::hash(name.as_bytes());
    Ok(Path::new("/tmp").join(format!("claviron-process-{key}.lock")))
}

#[cfg(test)]
#[path = "../../tests/process/lock.rs"]
mod tests;
