//! Stable process identity, async liveness handles and named process exclusion.
//!
//! A PID alone is not an identity: kernels reuse numeric PIDs. This module
//! pairs the PID with the process start stamp exposed by the host kernel, then
//! opens the strongest handle available for waiting and signaling.

use std::io;

mod lock;
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(not(target_os = "freebsd"))]
use std::time::Duration;

pub use lock::ProcessLock;
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use tokio::io::unix::AsyncFd;

/// LEGACY_LINUX (Linux < 5.3, no pidfd) — and macOS.
///
/// How often a handle without a process descriptor looks again for its
/// process's exit: on macOS always; on Linux kernels older than
/// 5.3, which have no `pidfd_open` (AlmaLinux 8's 4.18 among them), only for
/// a process that is not this one's child — a child's exit arrives as
/// `SIGCHLD` there, and nothing arrives for anyone else's.
#[cfg(not(target_os = "freebsd"))]
const FALLBACK_PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// PID plus a kernel-sourced process start stamp, safe against PID reuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProcessIdentity {
    pid: u32,
    started_at: u64,
}

impl ProcessIdentity {
    pub const fn new(
        pid: u32,
        started_at: u64,
    ) -> Self {
        Self { pid, started_at }
    }

    pub const fn pid(self) -> u32 {
        self.pid
    }

    pub const fn started_at(self) -> u64 {
        self.started_at
    }

    /// Whether this exact process incarnation is still alive.
    pub fn is_alive(self) -> bool {
        identity(self.pid) == Some(self)
    }
}

/// Query one process's stable identity from the host kernel.
pub fn identity(pid: u32) -> Option<ProcessIdentity> {
    impl_identity(pid)
}

/// An asynchronously waitable handle to a process that is not necessarily a
/// child of the current Claviron incarnation.
pub struct ProcessHandle {
    identity: ProcessIdentity,
    /// LEGACY_LINUX: `None` below Linux 5.3.
    ///
    /// The process descriptor, where the kernel has one. `None` on a Linux
    /// kernel older than 5.3: the handle then waits and signals the way it
    /// does on macOS.
    #[cfg(target_os = "linux")]
    notifier: Option<AsyncFd<OwnedFd>>,
    /// An unread NOTE_EXIT event remains ready for every waiter until drop.
    #[cfg(target_os = "freebsd")]
    notifier: AsyncFd<OwnedFd>,
}

impl ProcessHandle {
    /// Attach to an existing process without relying on parent/child ancestry.
    pub fn open(identity: ProcessIdentity) -> io::Result<Self> {
        if !identity.is_alive() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "process identity is not alive",
            ));
        }

        #[cfg(target_os = "linux")]
        {
            let raw = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_open,
                    identity.pid as libc::pid_t,
                    0,
                )
            };
            if raw == -1 {
                let error = io::Error::last_os_error();
                // LEGACY_LINUX: no `pidfd_open` before Linux 5.3.
                if error.raw_os_error() == Some(libc::ENOSYS) {
                    #[cfg(feature = "legacy-linux")]
                    {
                        warn_legacy_linux();
                        return Ok(Self { identity, notifier: None });
                    }
                    #[cfg(not(feature = "legacy-linux"))]
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "this kernel has no pidfd_open (Linux < 5.3); build with the \
                         `legacy-linux` feature to run on it",
                    ));
                }
                return Err(error);
            }
            let owned = unsafe { OwnedFd::from_raw_fd(raw as libc::c_int) };
            // SAFETY: OwnedFd owns this pidfd until drop. ProcessHandle only
            // borrows it for readiness/signaling and never replaces it.
            let notifier =
                unsafe { AsyncFd::register(owned) }.map_err(io::Error::from)?;
            if !identity.is_alive() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "process exited while opening pidfd",
                ));
            }
            return Ok(Self { identity, notifier: Some(notifier) });
        }

        #[cfg(target_os = "freebsd")]
        {
            let notifier = freebsd_exit_notifier(identity)?;
            if !identity.is_alive() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "process exited while registering its exit event",
                ));
            }
            Ok(Self { identity, notifier })
        }

        #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
        Ok(Self { identity })
    }

    pub const fn identity(&self) -> ProcessIdentity {
        self.identity
    }

    /// Wait until this process incarnation exits.
    pub async fn wait(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(notifier) = &self.notifier {
            return wait_for_exit_ready(notifier).await;
        }

        #[cfg(target_os = "freebsd")]
        return wait_for_exit_ready(&self.notifier).await;

        // LEGACY_LINUX: no descriptor, but a child of ours — the kernel says
        // when it exits.
        #[cfg(all(target_os = "linux", feature = "legacy-linux"))]
        if is_child_of_this_process(self.identity.pid) {
            return wait_for_child(self.identity).await;
        }

        #[cfg(not(target_os = "freebsd"))]
        {
            let mut ticker =
                tokio::time::interval(FALLBACK_PROCESS_POLL_INTERVAL);
            ticker.set_missed_tick_behavior(
                tokio::time::MissedTickBehavior::Skip,
            );
            loop {
                ticker.tick().await;
                if has_exited(self.identity) {
                    return Ok(());
                }
            }
        }
    }

    /// Send a Unix signal after validating the process incarnation.
    ///
    /// Linux uses `pidfd_send_signal`, which is immune to PID reuse. Platforms
    /// without a process descriptor validate the start stamp immediately
    /// before `kill(2)` but retain the kernel API's small check-to-signal race.
    pub fn signal(
        &self,
        signal: libc::c_int,
    ) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(notifier) = &self.notifier {
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    notifier.get_ref().as_raw_fd(),
                    signal,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            };
            if rc == -1 {
                return Err(io::Error::last_os_error());
            }
            return Ok(());
        }

        if !self.identity.is_alive() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "process identity is not alive",
            ));
        }
        let rc =
            unsafe { libc::kill(self.identity.pid as libc::pid_t, signal) };
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(target_os = "freebsd")]
fn freebsd_exit_notifier(
    identity: ProcessIdentity
) -> io::Result<AsyncFd<OwnedFd>> {
    let raw = unsafe { libc::kqueue() };
    if raw == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: kqueue returned a new descriptor, owned here until drop.
    let owned = unsafe { OwnedFd::from_raw_fd(raw) };
    if unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: all-zero kevent is valid before setting the registration fields.
    let mut event: libc::kevent = unsafe { std::mem::zeroed() };
    event.ident = identity.pid as libc::uintptr_t;
    event.filter = libc::EVFILT_PROC;
    event.flags = libc::EV_ADD | libc::EV_ONESHOT;
    event.fflags = libc::NOTE_EXIT;
    // No event buffer: this registers without waiting or consuming an exit.
    let result = unsafe {
        libc::kevent(raw, &event, 1, std::ptr::null_mut(), 0, std::ptr::null())
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the descriptor remains owned by AsyncFd. Only NOTE_EXIT can
    // make this queue readable; waiters never consume its terminal event.
    unsafe {
        AsyncFd::register_with_interest(owned, tokio::io::Interest::READABLE)
    }
    .map_err(io::Error::from)
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
async fn wait_for_exit_ready(notifier: &AsyncFd<OwnedFd>) -> io::Result<()> {
    loop {
        let mut ready = notifier.readable().await?;
        match process_exit_ready(notifier.get_ref().as_raw_fd()) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                ready.clear_ready();
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn process_exit_ready(raw_fd: libc::c_int) -> io::Result<()> {
    let mut poll_fd =
        libc::pollfd { fd: raw_fd, events: libc::POLLIN, revents: 0 };
    let result = unsafe { libc::poll(&mut poll_fd, 1, 0) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    if result == 0 || poll_fd.revents & libc::POLLIN == 0 {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }
    Ok(())
}

/// LEGACY_LINUX (Linux < 5.3): said once per process, the first time a
/// process handle finds no `pidfd_open`.
#[cfg(all(target_os = "linux", feature = "legacy-linux"))]
fn warn_legacy_linux() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        tracing::warn!(
            "LEGACY_LINUX: this kernel has no pidfd_open (Linux < 5.3); child exits are \
             observed through SIGCHLD, adopted processes by polling, and signals are \
             sent by pid after an identity check"
        );
    });
}

/// LEGACY_LINUX (Linux < 5.3).
///
/// Wait for a child of this process to exit, on a Linux kernel without
/// `pidfd_open`.
///
/// The kernel sends `SIGCHLD` when a child exits, so the wait is woken by the
/// exit itself rather than by a clock. Signals coalesce and any child's exit
/// wakes every waiter, so each wake looks again at this child alone. The
/// listener is made before the first look: an exit between the two would
/// otherwise leave nothing to wake it.
#[cfg(all(target_os = "linux", feature = "legacy-linux"))]
async fn wait_for_child(identity: ProcessIdentity) -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut children = signal(SignalKind::child())?;
    loop {
        if has_exited(identity) {
            return Ok(());
        }
        if children.recv().await.is_none() {
            return Err(io::Error::other("SIGCHLD listener closed"));
        }
    }
}

/// LEGACY_LINUX (Linux < 5.3).
///
/// Whether `pid` is a child of this process: only then does its exit reach
/// this process as `SIGCHLD`. A worker adopted from an earlier master is not.
#[cfg(all(target_os = "linux", feature = "legacy-linux"))]
fn is_child_of_this_process(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    // Field 4, the parent's pid: token 1 after the command's closing `)`.
    stat.rfind(')')
        .and_then(|end| stat.get(end + 1..))
        .and_then(|rest| rest.split_whitespace().nth(1))
        .and_then(|ppid| ppid.parse::<u32>().ok())
        == Some(std::process::id())
}

/// LEGACY_LINUX (Linux < 5.3). Also the ordinary path on macOS.
///
/// Whether this process incarnation has ended, for a handle without a
/// process descriptor.
///
/// On Linux a process that has exited but not been reaped keeps its `/proc`
/// entry and its start stamp, so identity alone would call a dead child
/// alive until its parent reaps it — and its parent is the one waiting here
/// before reaping. A zombie has ended; it is waiting only to be collected.
#[cfg(not(target_os = "freebsd"))]
fn has_exited(identity: ProcessIdentity) -> bool {
    #[cfg(all(target_os = "linux", feature = "legacy-linux"))]
    {
        let Ok(stat) =
            std::fs::read_to_string(format!("/proc/{}/stat", identity.pid))
        else {
            return true;
        };
        let Some(fields) = stat
            .rfind(')')
            .and_then(|end| stat.get(end + 1..))
            .map(|rest| rest.split_whitespace().collect::<Vec<_>>())
        else {
            return true;
        };
        let started_at =
            fields.get(19).and_then(|field| field.parse::<u64>().ok());
        if started_at != Some(identity.started_at) {
            return true;
        }
        return matches!(fields.first(), Some(&"Z") | Some(&"X"));
    }

    #[cfg(not(all(target_os = "linux", feature = "legacy-linux")))]
    !identity.is_alive()
}

#[cfg(target_os = "linux")]
fn impl_identity(pid: u32) -> Option<ProcessIdentity> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // `comm` is parenthesized and may contain spaces or `)`, so split after
    // its final closing parenthesis. The remaining token 0 is field 3; the
    // process start ticks are field 22, hence index 19.
    let fields = stat
        .get(stat.rfind(')')? + 1..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    let started_at = fields.get(19)?.parse().ok()?;
    Some(ProcessIdentity::new(pid, started_at))
}

#[cfg(target_os = "freebsd")]
fn impl_identity(pid: u32) -> Option<ProcessIdentity> {
    use std::mem;

    let mut mib: [libc::c_int; 4] = [
        libc::CTL_KERN,
        libc::KERN_PROC,
        libc::KERN_PROC_PID,
        pid as libc::c_int,
    ];
    let mut process: libc::kinfo_proc = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::kinfo_proc>();
    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            4,
            &mut process as *mut _ as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0
        || len != mem::size_of::<libc::kinfo_proc>()
        || process.ki_stat == libc::SZOMB
    {
        return None;
    }
    let seconds = u64::try_from(process.ki_start.tv_sec).ok()?;
    let micros = u64::try_from(process.ki_start.tv_usec).ok()?;
    Some(ProcessIdentity::new(
        pid,
        seconds.saturating_mul(1_000_000).saturating_add(micros),
    ))
}

#[cfg(target_os = "macos")]
fn impl_identity(pid: u32) -> Option<ProcessIdentity> {
    use std::mem::{self, MaybeUninit};

    let mut info = MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let read = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if read != size {
        return None;
    }
    let info = unsafe { info.assume_init() };
    Some(ProcessIdentity::new(
        pid,
        info.pbi_start_tvsec
            .saturating_mul(1_000_000)
            .saturating_add(info.pbi_start_tvusec),
    ))
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "macos"
)))]
fn impl_identity(_pid: u32) -> Option<ProcessIdentity> {
    None
}

#[cfg(test)]
#[path = "../tests/process.rs"]
mod tests;
