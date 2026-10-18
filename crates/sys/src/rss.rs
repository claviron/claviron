//! Resident Set Size (RSS) lookup for any pid — own process or external.
//!
//! Single entry point: [`read_rss`].
//! - `pid = None`  → query the calling process. Linux uses
//!   `/proc/self/status`, FreeBSD uses `sysctl(KERN_PROC_PID, getpid())`,
//!   and macOS uses `proc_pid_rusage`.
//! - `pid = Some`  → query that process. Linux/FreeBSD same paths
//!   with the requested pid; macOS uses `proc_pid_rusage`.
//!
//! Linux-only deployments hit the `/proc` path; FreeBSD hits sysctl
//! with `libc::kinfo_proc`; macOS hits libproc. On
//! any other platform the call returns `None` and the caller should
//! treat the metric as unavailable.

#[cfg(target_os = "linux")]
use std::fs;

#[allow(unused_imports)]
use tracing::{debug, warn};

/// Read RSS in bytes for the given pid, or for the current process
/// when `pid` is `None`. Returns `None` on platforms / paths that
/// can't satisfy the request — caller decides what to do (omit
/// metric, fallback, etc.).
pub fn read_rss(pid: Option<u32>) -> Option<u64> {
    impl_read_rss(pid)
}

// ---------------------------------------------------------------------------
// Linux: /proc/<pid>/status :: VmRSS
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn impl_read_rss(pid: Option<u32>) -> Option<u64> {
    let path = match pid {
        Some(p) => format!("/proc/{}/status", p),
        None => String::from("/proc/self/status"),
    };
    let content = fs::read_to_string(&path).ok()?;
    for line in content.lines() {
        // Format: "VmRSS:    12345 kB"
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let mut parts = rest.split_whitespace();
            if let Some(kb_str) = parts.next()
                && let Ok(kb) = kb_str.parse::<u64>()
            {
                return Some(kb.saturating_mul(1024));
            }
            warn!(?pid, "failed to parse VmRSS line: {:?}", line);
            return None;
        }
    }
    None
}

// ---------------------------------------------------------------------------
// FreeBSD: sysctl(KERN_PROC_PID) → kinfo_proc.ki_rssize × _SC_PAGESIZE
// ---------------------------------------------------------------------------

#[cfg(target_os = "freebsd")]
fn impl_read_rss(pid: Option<u32>) -> Option<u64> {
    use std::mem;

    let target_pid = match pid {
        Some(p) => p as libc::c_int,
        None => unsafe { libc::getpid() },
    };

    let mut mib: [libc::c_int; 4] =
        [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_PID, target_pid];
    let mut kp: libc::kinfo_proc = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::kinfo_proc>();

    let rc = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            4,
            &mut kp as *mut _ as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };

    if rc != 0 || len == 0 {
        debug!(
            pid = target_pid,
            "sysctl KERN_PROC_PID failed or returned empty"
        );
        return None;
    }

    let pages = kp.ki_rssize as u64;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        warn!("sysconf(_SC_PAGESIZE) returned non-positive");
        return None;
    }
    Some(pages.saturating_mul(page_size as u64))
}

// ---------------------------------------------------------------------------
// macOS: proc_pid_rusage(RUSAGE_INFO_V2) — self or external pid.
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn impl_read_rss(pid: Option<u32>) -> Option<u64> {
    use std::mem::MaybeUninit;

    let target_pid = pid
        .map(|pid| pid as libc::c_int)
        .unwrap_or_else(|| unsafe { libc::getpid() });
    let mut info = MaybeUninit::<libc::rusage_info_v2>::uninit();
    let rc = unsafe {
        libc::proc_pid_rusage(
            target_pid,
            libc::RUSAGE_INFO_V2,
            info.as_mut_ptr().cast(),
        )
    };
    if rc != 0 {
        debug!(pid = target_pid, "proc_pid_rusage returned {rc}");
        return None;
    }

    Some(unsafe { info.assume_init() }.ri_resident_size)
}

// ---------------------------------------------------------------------------
// Other platforms (Windows et al): no implementation.
// ---------------------------------------------------------------------------

#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "macos"
)))]
fn impl_read_rss(_pid: Option<u32>) -> Option<u64> {
    None
}

#[cfg(test)]
#[path = "../tests/rss.rs"]
mod tests;
