//! Process resource limits.

/// The soft `RLIMIT_NOFILE` of the calling process, or `None` where the
/// platform has no such limit or the query failed.
///
/// The soft limit is the one that matters: it is what `accept(2)` fails
/// against, and raising it to the hard limit is the operator's move, not
/// ours. `RLIM_INFINITY` reports as `None` — "no limit" and "cannot tell" are
/// the same answer to every caller here.
#[cfg(unix)]
pub fn open_files() -> Option<u64> {
    // SAFETY: `getrlimit` writes into the `rlimit` we hand it and touches
    // nothing else. Zeroed is a valid initial value for the struct.
    let limit = unsafe {
        let mut limit: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return None;
        }
        limit
    };

    if limit.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }

    #[allow(
        clippy::useless_conversion,
        reason = "libc::rlim_t is platform-defined; retain a checked conversion into the public u64 API"
    )]
    u64::try_from(limit.rlim_cur).ok()
}

#[cfg(not(unix))]
pub fn open_files() -> Option<u64> {
    None
}
