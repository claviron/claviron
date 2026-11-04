use std::{io, mem::MaybeUninit, time::Duration};

use serde::{Deserialize, Serialize};

/// What all threads of this process consumed, excluding child processes.
///
/// User and system time stay separate: a difference in user time is the
/// serving code, a difference in system time is how it uses the kernel.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct Usage {
    pub(super) user_ns: u64,
    pub(super) system_ns: u64,
}

impl Usage {
    pub(super) fn process() -> io::Result<Self> {
        let mut usage = MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the supplied rusage on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) }
            != 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful call above initialized every rusage field.
        let usage = unsafe { usage.assume_init() };
        let time = |value: libc::timeval| -> io::Result<u64> {
            let seconds =
                u64::try_from(value.tv_sec).map_err(io::Error::other)?;
            let micros =
                u64::try_from(value.tv_usec).map_err(io::Error::other)?;
            let elapsed =
                Duration::from_secs(seconds) + Duration::from_micros(micros);
            u64::try_from(elapsed.as_nanos()).map_err(io::Error::other)
        };
        Ok(Self {
            user_ns: time(usage.ru_utime)?,
            system_ns: time(usage.ru_stime)?,
        })
    }

    /// What this process consumed after `earlier` was read.
    pub(super) fn since(
        self,
        earlier: Self,
    ) -> io::Result<Self> {
        let elapsed = |later: u64, earlier: u64| {
            later.checked_sub(earlier).ok_or_else(|| {
                io::Error::other("a process usage counter moved backwards")
            })
        };
        Ok(Self {
            user_ns: elapsed(self.user_ns, earlier.user_ns)?,
            system_ns: elapsed(self.system_ns, earlier.system_ns)?,
        })
    }
}
