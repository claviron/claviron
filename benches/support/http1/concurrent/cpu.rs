use std::{io, mem::MaybeUninit, time::Duration};

/// CPU consumed by all threads in this process, excluding child processes.
pub(super) fn process() -> io::Result<Duration> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied rusage on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful call above initialized every rusage field.
    let usage = unsafe { usage.assume_init() };
    let time = |value: libc::timeval| -> io::Result<Duration> {
        let seconds = u64::try_from(value.tv_sec).map_err(io::Error::other)?;
        let micros = u64::try_from(value.tv_usec).map_err(io::Error::other)?;
        Ok(Duration::from_secs(seconds) + Duration::from_micros(micros))
    };
    Ok(time(usage.ru_utime)? + time(usage.ru_stime)?)
}
