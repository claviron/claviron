//! Unix group lookup by name, for files a process creates and hands to a
//! group it belongs to.

use std::ffi::CString;
use std::io;

/// The id of the group called `name`, from the system's group database
/// (`getgrnam_r`). A name the database does not know is `NotFound`.
pub fn group_id(name: &str) -> io::Result<u32> {
    let c_name = CString::new(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "a group name cannot contain a NUL byte",
        )
    })?;
    // The record's strings live in this buffer. The system's suggested size
    // is a starting point, not a bound: ERANGE asks for a larger buffer.
    let suggested = unsafe { libc::sysconf(libc::_SC_GETGR_R_SIZE_MAX) };
    let mut capacity = usize::try_from(suggested)
        .ok()
        .filter(|&size| size > 0)
        .unwrap_or(1024);
    loop {
        let mut buffer = vec![0 as libc::c_char; capacity];
        let mut group: libc::group = unsafe { std::mem::zeroed() };
        let mut found: *mut libc::group = std::ptr::null_mut();
        let status = unsafe {
            libc::getgrnam_r(
                c_name.as_ptr(),
                &mut group,
                buffer.as_mut_ptr(),
                buffer.len(),
                &mut found,
            )
        };
        if status == libc::ERANGE {
            capacity = capacity.checked_mul(2).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "group record too large",
                )
            })?;
            continue;
        }
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        if found.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no group named `{name}`"),
            ));
        }
        return Ok(group.gr_gid);
    }
}
