//! Host identity reported by the operating system.

use std::ffi::{OsStr, OsString};

/// Return the kernel hostname without its DNS suffix.
///
/// The fallback remains a stable, non-empty value so boot diagnostics and
/// host-specific configuration lookup can share one primitive.
pub fn short_hostname() -> String {
    let hostname =
        ::hostname::get().unwrap_or_else(|_| OsString::from("unknown"));
    short_hostname_from(&hostname)
}

fn short_hostname_from(hostname: &OsStr) -> String {
    hostname
        .to_string_lossy()
        .split('.')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_owned()
}

#[cfg(test)]
#[path = "../tests/hostname.rs"]
mod tests;
