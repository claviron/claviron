use std::io;

/// Meaning of an accept failure, without a retry or logging policy. An
/// accepted peer can disappear before accept; that does not break the listener.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptErrorClass {
    Transient,
    ResourcePressure,
    Fatal,
}

pub fn classify_accept_error(error: &io::Error) -> AcceptErrorClass {
    #[cfg(unix)]
    if matches!(
        error.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
    ) {
        return AcceptErrorClass::ResourcePressure;
    }

    // Linux forwards pending network errors from the new socket through
    // accept(2). They describe that peer, not failure of the listening socket.
    #[cfg(target_os = "linux")]
    if matches!(
        error.raw_os_error(),
        Some(
            libc::ENETDOWN
                | libc::EPROTO
                | libc::ENOPROTOOPT
                | libc::EHOSTDOWN
                | libc::ENONET
                | libc::EHOSTUNREACH
                | libc::EOPNOTSUPP
                | libc::ENETUNREACH
                | libc::EPERM
        )
    ) {
        return AcceptErrorClass::Transient;
    }

    match error.kind() {
        io::ErrorKind::Interrupted
        | io::ErrorKind::WouldBlock
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::TimedOut => AcceptErrorClass::Transient,
        io::ErrorKind::OutOfMemory => AcceptErrorClass::ResourcePressure,
        _ => AcceptErrorClass::Fatal,
    }
}

#[cfg(test)]
#[path = "../../tests/net/accept.rs"]
mod tests;
