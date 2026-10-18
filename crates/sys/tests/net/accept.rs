use super::*;

#[test]
fn portable_accept_error_categories_do_not_hide_local_failures() {
    for kind in [
        io::ErrorKind::Interrupted,
        io::ErrorKind::WouldBlock,
        io::ErrorKind::ConnectionAborted,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::TimedOut,
    ] {
        assert_eq!(
            classify_accept_error(&kind.into()),
            AcceptErrorClass::Transient
        );
    }
    assert_eq!(
        classify_accept_error(&io::ErrorKind::OutOfMemory.into()),
        AcceptErrorClass::ResourcePressure
    );
    for kind in [
        io::ErrorKind::InvalidInput,
        io::ErrorKind::PermissionDenied,
        io::ErrorKind::Other,
    ] {
        assert_eq!(
            classify_accept_error(&kind.into()),
            AcceptErrorClass::Fatal
        );
    }
}

#[cfg(unix)]
#[test]
fn errno_classification_is_symbolic_and_does_not_exhaust_real_descriptors() {
    for errno in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
        assert_eq!(
            classify_accept_error(&io::Error::from_raw_os_error(errno)),
            AcceptErrorClass::ResourcePressure
        );
    }
    for errno in [libc::EBADF, libc::EINVAL, libc::ENOTSOCK] {
        assert_eq!(
            classify_accept_error(&io::Error::from_raw_os_error(errno)),
            AcceptErrorClass::Fatal
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn linux_pending_network_errors_are_transient() {
    for errno in [
        libc::ENETDOWN,
        libc::EPROTO,
        libc::ENOPROTOOPT,
        libc::EHOSTDOWN,
        libc::ENONET,
        libc::EHOSTUNREACH,
        libc::EOPNOTSUPP,
        libc::ENETUNREACH,
        libc::EPERM,
    ] {
        assert_eq!(
            classify_accept_error(&io::Error::from_raw_os_error(errno)),
            AcceptErrorClass::Transient
        );
    }
}
