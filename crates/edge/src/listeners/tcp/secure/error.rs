use std::fmt::Display;

use crate::transport::HostRejection;

/// Why the selected host refused a connection the handshake had accepted.
#[derive(Debug)]
pub(crate) enum ConnectionError {
    ApplicationUnavailable,
    /// The SNI host is not served on this listener.
    InvalidSni(HostRejection),
}

impl From<HostRejection> for ConnectionError {
    fn from(err: HostRejection) -> Self {
        ConnectionError::InvalidSni(err)
    }
}

impl Display for ConnectionError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            ConnectionError::ApplicationUnavailable => {
                f.write_str("host application is unavailable")
            }
            ConnectionError::InvalidSni(e) => {
                write!(f, "SNI resolve error: {}", e)
            }
        }
    }
}
