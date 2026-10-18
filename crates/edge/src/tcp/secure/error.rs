use std::fmt::Display;

use crate::transport::HostRejection;

#[derive(Debug)]
pub(crate) enum ConnectionError {
    ApplicationUnavailable,
    HostPolicyUnavailable,
    /// Hyper-level serving error
    Serve(hyper::Error),
    /// Timeout in http
    Timeout,
    /// Host policy requires ALPN but client did not negotiate one
    AlpnRequired,
    /// Negotiated/assumed protocol is not allowed by host policy
    ProtocolNotAllowed,
    /// SNI resolution error bubbled up from resolver
    InvalidSni(HostRejection),
}

impl From<hyper::Error> for ConnectionError {
    fn from(err: hyper::Error) -> Self {
        if err.is_timeout() { Self::Timeout } else { Self::Serve(err) }
    }
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
            ConnectionError::HostPolicyUnavailable => {
                f.write_str("host access policy is unavailable")
            }
            ConnectionError::Serve(err) => {
                write!(f, "Connection serve error: {}", err)
            }
            ConnectionError::AlpnRequired => {
                write!(f, "ALPN is required by host policy")
            }
            ConnectionError::ProtocolNotAllowed => {
                write!(f, "Protocol not allowed by host policy")
            }
            ConnectionError::InvalidSni(e) => {
                write!(f, "SNI resolve error: {}", e)
            }
            ConnectionError::Timeout => {
                write!(f, "Connection timeout")
            }
        }
    }
}
