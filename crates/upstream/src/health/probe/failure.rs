//! Allocation-free owned failure facts; origin and raw errors log at the source.
use std::{fmt, io};

use super::message::has_early_eof;
use crate::config::profile::UpstreamProtocol;
use crate::proxy::failure::RequestFailure;
#[derive(Clone, Debug, PartialEq, Eq)]
enum Cause {
    ConnectTimeout,
    Io { kind: io::ErrorKind, raw_os_error: Option<i32>, early_eof: bool },
    Handshake(UpstreamProtocol),
    H2SenderNotReady,
    H2StreamCapacityTimeout,
    Tls(TlsCause),
}
impl Cause {
    fn as_str(&self) -> &'static str {
        match self {
            Self::ConnectTimeout => "connect_timeout",
            Self::Handshake(_) => "handshake",
            Self::H2SenderNotReady => "h2_sender_not_ready",
            Self::H2StreamCapacityTimeout => "h2_stream_capacity_timeout",
            Self::Tls(cause) => cause.as_str(),
            Self::Io { kind, early_eof, .. } => match kind {
                io::ErrorKind::ConnectionRefused => "connection_refused",
                io::ErrorKind::ConnectionReset => "connection_reset",
                io::ErrorKind::TimedOut => "connect_timeout",
                io::ErrorKind::UnexpectedEof => "unexpected_eof",
                io::ErrorKind::NotConnected => "not_connected",
                io::ErrorKind::BrokenPipe => "broken_pipe",
                io::ErrorKind::NotFound => "not_found",
                io::ErrorKind::PermissionDenied => "permission_denied",
                io::ErrorKind::HostUnreachable => "host_unreachable",
                io::ErrorKind::NetworkUnreachable => "network_unreachable",
                _ if *early_eof => "early_eof",
                _ => "io_error",
            },
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OriginReachabilityFailure {
    cause: Cause,
    error: RequestFailure,
}
impl OriginReachabilityFailure {
    pub(crate) fn connect_timeout() -> Self {
        Self {
            cause: Cause::ConnectTimeout,
            error: RequestFailure::ConnectTimeout,
        }
    }
    pub(crate) fn connect_io(error: &io::Error) -> Self {
        let early_eof =
            is_origin_reachability_io(error) && !known_origin_io(error.kind());
        Self {
            cause: Cause::Io {
                kind: error.kind(),
                raw_os_error: error.raw_os_error(),
                early_eof,
            },
            error: RequestFailure::ConnectFailed,
        }
    }
    pub(crate) fn handshake(protocol: UpstreamProtocol) -> Self {
        Self {
            cause: Cause::Handshake(protocol),
            error: RequestFailure::ProtocolError,
        }
    }
    pub(crate) fn h2_sender_not_ready() -> Self {
        Self {
            cause: Cause::H2SenderNotReady,
            error: RequestFailure::ProtocolError,
        }
    }
    pub(crate) fn h2_stream_capacity_timeout() -> Self {
        Self {
            cause: Cause::H2StreamCapacityTimeout,
            error: RequestFailure::ResponseHeaderTimeout,
        }
    }
    pub(crate) fn tls(cause: TlsCause) -> Self {
        Self { cause: Cause::Tls(cause), error: RequestFailure::ProtocolError }
    }
    pub(crate) fn retry_cause(
        &self
    ) -> Option<crate::config::profile::retry::RetryCause> {
        use crate::config::profile::retry::RetryCause;
        match self.cause {
            Cause::Io { kind: io::ErrorKind::ConnectionRefused, .. } => {
                Some(RetryCause::Refused)
            }
            Cause::Io { .. } | Cause::ConnectTimeout => {
                Some(RetryCause::Connect)
            }
            _ => None,
        }
    }
    pub(crate) fn cause(&self) -> &'static str {
        self.cause.as_str()
    }
    pub(crate) fn request_failure(&self) -> RequestFailure {
        self.error.clone()
    }
}
impl fmt::Display for OriginReachabilityFailure {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match &self.cause {
            Cause::Io { kind, raw_os_error, .. } => {
                write!(
                    f,
                    "{} (kind={kind:?}, os_error={raw_os_error:?})",
                    self.cause()
                )
            }
            Cause::Handshake(protocol) => {
                write!(f, "upstream {protocol:?} handshake failed")
            }
            _ => f.write_str(self.cause()),
        }
    }
}
fn known_origin_io(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::NotConnected
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotFound
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
    )
}
pub(crate) fn is_origin_reachability_io(error: &io::Error) -> bool {
    known_origin_io(error.kind()) || has_early_eof(error)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TlsCause {
    Handshake,
    Certificate,
    AlpnMismatch,
    Timeout,
}
impl TlsCause {
    fn as_str(self) -> &'static str {
        match self {
            Self::Handshake => "tls_handshake",
            Self::Certificate => "tls_certificate",
            Self::AlpnMismatch => "alpn_mismatch",
            Self::Timeout => "handshake_timeout",
        }
    }
}
