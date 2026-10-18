//! Owns the shared upstream failure vocabulary: why one request attempt did
//! not complete, and why the connection it used must not go back to its
//! pool. The pools themselves are `h1::pool` and `h2::pool`; every module
//! that can end an attempt — including `state`, `resolve` and `health` —
//! names its outcome from here rather than from the protocol that carried
//! it.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestFailure {
    Closed,
    ProfileStopped,
    AcquireTimeout,
    CapacityFull,
    BreakerOpen,
    ConnectTimeout,
    ConnectFailed,
    NoHealthyOrigin,
    RequestBodyReadIdleTimeout,
    RequestBodyFailed,
    RequestWriteFailed,
    ResponseHeaderTimeout,
    ResponseBodyIdleTimeout,
    ProtocolError,
}

impl RequestFailure {
    pub(crate) fn message(&self) -> &'static str {
        match self {
            Self::ProfileStopped => "upstream profile is stopped",
            Self::Closed => "upstream pool is closed",
            Self::BreakerOpen => "upstream circuit breaker is open",
            Self::CapacityFull => "upstream in-flight state counter is full",
            Self::AcquireTimeout => "timed out acquiring upstream connection",
            Self::ConnectTimeout => "timed out connecting to upstream",
            Self::ConnectFailed => "failed connecting to upstream",
            Self::NoHealthyOrigin => "no healthy upstream origin is available",
            Self::RequestBodyReadIdleTimeout => "incoming request body stalled",
            Self::RequestBodyFailed => "incoming request body failed",
            Self::RequestWriteFailed => "upstream request write failed",
            Self::ResponseHeaderTimeout => {
                "timed out waiting for upstream response headers"
            }
            Self::ResponseBodyIdleTimeout => "upstream response body stalled",
            Self::ProtocolError => "upstream protocol error",
        }
    }
}
impl std::fmt::Display for RequestFailure {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for RequestFailure {}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DirtyReason {
    ResponseBodyDroppedBeforeEof,
    ResponseBodyIdleTimeout,
    ResponseBodyError,
    OriginMarkedDown,
    Unknown,
}

impl DirtyReason {
    #[cfg(debug_assertions)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ResponseBodyDroppedBeforeEof => {
                "response_body_dropped_before_eof"
            }
            Self::ResponseBodyIdleTimeout => "response_body_idle_timeout",
            Self::ResponseBodyError => "response_body_error",
            Self::OriginMarkedDown => "origin_marked_down",
            Self::Unknown => "unknown",
        }
    }
}
