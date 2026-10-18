//! Owns error policy and runtime helpers.

use bytes::Bytes;
use claviron_http::session::SessionSpawnError;
use claviron_http::{RouteResponse, bytes_body};
use http::{Response, StatusCode};

use crate::proxy::failure::RequestFailure;

/// Inbound lifecycle rejection is not a pool failure or an origin health signal.
pub(crate) fn session_error_response(
    error: SessionSpawnError
) -> RouteResponse {
    error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        match error {
            SessionSpawnError::ListenerClosed => {
                "native session listener is closed"
            }
            SessionSpawnError::ConnectionClosed => {
                "native session connection is closed"
            }
            SessionSpawnError::Draining => {
                "native session listener is draining"
            }
        },
    )
}

pub(crate) fn error_response(
    status: StatusCode,
    message: impl Into<Bytes>,
) -> RouteResponse {
    let message = message.into();
    tracing::warn!(error = %String::from_utf8_lossy(&message), "http upstream request failed");
    Response::builder()
        .status(status)
        .body(bytes_body(message))
        .expect("static upstream error response is valid")
}

pub(crate) fn failure_response(error: RequestFailure) -> RouteResponse {
    let status = match error {
        RequestFailure::Closed
        | RequestFailure::ProfileStopped
        | RequestFailure::AcquireTimeout
        | RequestFailure::CapacityFull
        | RequestFailure::BreakerOpen => StatusCode::SERVICE_UNAVAILABLE,
        RequestFailure::NoHealthyOrigin => StatusCode::SERVICE_UNAVAILABLE,
        RequestFailure::ConnectTimeout
        | RequestFailure::ResponseHeaderTimeout
        | RequestFailure::ResponseBodyIdleTimeout => {
            StatusCode::GATEWAY_TIMEOUT
        }
        RequestFailure::ConnectFailed
        | RequestFailure::RequestBodyReadIdleTimeout
        | RequestFailure::RequestBodyFailed
        | RequestFailure::RequestWriteFailed
        | RequestFailure::ProtocolError => StatusCode::BAD_GATEWAY,
    };
    error_response(status, Bytes::from_static(error.message().as_bytes()))
}
