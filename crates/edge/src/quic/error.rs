//! Classify normal transport closure without hiding protocol failures.

use h3::error::{ConnectionError, StreamError};
use h3::quic::ConnectionErrorIncoming;
use tracing::{debug, error};

fn normal_peer_close(error: &ConnectionErrorIncoming) -> bool {
    match error {
        ConnectionErrorIncoming::ApplicationClose { error_code } => {
            *error_code == 0
                || *error_code == h3::error::Code::H3_NO_ERROR.value()
        }
        ConnectionErrorIncoming::ConnectionClosed { error_code } => {
            *error_code == 0
        }
        _ => false,
    }
}

pub(super) fn normal_connection_close(error: &ConnectionError) -> bool {
    error.is_h3_no_error()
        || matches!(error, ConnectionError::Remote(remote, ..) if normal_peer_close(remote))
}

pub(super) fn log_stream_error(
    error: &StreamError,
    operation: &str,
) {
    let normal = error.is_h3_no_error()
        || matches!(error, StreamError::ConnectionError(connection, ..)
            if normal_connection_close(connection));
    if normal {
        debug!(
            ?error,
            operation, "H3 stream ended with normal connection closure"
        );
    } else {
        error!(?error, operation, "H3 stream operation failed");
    }
}

/// A closed connection or a peer-reset request is not malformed HTTP input.
pub(super) fn request_interrupted(error: &StreamError) -> bool {
    matches!(
        error,
        StreamError::ConnectionError(..) | StreamError::RemoteTerminate { .. }
    )
}

#[cfg(test)]
#[path = "../../tests/quic/error.rs"]
mod tests;
