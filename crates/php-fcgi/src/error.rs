use std::io;

use http::StatusCode;

#[derive(thiserror::Error, Debug)]
pub enum GatewayError {
    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Execute(#[from] ExecuteError),
}

impl GatewayError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            GatewayError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            GatewayError::Execute(e) => e.status_code(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("pool error ({0:?}) : {1}")]
pub struct ConnectError(pub Connection, #[source] pub io::Error);

impl ConnectError {
    /// Construct an FCGI-ping failure (post-connect handshake validation).
    /// Surfaces as `Connection::Ping` so callers can distinguish "TCP/Unix
    /// dial failed" from "FPM end accepted the socket but isn't actually
    /// responding to a ping".
    pub fn ping(err: io::Error) -> Self {
        ConnectError(Connection::Ping, err)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {
    #[error("fastcgi client error: {0}")]
    Client(#[from] fastcgi_client::ClientError),

    #[error("wrong fastcgi service identifier")]
    Missing,

    #[error("fastcgi timeout while {0}")]
    Timeout(&'static str), // e.g. "connecting" / "executing"

    #[error(transparent)]
    Connect(#[from] ConnectError),
}

impl ExecuteError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            ExecuteError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            ExecuteError::Connect(e) => match e.1.kind() {
                io::ErrorKind::TimedOut => StatusCode::GATEWAY_TIMEOUT,
                io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::NotConnected
                | io::ErrorKind::AddrInUse
                | io::ErrorKind::AddrNotAvailable
                | io::ErrorKind::NotFound => StatusCode::BAD_GATEWAY,
                _ => StatusCode::BAD_GATEWAY,
            },
            ExecuteError::Client(_) | ExecuteError::Missing => {
                StatusCode::BAD_GATEWAY
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Connection {
    Tcp,
    Unix,
    /// FCGI ping handshake failure: socket connected but FPM didn't
    /// return a valid `pong` for the configured `ping.path`.
    Ping,
}
