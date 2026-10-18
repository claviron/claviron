use std::path::PathBuf;

use http::StatusCode;

#[derive(thiserror::Error, Debug)]
pub enum SapiGatewayError {
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("php script not found: {}", .0.display())]
    ScriptNotFound(PathBuf),

    #[error("php sapi backend is not implemented yet")]
    NotImplemented,

    /// PHP-side accepted dispatch but failed to produce a usable
    /// gateway response (worker died, reply channel dropped, or
    /// streaming headers never committed).
    #[error("bad gateway: {0}")]
    BadGateway(String),

    /// PHP-side is reachable but cannot accept this request right now:
    /// pool saturated, draining for shutdown, or no execution slot
    /// became available within the dispatch queue timeout.
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl SapiGatewayError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            SapiGatewayError::BadRequest(_) => StatusCode::BAD_REQUEST,
            SapiGatewayError::ScriptNotFound(_) => StatusCode::NOT_FOUND,
            SapiGatewayError::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            SapiGatewayError::BadGateway(_) => StatusCode::BAD_GATEWAY,
            SapiGatewayError::ServiceUnavailable(_) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            SapiGatewayError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
