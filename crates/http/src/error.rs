use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use http::StatusCode;

pub type Result<T> = std::result::Result<T, Error>;

type BoxError = Box<dyn StdError + Send + Sync>;

/// Framework-neutral error returned at an HTTP application boundary.
///
/// The status, code, and message are safe to expose to a client. The
/// optional source is retained for diagnostics but is never rendered by
/// framework bindings.
#[derive(Debug)]
pub struct Error {
    status: StatusCode,
    code: &'static str,
    message: String,
    source: Option<BoxError>,
}

impl Error {
    pub fn new(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self { status, code, message: message.into(), source: None }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            message,
        )
    }

    /// Hide an internal error behind a stable, non-sensitive response.
    pub fn internal<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_server_error",
            message: "internal server error".to_string(),
            source: Some(Box::new(source)),
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for Error {
    fn fmt(
        &self,
        formatter: &mut Formatter<'_>,
    ) -> fmt::Result {
        match &self.source {
            Some(source) => write!(formatter, "{}: {source}", self.message),
            None => formatter.write_str(&self.message),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn StdError + 'static))
    }
}

#[cfg(test)]
#[path = "../tests/error.rs"]
mod tests;
