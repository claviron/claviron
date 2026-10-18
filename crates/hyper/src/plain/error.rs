//! Local error type for cleartext handling. Bootstrap collects these
//! into the top-level `claviron::Error` — propagating with `?` is fine.

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("plaintext HTTP server: {0}")]
    Server(String),

    #[error("plaintext HTTP rejected: {0}")]
    Rejected(&'static str),

    #[error("plaintext HTTP: {0}")]
    Other(String),

    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<claviron_runtime::gate::Error> for Error {
    fn from(e: claviron_runtime::gate::Error) -> Self {
        // claviron_runtime::gate::Error doesn't implement Display.
        Error::Server(format!("{e:?}"))
    }
}

impl From<claviron_runtime::BoxError> for Error {
    fn from(e: claviron_runtime::BoxError) -> Self {
        Error::Server(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
