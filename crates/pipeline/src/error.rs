use std::fmt::Display;

/// Crate-wide error for vhost-resolver/data work that is not connection-bound.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("vhost config io: {0}")]
    Io(#[from] std::io::Error),
    #[error("vhost config parse: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("vhost config invalid: {0}")]
    Invalid(String),
    #[error("missing: {0}")]
    Missing(String),
    #[error("other: {0}")]
    Other(String),
}

impl Error {
    pub fn other<E: Display>(err: E) -> Self {
        Error::Other(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
