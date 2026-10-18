use std::fmt::{Debug, Display};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),

    #[error(
        "server instance '{name}' already has a running master or standalone process"
    )]
    InstanceInUse { name: String },

    #[error("registry: {0}")]
    Registry(claviron_runtime::registry::Error),

    #[error("logging: {0}")]
    Logging(#[from] claviron_logging::Error),

    #[cfg(feature = "supervisor")]
    #[error("supervisor: {0}")]
    Supervisor(#[from] claviron_supervisor::Error),

    /// An environment file that exists but could not be read or parsed.
    #[error("env file {}: {reason}", path.display())]
    EnvFile { path: std::path::PathBuf, reason: String },

    #[error("server: {0}")]
    Other(String),
}

impl Error {
    pub fn other<E: Display + Debug>(error: E) -> Self {
        Self::Other(error.to_string())
    }
}

impl From<claviron_runtime::registry::Error> for Error {
    fn from(error: claviron_runtime::registry::Error) -> Self {
        Self::Registry(error)
    }
}
