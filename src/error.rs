use std::fmt::{Debug, Display};
use std::io::Error as IoError;

#[cfg(feature = "logging")]
use claviron_logging::Error as LoggingError;
#[cfg(feature = "tls")]
use claviron_tls::TlsError;
use serde_json::Error as SerdeError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO Error: {0}")]
    Io(#[from] IoError),

    #[error("Serialization failed: {0}")]
    Serde(#[from] SerdeError),

    #[cfg(feature = "server")]
    #[error(
        "server instance '{name}' already has a running master or standalone process"
    )]
    InstanceInUse { name: String },

    #[cfg(feature = "db")]
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error("Worker {0} {1}")]
    Worker(u8, String),

    #[cfg(feature = "http3")]
    #[error("Quinn Transport setup failed {0:?}")]
    Quinn(#[from] quinn::VarIntBoundsExceeded),

    #[cfg(feature = "tls")]
    #[error("Tls {0:?}")]
    Tls(#[from] TlsError),

    #[cfg(feature = "logging")]
    #[error("logging: {0}")]
    Logging(#[from] LoggingError),

    #[error("client failed: {0}")]
    Cli(String),

    #[error("Missing: {0}")]
    Missing(String),

    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),

    #[cfg(feature = "redis")]
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Rejected: {0}")]
    Rejected(String),

    #[error("Other: {0}")]
    Other(String),
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Other(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Other(s.to_string())
    }
}

impl Error {
    pub fn other<E: Display + Debug>(err: E) -> Self {
        Error::Other(format!("{err}"))
    }

    pub fn wrap<E: Display + Debug>(
        msg: impl Display,
        err: E,
    ) -> Self {
        Error::Other(format!("{msg}: {err}"))
    }
}

impl From<time::error::Format> for Error {
    fn from(e: time::error::Format) -> Self {
        Error::Other(e.to_string())
    }
}
#[cfg(feature = "server")]
/// Accept errors coming back from `claviron-runtime` registry so that `?`
/// in claviron code keeps working transparently.
impl From<claviron_runtime::registry::Error> for Error {
    fn from(e: claviron_runtime::registry::Error) -> Self {
        Error::Other(e.to_string())
    }
}

#[cfg(feature = "server")]
impl From<claviron_runtime::registry::BoxError> for Error {
    fn from(e: claviron_runtime::registry::BoxError) -> Self {
        Error::Other(e.to_string())
    }
}

#[cfg(feature = "server")]
impl From<crate::server::Error> for Error {
    fn from(value: crate::server::Error) -> Self {
        match value {
            crate::server::Error::Io(error) => Self::Io(error),
            crate::server::Error::InstanceInUse { name } => {
                Self::InstanceInUse { name }
            }
            crate::server::Error::Registry(error) => {
                Self::Other(error.to_string())
            }
            crate::server::Error::Logging(error) => Self::Logging(error),
            #[cfg(feature = "supervisor")]
            crate::server::Error::Supervisor(error) => {
                Self::Other(error.to_string())
            }
            error @ crate::server::Error::EnvFile { .. } => {
                Self::Other(error.to_string())
            }
            crate::server::Error::Other(error) => Self::Other(error),
        }
    }
}

#[cfg(feature = "cache")]
impl From<claviron_cache::Error> for Error {
    fn from(value: claviron_cache::Error) -> Self {
        Error::other(value)
    }
}

#[cfg(feature = "supervisor")]
impl From<claviron_supervisor::Error> for Error {
    fn from(value: claviron_supervisor::Error) -> Self {
        Error::other(value)
    }
}
