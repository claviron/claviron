pub type TaskResult<T> = Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Reason {
    /// Explicitly cancelled by the runner/service (e.g., reload/remove)
    Cancelled,

    /// Global shutdown requested
    Shutdown,

    /// Graceful shutdown timeout exceeded
    GracefulTimeout,

    /// A bounded execution exceeded its configured or requested timeout
    ExecutionTimeout,

    /// Custom static reason label
    Custom(&'static str),
}

impl std::fmt::Display for Reason {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Reason::Cancelled => write!(f, "❎ cancelled"),
            Reason::Shutdown => write!(f, "❎ shutdown"),
            Reason::GracefulTimeout => {
                write!(f, "‼️ graceful timeout ‼️")
            }
            Reason::ExecutionTimeout => write!(f, "‼️ execution timeout ‼️"),
            Reason::Custom(label) => write!(f, "⚠️  {}", label),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Retryable I/O error
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Non-retryable: caller asked to stop without retrying
    #[error("stopped: {0:?}")]
    Exit(Reason),

    #[error("error: {0}")]
    Other(String),
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Error::Other(err.to_string())
    }
}

impl Error {
    #[inline]
    pub fn stopped(reason: Reason) -> Self {
        Error::Exit(reason)
    }

    #[inline]
    pub fn other<S: Into<String>>(msg: S) -> Self {
        Error::Other(msg.into())
    }
}
