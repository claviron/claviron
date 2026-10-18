use claviron_runtime::WorkerId;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bootstrap error: {0}")]
    Bootstrap(claviron_runtime::registry::Error),

    #[error("worker #{number} error: {msg}")]
    Worker { number: WorkerId, msg: String },

    #[error("service error: {0}")]
    Service(String),
}

impl Error {
    pub(crate) fn bootstrap(msg: impl Into<String>) -> Self {
        Self::Bootstrap(claviron_runtime::registry::Error::msg(msg))
    }

    pub(crate) fn worker(
        number: WorkerId,
        msg: impl Into<String>,
    ) -> Self {
        Self::Worker { number, msg: msg.into() }
    }

    pub(crate) fn service(msg: impl Into<String>) -> Self {
        Self::Service(msg.into())
    }
}

#[cfg(test)]
#[path = "../tests/error.rs"]
mod tests;
