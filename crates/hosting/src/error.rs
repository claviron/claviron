pub type Result<T> = std::result::Result<T, HostingError>;

/// Storage-neutral failure returned by the public hosting API.
///
/// SQLx errors are rendered at this crate boundary rather than becoming part
/// of the public error shape.
#[derive(Debug, thiserror::Error)]
pub enum HostingError {
    #[error("invalid hosting input: {0}")]
    InvalidInput(String),
    #[error("hosting record not found: {0}")]
    NotFound(String),
    #[error("hosting record conflict: {0}")]
    Conflict(String),
    #[error("hosting storage failure: {0}")]
    Storage(String),
    #[error("hosting service is not initialized")]
    NotInitialized,
}

impl From<sqlx::Error> for HostingError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error.to_string())
    }
}
