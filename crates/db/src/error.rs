#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("db config io: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(feature = "server")]
    #[error("db config: {0}")]
    Config(#[from] claviron_runtime::congif::Error),
    #[error("db config invalid: {0}")]
    Invalid(String),
    #[error("db pool: {0}")]
    Sqlx(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
