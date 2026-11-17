#[cfg(feature = "axum")]
use axum::response::{IntoResponse, Response};
#[cfg(feature = "axum")]
use serde_json::json;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cache io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("cache config parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[cfg(feature = "redis")]
    #[error("cache redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("cache file storage error: {0}")]
    Storage(#[from] cacache::Error),

    #[error("cache missing: {0}")]
    Missing(String),

    #[error("cache rejected: {0}")]
    Rejected(String),

    /// A tagged operation on a store whose
    /// [`CacheRepository::supports_tags`](crate::CacheRepository::supports_tags)
    /// is false. Its untagged operations are unaffected.
    #[error("cache store '{store}' (driver '{driver}') does not support tags")]
    TagsUnsupported { store: Box<str>, driver: Box<str> },

    /// More tags than the store keeps on one entry or names in one
    /// invalidation. `max` can be below the store's general bound when a long
    /// key leaves less room; nothing was written.
    #[error("too many cache tags: {count} > {max}")]
    TooManyTags { count: usize, max: usize },

    /// A tag invalidation must name at least one tag.
    #[error("a cache tag invalidation must name at least one tag")]
    TagsEmpty,

    #[error("cache other: {0}")]
    Other(String),
}

impl Error {
    pub fn other(err: impl std::fmt::Display) -> Self {
        Self::Other(err.to_string())
    }

    pub fn wrap(
        msg: impl std::fmt::Display,
        err: impl std::fmt::Display,
    ) -> Self {
        Self::Other(format!("{msg}: {err}"))
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::Other(value)
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Self::Other(value.to_string())
    }
}

#[cfg(feature = "orbit")]
impl From<claviron_orbit::cache::Error> for Error {
    fn from(value: claviron_orbit::cache::Error) -> Self {
        use claviron_orbit::cache::Error as Orbit;
        match value {
            Orbit::TooManyTags { count, max } => {
                Self::TooManyTags { count, max }
            }
            Orbit::TagsEmpty => Self::TagsEmpty,
            Orbit::TagsUnsupported { store, .. } => {
                Self::TagsUnsupported { store, driver: "orbit".into() }
            }
            other => Self::wrap("Orbit cache operation failed", other),
        }
    }
}

impl From<crate::ValueError> for Error {
    fn from(value: crate::ValueError) -> Self {
        Self::wrap("cache value operation failed", value)
    }
}

#[cfg(feature = "axum")]
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Error::Missing(message) => {
                (axum::http::StatusCode::NOT_FOUND, json!({ "error": message }))
            }
            Error::Rejected(message) => (
                axum::http::StatusCode::NOT_IMPLEMENTED,
                json!({ "error": message }),
            ),
            error @ Error::TagsUnsupported { .. } => (
                axum::http::StatusCode::NOT_IMPLEMENTED,
                json!({ "error": error.to_string() }),
            ),
            other => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": other.to_string() }),
            ),
        };

        (status, body.to_string()).into_response()
    }
}
