use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("UTF-8 decode failed: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("certificate parsing failed: {0}")]
    X509Parser(#[from] x509_parser::error::X509Error),

    #[error("rcgen error: {0}")]
    RcGen(#[from] rcgen::Error),

    #[error("Invalid PEM encoded data: {0}")]
    PemDecode(#[from] x509_parser::error::PEMError),

    #[error("failed to parse hostname: {0}")]
    InvalidHostname(String),

    #[error("No domains provided")]
    NoDomainsProvided,

    #[error("{0} at {1}")]
    Exists(String, PathBuf),

    #[error("Txt format error: {0}")]
    Format(#[from] std::fmt::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Io error:  {source} (path: {path:?})")]
    Io { source: std::io::Error, path: PathBuf },

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
    pub fn io_error(
        path: impl AsRef<Path>,
        e: std::io::Error,
    ) -> Self {
        Self::Io { source: e, path: path.as_ref().to_path_buf() }
    }
}
