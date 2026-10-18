pub type Result<T> = std::result::Result<T, Error>;

use std::path::{Path, PathBuf};

use claviron_types::ssl::SSLError;
#[cfg(feature = "db")]
use x509_parser::error::X509Error;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("TLS build failed: {0}")]
    Build(#[from] rustls::Error),

    #[error("client verifier build failed: {0}")]
    ServerVerifier(#[source] rustls::server::VerifierBuilderError),

    #[error("certificate error: {path}")]
    Certificate {
        path: PathBuf,
        #[source]
        source: rustls::pki_types::pem::Error,
    },

    #[error("private key parser error: {path}")]
    Key {
        path: PathBuf,
        #[source]
        source: rustls::pki_types::pem::Error,
    },

    #[error("Signing key error: {message}  err: {source}")]
    SigningKey {
        message: String,
        #[source]
        source: rustls::Error,
    },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("No private key for cert '{0}'")]
    NoKey(String),

    #[error(
        "Missing default host configuration: please include a hosts.default key in your config."
    )]
    NoDefaultCert,

    #[cfg(feature = "db")]
    #[error("failed to extract certs from db for host {cn} error: {source}")]
    DbCerts { cn: String, source: X509Error },

    #[error("failed to extract certificates from path: {path} error: {source}")]
    CertExtract {
        path: PathBuf,
        #[source]
        source: SSLError,
    },

    #[error(transparent)]
    Ssl(#[from] SSLError),

    #[error("Tls Other: {0}")]
    Other(String),
}

impl Error {
    pub fn extract_failed(
        path: &Path,
        source: SSLError,
    ) -> Error {
        Error::CertExtract { path: path.to_path_buf(), source }
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Error::Other(message)
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Error::Other(message.to_string())
    }
}
