use std::error::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ResolveErrorKind {
    #[error("invalid resolver config")]
    Config,
    #[error("invalid DNS name")]
    InvalidName,
    #[error("DNS name does not exist")]
    NxDomain,
    #[error("DNS query has no records")]
    NoRecords,
    #[error("DNS request timed out")]
    Timeout,
    #[error("DNS backend failed")]
    Backend,
}

#[derive(Debug, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct ResolveError {
    pub kind: ResolveErrorKind,
    message: String,
    #[source]
    source: Option<Box<dyn Error + Send + Sync>>,
}
impl ResolveError {
    pub fn new(
        kind: ResolveErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self { kind, message: message.into(), source: None }
    }
    pub(crate) fn config(message: impl Into<String>) -> Self {
        Self::new(ResolveErrorKind::Config, message)
    }
    #[cfg(feature = "hickory")]
    pub(crate) fn with_source(
        kind: ResolveErrorKind,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: source.to_string(),
            source: Some(Box::new(source)),
        }
    }
}
