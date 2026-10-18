//! Application-owned subject lookup contract.

use std::error::Error as StdError;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::Authenticated;

/// Type-erased authenticated subject returned across crate boundaries.
pub type AnyAuthenticated = Arc<dyn Authenticated>;

/// Error returned by an application-provided identity store.
#[derive(Debug, Error)]
pub enum UserProviderError {
    #[error("user provider error: {0}")]
    Message(String),

    #[error(transparent)]
    Source(#[from] Box<dyn StdError + Send + Sync>),
}

impl UserProviderError {
    pub fn msg(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

/// Resolves token/chat identifiers into application-owned users.
///
/// The trait lives in core while its implementation lives in the downstream
/// application. Bind the concrete provider as `dyn UserProvider` in the
/// runtime registry; JWT and chat can then resolve it without depending on
/// the application's model crate.
#[async_trait]
pub trait UserProvider: Send + Sync + 'static {
    /// Resolve the stable token subject (for example a UUID string).
    async fn find_by_subject(
        &self,
        subject: &str,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError>;

    /// Resolve an optional application-local numeric key.
    async fn find_by_local_id(
        &self,
        _local_id: i64,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError> {
        Ok(None)
    }

    /// Resolve a human-readable identifier such as a username.
    async fn find_by_identifier(
        &self,
        _identifier: &str,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError> {
        Ok(None)
    }
}
