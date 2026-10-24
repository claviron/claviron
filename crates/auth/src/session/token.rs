//! Authenticate a session-bound token and resolve its application user.

use std::sync::Arc;
use std::time::Duration;

use super::auth::{
    Authority, FleetAuth, OwnedValidator, Purpose, StateProtection, Validation,
};
use super::{AuthError, AuthenticatedSession, VerifiedSession};
use crate::UserProvider;
use async_trait::async_trait;

/// Bind this capability once; consumers need not know the state protection type.
#[async_trait]
pub trait TokenAuthenticator: Send + Sync + 'static {
    fn validate(
        &self,
        token: &str,
        now: u64,
    ) -> Result<VerifiedSession, AuthError>;

    /// Resolve user data only when needed. The provider owns its storage/cache policy.
    async fn authenticate(
        &self,
        token: &str,
        now: u64,
    ) -> Result<AuthenticatedSession, AuthError>;
}

/// Reuses an existing FleetAuth, including its local caches and shared session table.
/// The host supplies trusted audience/purpose policy, keys, protection and cache TTL.
pub struct OrbitAuth<P: StateProtection> {
    validator: OwnedValidator,
    state: Arc<FleetAuth<P>>,
    users: Arc<dyn UserProvider>,
    cache_ttl: Duration,
}

impl<P: StateProtection> OrbitAuth<P> {
    pub fn new(
        authority: Arc<Authority>,
        validation: Validation,
        state: Arc<FleetAuth<P>>,
        users: Arc<dyn UserProvider>,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            validator: authority.validator(validation).into_owned(),
            state,
            users,
            cache_ttl,
        }
    }
}

#[async_trait]
impl<P: StateProtection> TokenAuthenticator for OrbitAuth<P> {
    fn validate(
        &self,
        token: &str,
        now: u64,
    ) -> Result<VerifiedSession, AuthError> {
        let principal = self.validator.validate_cached(
            token,
            now,
            self.state.as_ref(),
            self.cache_ttl,
            self.state.as_ref(),
        )?;
        if principal.claims().purpose == Purpose::Refresh {
            return Err(AuthError::RefreshCredential);
        }
        Ok(VerifiedSession { principal, state: self.state.clone() })
    }

    async fn authenticate(
        &self,
        token: &str,
        now: u64,
    ) -> Result<AuthenticatedSession, AuthError> {
        let session = self.validate(token, now)?;
        let user = self
            .users
            .find_by_subject(session.principal().subject())
            .await?
            .ok_or(AuthError::UserNotFound)?;
        if user.auth_subject() != session.principal().subject() {
            return Err(AuthError::SubjectMismatch);
        }
        Ok(AuthenticatedSession { user, session })
    }
}
