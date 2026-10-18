//! Fleet session authentication composed with an application-owned user provider.
//!
//! Construct once after the host supplies its fleet/auth state and bind
//! `Arc<dyn TokenAuthenticator>` for consumers. No fleet is joined here.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use auth::{
    Authority, FleetAuth, OwnedValidator, Principal, Purpose, StateProtection,
    Validation, ValidationHook,
};
pub use orbitive::auth;
use thiserror::Error;

use crate::{AnyAuthenticated, UserProvider, UserProviderError};

#[derive(Debug, Error)]
pub enum AuthError {
    #[error(transparent)]
    Token(#[from] auth::Error),
    #[error(transparent)]
    UserProvider(#[from] UserProviderError),
    #[error("user not found")]
    UserNotFound,
    #[error("user provider returned a different subject")]
    SubjectMismatch,
    #[error("refresh credentials cannot authorize application requests")]
    RefreshCredential,
}

/// A locally validated credential, retaining no raw token. Cannot be deserialized
/// or constructed from a caller-supplied Principal. Recheck before each admission.
#[derive(Clone)]
pub struct VerifiedSession {
    principal: Arc<Principal>,
    state: Arc<dyn LiveSession>,
}

impl VerifiedSession {
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Trusted current Unix seconds. Checks expiry and live fleet state without
    /// hashing/decrypting the token or resolving the user again.
    pub fn check(
        &self,
        now: u64,
    ) -> Result<(), AuthError> {
        self.state.check(&self.principal, now).map_err(Into::into)
    }
}

trait LiveSession: Send + Sync {
    fn check(
        &self,
        principal: &Principal,
        now: u64,
    ) -> auth::Result<()>;
}

impl<P: StateProtection> LiveSession for FleetAuth<P> {
    fn check(
        &self,
        principal: &Principal,
        now: u64,
    ) -> auth::Result<()> {
        ValidationHook::check(self, principal, now)
    }
}

/// A request may attach this value to its HTTP extensions after authentication.
/// Consumers of that same request reuse the user instead of resolving it again.
/// Never retain it as a serialized session or copy it into an unrelated request.
#[derive(Clone)]
pub struct AuthenticatedSession {
    pub user: AnyAuthenticated,
    pub session: VerifiedSession,
}

/// A native request adapter has already decided authentication, including
/// anonymous admission. Paired with an optional `AuthenticatedSession` on that
/// same request, this prevents another adapter from retrying a different policy.
/// Not a credential, serialized state, or authorization for another request.
#[derive(Clone, Copy)]
pub struct AuthenticationResolved;

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
