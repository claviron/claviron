//! Live fleet-session handles and authenticated request context.
//! Token verification and user lookup are implemented in [`token`].

use std::sync::Arc;

use auth::{FleetAuth, Principal, StateProtection, ValidationHook};
pub use orbitive::auth;
use thiserror::Error;

use crate::{AnyAuthenticated, UserProviderError};

pub mod token;

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
