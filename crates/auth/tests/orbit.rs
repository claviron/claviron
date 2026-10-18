use std::any::Any;
use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use claviron_auth::orbit::auth::{
    Audience, Authority, Blake3State, Claims, Error, FleetAuth, Keyring,
    Purpose, SecretKey, Validation,
};
use claviron_auth::orbit::{AuthError, OrbitAuth, TokenAuthenticator};
use claviron_auth::{
    AnyAuthenticated, Authenticated, UserProvider, UserProviderError,
};
use orbitive::Fleet;

struct Users;
struct User(String);
impl Authenticated for User {
    fn auth_subject(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }
    fn auth_identifier(&self) -> &str {
        &self.0
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
#[async_trait]
impl UserProvider for Users {
    async fn find_by_subject(
        &self,
        subject: &str,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError> {
        Ok(Some(Arc::new(User(subject.to_owned()))))
    }
}

fn authority() -> Authority {
    Authority::new(
        "test",
        "issuer",
        Keyring::new(1, [(1, SecretKey::from_bytes([7; 32]))]).unwrap(),
    )
    .unwrap()
}
fn state(
    fleet: Arc<Fleet>,
    authority: &Authority,
) -> Arc<FleetAuth<Blake3State>> {
    Arc::new(
        FleetAuth::new(
            fleet,
            authority,
            SecretKey::from_bytes([9; 32]),
            NonZeroUsize::new(8).unwrap(),
            Blake3State,
        )
        .unwrap(),
    )
}
fn claims(purpose: Purpose) -> Claims {
    Claims {
        subject: "alice".into(),
        audience: Audience::new("chat").unwrap(),
        purpose,
        capabilities: Default::default(),
        issued_at: 10,
        not_before: 10,
        expires_at: 100,
    }
}
fn adapter(
    authority: &Arc<Authority>,
    state: Arc<FleetAuth<Blake3State>>,
    purpose: Purpose,
) -> OrbitAuth<Blake3State> {
    OrbitAuth::new(
        authority.clone(),
        Validation {
            audience: Audience::new("chat").unwrap(),
            purpose,
            required_capabilities: Default::default(),
        },
        state,
        Arc::new(Users),
        Duration::from_secs(90),
    )
}

#[tokio::test]
async fn cached_credentials_and_live_connections_observe_peer_revocation() {
    let authority = Arc::new(authority());
    let fleet = Arc::new(Fleet::join("adapter-revoke", 1).unwrap());
    let first = state(fleet.clone(), &authority);
    let peer = state(fleet, &authority);
    let session = first.create_session("alice", 10, 100).unwrap();
    let token =
        authority.issue_session(claims(Purpose::Access), &session).unwrap();
    let auth = adapter(&authority, peer, Purpose::Access);
    // Prepared validator remains usable after the issuing authority is dropped.
    drop(authority);
    let authenticated = auth.authenticate(token.expose(), 11).await.unwrap();
    assert_eq!(authenticated.user.auth_subject(), "alice");
    authenticated.session.check(12).unwrap();
    auth.validate(token.expose(), 12).unwrap();
    first.revoke_session(session.id(), 13).unwrap();
    assert!(matches!(
        authenticated.session.check(14),
        Err(AuthError::Token(Error::Revoked))
    ));
    assert!(matches!(
        auth.validate(token.expose(), 14),
        Err(AuthError::Token(Error::Revoked))
    ));
}

#[test]
fn cached_and_connection_credentials_expire_and_reject_clock_rollback() {
    let authority = Arc::new(authority());
    let state =
        state(Arc::new(Fleet::join("adapter-expiry", 1).unwrap()), &authority);
    let session = state.create_session("alice", 10, 100).unwrap();
    let token =
        authority.issue_session(claims(Purpose::Access), &session).unwrap();
    let auth = adapter(&authority, state, Purpose::Access);
    let verified = auth.validate(token.expose(), 11).unwrap();
    assert!(matches!(
        verified.check(9),
        Err(AuthError::Token(Error::NotYetValid))
    ));
    assert!(matches!(
        verified.check(100),
        Err(AuthError::Token(Error::Expired))
    ));
    assert!(matches!(
        auth.validate(token.expose(), 100),
        Err(AuthError::Token(Error::Expired))
    ));
}

#[test]
fn requires_session_and_does_not_accept_refresh_as_access() {
    let authority = Arc::new(authority());
    let state =
        state(Arc::new(Fleet::join("adapter-purpose", 1).unwrap()), &authority);
    let session = state.create_session("alice", 10, 100).unwrap();
    let unbound = authority.issue(claims(Purpose::Access)).unwrap();
    let auth = adapter(&authority, state.clone(), Purpose::Access);
    assert!(auth.validate(unbound.expose(), 11).is_err());
    let refresh =
        authority.issue_session(claims(Purpose::Refresh), &session).unwrap();
    assert!(auth.validate(refresh.expose(), 11).is_err());
    let refresh_policy = adapter(&authority, state, Purpose::Refresh);
    assert!(matches!(
        refresh_policy.validate(refresh.expose(), 11),
        Err(AuthError::RefreshCredential)
    ));
}

struct WrongUser;
#[async_trait]
impl UserProvider for WrongUser {
    async fn find_by_subject(
        &self,
        _: &str,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError> {
        Ok(Some(Arc::new(User("someone-else".into()))))
    }
}

#[tokio::test]
async fn provider_cannot_substitute_a_different_subject() {
    let authority = Arc::new(authority());
    let state =
        state(Arc::new(Fleet::join("adapter-subject", 1).unwrap()), &authority);
    let session = state.create_session("alice", 10, 100).unwrap();
    let token =
        authority.issue_session(claims(Purpose::Access), &session).unwrap();
    let auth = OrbitAuth::new(
        authority,
        Validation {
            audience: Audience::new("chat").unwrap(),
            purpose: Purpose::Access,
            required_capabilities: Default::default(),
        },
        state,
        Arc::new(WrongUser),
        Duration::from_secs(90),
    );
    assert!(matches!(
        auth.authenticate(token.expose(), 11).await,
        Err(AuthError::SubjectMismatch)
    ));
}
