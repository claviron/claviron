//! Successful auth lifecycle facts. The host emits only after its state changed.
//! No credentials, ORM model or framework event crosses this boundary.

use std::net::IpAddr;

use time::OffsetDateTime;
use uuid::Uuid;

/// Host-supplied facts at successful login, not live identity or authorization.
/// Built from the authenticated user and request already in hand, without a
/// second lookup. Missing connection facts remain absent, never invented.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoginContext {
    pub username: String,
    pub name: String,
    pub occurred_at: OffsetDateTime,
    pub ip: Option<IpAddr>,
    /// Browser/device UUID, not a cookie, session identifier or credential.
    pub identity: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthEvent {
    Login {
        guard: String,
        subject: String,
        remember: bool,
        context: Option<LoginContext>,
    },
    Logout {
        guard: String,
        subject: String,
        /// Host-supplied username snapshot; absent if the user is unavailable.
        username: Option<String>,
    },
    Registered {
        subject: String,
    },
}

impl continuo::events::Event for AuthEvent {}
