//! Schema-independent authentication contracts for Claviron services.
//! Optional user management describes application-owned administrative views,
//! independently of authentication, database models, and network transports.

#[cfg(feature = "csrf")]
pub mod csrf;
pub mod events;
mod identity;
mod provider;

#[cfg(feature = "authorization")]
pub mod authorization;
#[cfg(any(feature = "users", feature = "authorization"))]
mod model;
#[cfg(feature = "users")]
pub mod users;

pub use identity::Authenticated;
pub use provider::{AnyAuthenticated, UserProvider, UserProviderError};

#[cfg(feature = "orbit")]
pub mod orbit;
