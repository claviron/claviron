//! Schema-independent authentication contracts for Claviron services.

#[cfg(feature = "csrf")]
pub mod csrf;
pub mod events;
mod identity;
mod provider;

pub use identity::Authenticated;
pub use provider::{AnyAuthenticated, UserProvider, UserProviderError};

#[cfg(feature = "orbit")]
pub mod orbit;
