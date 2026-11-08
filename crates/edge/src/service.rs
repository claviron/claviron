//! An accepted connection bound to a host Edge selected: request handoff and
//! the dispatch of a published host plan.

#[cfg(feature = "tcp")]
mod authority;
mod host;

#[cfg(feature = "tcp")]
pub use authority::{CleartextRequest, HostIdentityCache};
#[cfg(feature = "tcp")]
pub use host::CleartextHostCache;
pub use host::{HostService, PreparedRequest};
