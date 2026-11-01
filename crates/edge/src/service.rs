//! Transport-to-host bindings, prepared-request handoff and native reply decisions.

mod authority;
mod extensions;
mod host;
mod response;

pub use authority::resolve_request_authority;
#[cfg(feature = "tcp")]
pub(crate) use authority::{CleartextRequest, HostIdentityCache};
pub use claviron_http::authority::{AuthorityError, RequestAuthority};
pub use extensions::{HostRejection, VhostCtx, resolve_sni_from_table};
#[cfg(feature = "tcp")]
pub(crate) use host::CleartextHostCache;
pub use host::{
    HostService, PreparedRequest, RequestArrival, RequestRejection,
};
pub use response::HostResponse;
