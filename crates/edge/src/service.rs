//! Transport-to-host bindings, prepared-request handoff and native reply decisions.

mod authority;
mod ctx;
mod host;
mod response;

pub use authority::resolve_request_authority;
#[cfg(feature = "tcp")]
pub use authority::{CleartextRequest, HostIdentityCache};
#[cfg(any(feature = "tcp", feature = "quic"))]
pub(crate) use claviron_http::authority::AcceptedAuthority;
pub use claviron_http::authority::{AuthorityError, RequestAuthority};
pub use ctx::{
    HostRejection, VhostCtx, resolve_sni_for_listener, resolve_sni_from_table,
};
#[cfg(feature = "tcp")]
pub use host::CleartextHostCache;
pub use host::{
    HostService, PreparedRequest, RequestArrival, RequestRejection,
};
pub use response::HostResponse;
