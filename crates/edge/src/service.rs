pub(crate) mod acl;
mod authority;
mod bindings;
mod cors;
mod exchange;
pub use exchange::ExchangePolicy;
mod extensions;
mod host;
pub(crate) mod policy;
#[cfg(feature = "rate-limit")]
pub(crate) mod quota;
mod response;

pub use authority::resolve_request_authority;
#[cfg(feature = "tcp")]
pub(crate) use authority::{CleartextRequest, HostIdentityCache};
pub use bindings::VhostBindings;
pub use claviron_http::authority::{AuthorityError, RequestAuthority};
pub use extensions::{HostRejection, VhostCtx, resolve_sni_from_table};
pub use host::{
    HostIngress, HostService, PreparedRequest, RequestArrival, RequestRejection,
};
pub use response::HostResponse;
