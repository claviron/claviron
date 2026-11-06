//! What a transport driver is allowed to see of this crate.
//!
//! The drivers live in `claviron-transport`. Its `edge` module borrows this
//! crate: it has to resolve a host before it can serve one — the TLS handshake
//! picks a configuration by SNI, and a request's authority is checked against
//! what that handshake agreed, both before any router exists.
//!
//! This module is the whole of what that crate may use. `loader` stays
//! crate-private so the boundary is enforced rather than described:
//! widening the port is one `pub use` line, written on purpose in a file named
//! for the boundary. `pipeline` stays public because it is the *other* door —
//! what an application author builds a `Routable` or an Other handler against.
//!
//! `HostExecution` is the compiled host owner seen by transports. Rust routers
//! and native backends are prepared inside that host execution, never selected
//! by a transport as an alternative to the host plan.

#[cfg(any(feature = "tcp", feature = "quic"))]
pub mod publication;

/// A single host's configuration, read by the TLS layer for its certificate
/// and protocol policy.
pub use crate::config::HostConfig;
/// Application-owned Rust handler registration, without Axum.
pub use crate::host::apps::other::{OtherHandler, OtherService};
/// Applications mounted on a host, and the connection-level handlers a
/// transport dispatches to before routing: CONNECT and WebTransport.
pub use crate::host::{
    AnyAuthenticated, AuthContext, H2ConnectAccept, H2ConnectBinding,
    H2ConnectHandler, H2ConnectRegistry, H2ConnectReject, H2ConnectResult,
    HostAppRegistry, HostAppResolver, RustRouter,
};
#[cfg(feature = "webtransport")]
pub use crate::host::{WebTransportBinding, WebTransportRegistry};
/// Hosts as the runtime registry hands them out.
pub use crate::loader::resolver::{HostBinding, VhostResolver};
/// Selecting a published host for a server name or a request's authority.
pub use crate::loader::selection::{
    HostRejection, VhostCtx, resolve_request_authority,
    resolve_sni_for_listener, resolve_sni_from_table,
};
/// The set of hosts a listener serves, as SNI and authority resolution read it.
pub use crate::loader::table::VhostTable;
