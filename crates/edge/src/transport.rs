//! What a transport driver is allowed to see of this crate.
//!
//! A transport — `claviron-hyper`, `claviron-h3` — has to resolve a host
//! before it can route to one: the TLS handshake picks a certificate by SNI,
//! and the request's authority is checked against what that handshake agreed,
//! both of them before any router exists. So the transports import from here,
//! and that direction is right.
//!
//! How far in they reached was not. `claviron-hyper` and `claviron-h3` both
//! imported `claviron_edge::loader::table::VhostTable` — three levels into a
//! neighbour's loader, a module whose job is reading YAML. Nothing stopped the
//! next transport from importing the snapshot writer beside it or the config
//! parser below it, and nothing would have failed if it had.
//!
//! This module is the whole of what a transport may use. `loader` and
//! `service` are crate-private so that it is enforced rather than described:
//! widening the port is still one `pub use` line, but it is a line written on
//! purpose in a file named for the boundary, rather than a deeper path that
//! reads like an ordinary import in review. `pipeline` stays public because it
//! is the *other* door — what an application author builds a `Routable` or a
//! bypass handler against — and a transport reaches its handful of names from
//! here so that it still has one import path rather than three.
//!
//! The names here are re-exports, not a second set of types — a transport and
//! an application author looking at `BypassService` are looking at the same
//! `BypassService`. What differs is which of them has to know where it lives.

/// A single host's configuration, read by the TLS layer for its certificate
/// and protocol policy.
pub use crate::config::HostConfig;
/// Hosts as the runtime registry hands them out.
pub use crate::loader::resolver::{HostBinding, VhostResolver};
/// The set of hosts a listener serves, as SNI and authority resolution read it.
pub use crate::loader::table::VhostTable;
/// The axum-free dispatch door: a host pinned straight to one adapter.
pub use crate::pipeline::bypass::{BypassHandler, BypassService};
/// Applications mounted on a host, and the connection-level handlers a
/// transport dispatches to before routing: CONNECT and WebTransport.
pub use crate::pipeline::{
    AnyAuthenticated, AppKind, AuthContext, H2ConnectAccept, H2ConnectBinding,
    H2ConnectHandler, H2ConnectRegistry, H2ConnectReject, H2ConnectResult,
    HostAppRegistry, HostAppResolver,
};
#[cfg(feature = "webtransport")]
pub use crate::pipeline::{WebTransportBinding, WebTransportRegistry};
/// Turning an accepted connection and a received request into a host's
/// response — the transport's actual reason for depending on this crate.
pub use crate::service::{
    HostIngress, HostRejection, HostResponse, HostService, PreparedRequest,
    RequestArrival, RequestAuthority, RequestRejection, VhostBindings,
    VhostCtx, resolve_request_authority, resolve_sni_from_table,
};
