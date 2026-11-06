//! Socket acceptance and HTTP connection driving.
//!
//! A transport accepts a connection, drives its protocol and hands each
//! request to a service. What that service is belongs to how the listener is
//! composed, not to which crate an application started from: one handler for
//! everything, or the virtual hosts of `claviron-edge`.
//!
//! Without the `edge` feature this crate knows no virtual host, route or
//! backend. With it, the [`edge`] module borrows Edge and is the only place an
//! Edge type is named.

#[cfg(feature = "tcp")]
pub mod doors;
#[cfg(feature = "edge")]
pub mod edge;
pub mod geo;
pub mod instances;
#[cfg(feature = "orbit")]
pub mod orbit;
pub mod owner;
#[cfg(feature = "quic")]
pub mod quic;
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(all(feature = "tcp", unix))]
pub mod unix;
