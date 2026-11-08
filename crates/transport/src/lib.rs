//! Socket acceptance and HTTP connection driving.
//!
//! A transport accepts a connection, drives its protocol and hands each
//! request to a service. What that service is belongs to how the listener is
//! composed, not to which crate an application started from: one handler for
//! everything, or the virtual hosts of `claviron-edge`.
//!
//! Request owners and host selection are supplied by consumers. This crate
//! does not depend on Edge.

pub mod geo;
pub mod instances;
pub mod listener;
#[cfg(feature = "orbit")]
pub mod orbit;
pub mod owner;
#[cfg(feature = "quic")]
pub mod quic;
#[cfg(feature = "tcp")]
pub mod single;
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(all(feature = "tcp", unix))]
pub mod unix;
#[cfg(feature = "websocket-session")]
pub mod websocket;
