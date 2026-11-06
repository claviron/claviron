//! Socket acceptance and HTTP connection driving, without host selection.
//!
//! A transport accepts a connection, drives its protocol and hands each
//! request to a service. Which host a connection belongs to, and what that
//! host does with a request, is decided by whoever supplies the service: an
//! edge resolves a virtual host, a single application supplies itself.
//!
//! This crate knows no virtual host, route or backend.

pub mod geo;
pub mod instances;
#[cfg(feature = "quic")]
pub mod quic;
#[cfg(feature = "tcp")]
pub mod tcp;
