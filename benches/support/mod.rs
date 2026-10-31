//! Shared fixtures and preparation; each measured boundary lives in its own file.
//! `dispatch/` begins after HTTP decoding. `transport/http1/` includes an actual
//! persistent loopback round trip. Compare targets only within one boundary.

#[cfg(feature = "axum")]
pub mod axum;
#[cfg(feature = "tls")]
pub mod certificate;
pub mod decoded;
pub mod edge;
pub mod http1;
pub mod json;
pub mod plain;
pub mod setup;
