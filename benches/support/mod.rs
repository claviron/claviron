//! Shared fixtures and preparation; each measured boundary lives in its own file.

#[cfg(feature = "tls")]
pub mod certificate;
pub mod decoded;
pub mod json;
pub mod setup;
