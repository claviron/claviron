//! TCP transport for HTTP/1.1 and HTTP/2, driven by Hyper.
//!
//! `plain` serves host applications, redirects and optional HTTP-01 challenges;
//! `secure` drives TLS-terminated vhost applications. Both use the shared
//! Congif configuration catalog and generation-scoped connection gates.
//! QUIC/H3 belongs to the sibling `quic` module, not to this transport.

mod geo;
mod listener;
pub mod plain;
mod protocol;
#[cfg(feature = "tls")]
pub mod secure;
mod service;

#[cfg(test)]
#[path = "../../tests/tcp/unit/config.rs"]
mod config_tests;

#[cfg(test)]
#[path = "../../tests/tcp/support/framing.rs"]
mod framing_cases;

pub use plain::HttpServer;
#[cfg(feature = "tls")]
pub use secure::HttpsServer;
