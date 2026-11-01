//! TCP transport for HTTP/1.1 and HTTP/2, driven by Hyper.
//!
//! `plain` selects hosts from cleartext authority; `secure` binds TLS hosts.
//! Both use the shared
//! Congif configuration catalog and generation-scoped connection gates.
//! QUIC/H3 belongs to the sibling `quic` module, not to this transport.

mod geo;
mod io;
mod listener;
pub mod plain;
mod protocol;
#[cfg(feature = "tls")]
pub mod secure;
mod service;

#[cfg(test)]
#[path = "../../tests/tcp/unit/config.rs"]
mod config_tests;

pub use plain::HttpServer;
#[cfg(feature = "tls")]
pub use secure::HttpsServer;
