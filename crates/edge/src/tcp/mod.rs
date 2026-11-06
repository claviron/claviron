//! TCP transport for HTTP/1.1 and HTTP/2, driven by Hyper.
//!
//! `plain` selects hosts from cleartext authority; `secure` binds TLS hosts.
//! Both use the shared
//! Congif configuration catalog and generation-scoped connection gates.
//! QUIC/H3 belongs to the sibling `quic` module, not to this transport.

// Acceptance and connection driving know no host and live in the transport.
pub(crate) use claviron_transport::tcp::listener;

pub mod plain;
#[cfg(feature = "tls")]
pub mod secure;
mod service;

#[cfg(test)]
#[path = "../../tests/tcp/unit/config.rs"]
mod config_tests;

pub use plain::HttpServer;
#[cfg(feature = "tls")]
pub use secure::HttpsServer;
