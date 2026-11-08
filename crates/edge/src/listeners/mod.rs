//! Edge adapters for transport-owned listener Runnables.
//!
//! These prepare coherent host, TLS and pipeline publications. Socket drivers
//! and managed listener execution belong to `claviron-transport`.

#[cfg(feature = "quic")]
pub mod quic;
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
pub(crate) mod tls;
