//! Serving Edge's virtual hosts over this crate's drivers.
//!
//! The transport borrows Edge: a listener's snapshot is prepared from the
//! host table Edge publishes, and a connection is given to the host Edge
//! selects for it. Edge owns its hosts, pipelines and registries; nothing
//! here outlives a publication it was prepared from. This is the only module
//! that names an Edge type, and it exists only with the `edge` feature.

#[cfg(feature = "quic")]
pub mod quic;
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
pub(crate) mod tls;
