//! Configuration a server declares, as plain data.
//!
//! The listener side (`http.yaml`, listeners, TCP, QUIC, proxy trust and the
//! host's transport policy) names listener types and so needs the `server`
//! feature; the rest is plain data any server reads.

pub mod acl;
pub mod caching;
pub mod cors;
pub mod files;
pub mod host;
pub mod rate_limit;
#[cfg(feature = "server")]
pub mod response;
pub mod routes;
pub mod spa;
pub mod statics;
#[cfg(feature = "server")]
pub mod transport;
#[cfg(feature = "server")]
mod yaml;
#[cfg(feature = "server")]
#[cfg(feature = "server")]
pub use yaml::*;
