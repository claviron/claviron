#[cfg(all(feature = "axum", feature = "cache"))]
mod cache;
#[cfg(feature = "server")]
mod conn;
mod env;
#[cfg(feature = "server")]
mod host;
#[cfg(feature = "axum")]
mod inject;
mod ip;
mod prefix;
mod received;
mod scheme;
mod xapp;

#[cfg(all(feature = "axum", feature = "cache"))]
pub use cache::*;
#[cfg(feature = "server")]
pub use conn::*;
pub use env::*;
// TODO: `HostConfig` lives in `claviron-edge`; re-exporting it here would
// create a cycle (vhosts already depends on http). Consumers should import
// it directly from `claviron_edge::hostconfig` instead.
#[cfg(feature = "server")]
pub use host::*;
#[cfg(feature = "axum")]
pub use inject::*;
pub use ip::*;
pub use prefix::*;
pub use received::*;
pub use scheme::*;
pub use xapp::*;
