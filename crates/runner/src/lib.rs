//! The shell Claviron's language runners share: one application, served by
//! one process kind, with no virtual hosts, and the language's own service —
//! the owner — answering what nothing in front of it claimed.
//!
//! See `ARCHITECTURE.md` for the whole of it; the pieces arrive here one at a
//! time from the runners that grew them.

#[cfg(feature = "broadcast")]
mod broadcast;
#[cfg(feature = "files")]
pub mod files;
pub mod launch;
pub mod management;
pub mod options;
pub mod orbit;
pub mod routing;
pub mod socket;
#[cfg(feature = "supervisor")]
pub mod supervisor;
#[cfg(feature = "tls")]
pub mod tls;
#[cfg(feature = "watch")]
pub mod watch;
#[cfg(feature = "websocket")]
pub mod websocket;

pub use options::{EdgeShutdownPolicy, RunnerOptions};
pub use orbit::{
    EdgeShutdownInitiated, Handshake, LinkCommandReceived, OrbitIngress,
};
#[cfg(feature = "axum")]
pub use routing::register_router;
pub use routing::{
    AppRequest, AppResponse, Application, Owner, RequestBody, Routing,
    ensure_application, register_owner, register_router_with,
};
pub use socket::{Bind, Protocols, SocketIngress};
#[cfg(feature = "tls")]
pub use tls::Tls;
