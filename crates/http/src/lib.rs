#[cfg(feature = "server")]
pub mod acme;
pub mod admission;
pub mod authority;
#[cfg(feature = "axum")]
mod bindings;
pub mod body;
pub mod config;
mod ctx;
mod error;
pub mod ext;
pub mod forwarding;
#[cfg(feature = "server")]
pub mod listener;
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod policy;
pub mod resolvers;
#[cfg(feature = "routing")]
pub mod routing;
#[cfg(feature = "server")]
pub mod session;
pub mod trust;
pub mod tuning;
#[cfg(feature = "server")]
pub mod watchdog;

#[cfg(feature = "axum")]
pub use bindings::{from_axum_response, into_axum_response};
pub use ctx::{
    HttpExchange, RequestContext, RouteBody, RouteBodyError, RouteFuture,
    RouteResponse, ServiceResult, boxed_body, bytes_body, empty_body,
    err_status,
};
/// Framework-neutral error and result types for HTTP handlers.
pub use error::{Error, Result};
#[cfg(all(feature = "axum", feature = "cache"))]
pub use ext::Cache;
#[cfg(feature = "axum")]
pub use ext::Inject;
#[cfg(feature = "server")]
pub use listener::ListenerId;

#[cfg(feature = "websocket")]
pub mod upgrade;
