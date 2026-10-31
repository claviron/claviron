//! Boot-compiled, segment-aware routing primitives.
//!
//! HTTP owns shared routing contracts and selected-prefix dispatch. Edge
//! prepares host-specific application engines and consumer-owned directives at boot.

pub use claviron_http::config::routes::RouteTable;

pub use claviron_http::dispatch::{ConfiguredRoute, RouteDispatch, RouteCondition, RuleContext};

#[cfg(feature = "axum")]
pub use super::ext::{RouteContext, RouteEffect, RouteExtension, RouteLayer, directive};
