//! Host-bound adapter for HTTP's shared routing extension contract.

use claviron_runtime::registry::Result;
use serde::de::DeserializeOwned;
use std::sync::Arc;

use crate::HostConfig;
pub use claviron_http::routing::{
    RouteCondition, RouteEffect, RouteExtension, RouteLayer, RuleContext,
};

pub type RouteContext<'a> =
    claviron_http::routing::RouteContext<'a, HostConfig>;

/// Bind a typed directive to the edge's resolved host configuration.
pub fn directive<C, F>(
    name: &'static str,
    compile: F,
) -> Arc<dyn RouteExtension<HostConfig>>
where
    C: DeserializeOwned + 'static,
    F: Fn(&RouteContext<'_>, C) -> Result<RouteEffect> + Send + Sync + 'static,
{
    claviron_http::routing::directive::<C, F, HostConfig>(name, compile)
}
