//! Host-owned directive compilation and native request effects.

use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;
use serde::de::DeserializeOwned;
use std::sync::Arc;

pub use super::host::{
    HostHandler, HostHandlerFuture, HostHandlerResult, HostLayerBuilder,
    HostRequest,
};
use crate::HostConfig;
pub use claviron_http::dispatch::{RouteCondition, RuleContext};

pub struct RouteContext<'a> {
    pub state: &'a AppState,
    pub host: &'a Host,
    /// Canonical host declaration, before path selection.
    pub config: &'a HostConfig,
    /// Prepared declaration selected by this URL prefix.
    pub route: &'a HostConfig,
    pub prefix: &'a claviron_http::ext::PathPrefix,
}

pub enum RouteEffect {
    /// A Rust contribution before the selected native files/backend chain.
    /// Unhandled returns the same unread body to that chain; a handled error
    /// response, including 404, never falls through.
    Handler(Arc<dyn HostHandler>),
    Condition(Arc<dyn RouteCondition>),
    Layer(Arc<dyn RouteLayer>),
}

/// Compile a declared route policy into native head/response actions.
/// Called only at boot/reload. No Router, request or body is supplied here.
pub trait RouteLayer: Send + Sync {
    fn prepare(
        &self,
        layer: &mut HostLayerBuilder,
    ) -> crate::Result<()>;
}

/// Provider-owned configuration codec, resolved while preparing a host.
pub trait RouteExtension: Send + Sync {
    fn name(&self) -> &'static str;

    /// Inherit a host-level layer unless a route overrides this directive.
    /// Inherited effects must be Layer; fallback scopes keep the host policy.
    fn inherit_from_host(&self) -> bool {
        false
    }

    fn resolve(
        &self,
        context: &RouteContext<'_>,
        source: &serde_yaml::Value,
    ) -> Result<RouteEffect>;
}

/// Bind a typed directive to the edge's resolved host configuration.
pub fn directive<C, F>(
    name: &'static str,
    compile: F,
) -> Arc<dyn RouteExtension>
where
    C: DeserializeOwned + 'static,
    F: Fn(&RouteContext<'_>, C) -> Result<RouteEffect> + Send + Sync + 'static,
{
    Arc::new(TypedDirective {
        name,
        compile,
        config: std::marker::PhantomData::<fn() -> C>,
    })
}

struct TypedDirective<C, F> {
    name: &'static str,
    compile: F,
    config: std::marker::PhantomData<fn() -> C>,
}

impl<C, F> RouteExtension for TypedDirective<C, F>
where
    C: DeserializeOwned,
    F: Fn(&RouteContext<'_>, C) -> Result<RouteEffect> + Send + Sync,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn resolve(
        &self,
        context: &RouteContext<'_>,
        source: &serde_yaml::Value,
    ) -> Result<RouteEffect> {
        let config =
            serde_yaml::from_value(source.clone()).map_err(|error| {
                Error::msg(format!(
                    "route '{}' directive '{}': {error}",
                    context.prefix, self.name,
                ))
            })?;
        (self.compile)(context, config)
    }
}
