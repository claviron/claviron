//! Provider-owned codecs compile once into handlers or head-only predicates.

use crate::ext::PathPrefix;
use axum::Router;
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;
use serde::de::DeserializeOwned;
use std::sync::Arc;

pub struct RouteContext<'a, C = ()> {
    pub state: &'a AppState,
    pub host: &'a Host,
    /// Canonical host/application declaration, before route selection.
    pub config: &'a C,
    /// Prepared selected declaration. Edge consumers read its `files.mapping`
    /// for disk paths; `prefix` and the HTTP request URI remain URL paths.
    pub route: &'a C,
    pub prefix: &'a PathPrefix,
}

pub struct RuleContext<'a> {
    pub method: &'a http::Method,
    pub uri: &'a http::Uri,
    pub headers: &'a http::HeaderMap,
}

/// A prepared predicate. It must not parse configuration or resolve services.
pub trait RouteCondition: Send + Sync {
    fn accepts(
        &self,
        request: &RuleContext<'_>,
    ) -> bool;
}

pub enum RouteEffect {
    Handler(Router<AppState>),
    Condition(Arc<dyn RouteCondition>),
    Layer(Arc<dyn RouteLayer>),
}

/// A boot-prepared wrapper around the selected route and its Rust intercepts.
/// Unlike a condition, a layer may reject a request with its own response.
pub trait RouteLayer: Send + Sync {
    fn apply(
        &self,
        router: Router,
    ) -> Router;
}

/// Cold-path extension contract. The existing host registry owns registration.
pub trait RouteExtension<C = ()>: Send + Sync {
    fn name(&self) -> &'static str;
    /// Inherit this host-level layer on routes that do not override it.
    /// Inherited directives must resolve to `Layer`, never an owner or condition.
    /// Defaults and rejected-route fallbacks receive the same layer.
    fn inherit_from_host(&self) -> bool {
        false
    }
    fn resolve(
        &self,
        context: &RouteContext<'_, C>,
        source: &serde_yaml::Value,
    ) -> Result<RouteEffect>;
}

/// Give a directive a typed Serde codec and a boot-time compiler. Only its
/// resolved effect survives in the dispatch table; the codec is not middleware.
pub fn directive<D, F, C>(
    name: &'static str,
    compile: F,
) -> Arc<dyn RouteExtension<C>>
where
    D: DeserializeOwned + 'static,
    C: 'static,
    F: Fn(&RouteContext<'_, C>, D) -> Result<RouteEffect>
        + Send
        + Sync
        + 'static,
{
    Arc::new(TypedDirective {
        name,
        compile,
        config: std::marker::PhantomData::<fn() -> (D, C)>,
    })
}

struct TypedDirective<D, F, C> {
    name: &'static str,
    compile: F,
    config: std::marker::PhantomData<fn() -> (D, C)>,
}

impl<D, F, C> RouteExtension<C> for TypedDirective<D, F, C>
where
    D: DeserializeOwned,
    F: Fn(&RouteContext<'_, C>, D) -> Result<RouteEffect> + Send + Sync,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn resolve(
        &self,
        context: &RouteContext<'_, C>,
        source: &serde_yaml::Value,
    ) -> Result<RouteEffect> {
        let config =
            serde_yaml::from_value(source.clone()).map_err(|error| {
                Error::msg(format!(
                    "route '{}' directive '{}': {error}",
                    context.prefix, self.name
                ))
            })?;
        (self.compile)(context, config)
    }
}
