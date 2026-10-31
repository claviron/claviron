//! Compile changed host declarations into immutable native dispatch owners.

use std::sync::Arc;

use claviron_http::dispatch::{ConfiguredRoute, RouteDispatch, RuleContext};
use claviron_http::config::routes::RouteTable;
use claviron_http::{RouteBody, RouteFuture};
use claviron_runtime::{AppState, registry::{Error, Result}};
use claviron_types::Host;
use http::Request;

use super::{HostHandler, compose_dispatch_other};
use super::backend::BackendPlan;
use super::files::scope_steps;
use super::extension::prepare_layers;
use crate::{HostConfig, config::SelectedRoute};
use crate::pipeline::other::{OtherHandler, OtherService};
use crate::pipeline::apps::{HostAppRegistry, ResolvedRouteExtensions};
use crate::pipeline::ext::{RouteContext, RouteLayer};
#[cfg(feature = "axum")]
use crate::pipeline::app;

struct SelectedOwner {
    config: Arc<HostConfig>,
    owner: OtherService,
}

impl OtherHandler for SelectedOwner {
    fn call(&self, mut request: Request<RouteBody>) -> RouteFuture {
        request.extensions_mut().insert(SelectedRoute(self.config.clone()));
        self.owner.handle(request)
    }
}

struct HostRoutes(RouteDispatch<OtherService>);

impl OtherHandler for HostRoutes {
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        self.0.select(&RuleContext {
            method: request.method(), uri: request.uri(), headers: request.headers(),
        }).handle(request)
    }
}

struct Compiler<'a> {
    #[cfg(feature = "axum")]
    state: &'a AppState,
    #[cfg(feature = "axum")]
    host: &'a Host,
    #[cfg(feature = "axum")]
    config: Arc<HostConfig>,
    applications: &'a [Arc<dyn HostHandler>],
    #[cfg(feature = "axum")]
    routers: Vec<axum::Router<AppState>>,
    registry: Option<Arc<HostAppRegistry>>,
    #[cfg(feature = "websocket")]
    websocket: Option<Arc<super::WebSocketStage>>,
}

impl Compiler<'_> {
    fn scope(
        &self, config: Arc<HostConfig>, plan: BackendPlan, accept_applications: bool,
        handler: Option<&Arc<dyn HostHandler>>,
        layers: &[Arc<dyn RouteLayer>],
    ) -> Result<OtherService> {
        let mut steps = scope_steps(&config);
        steps.extend(plan.steps);
        let registered = if accept_applications { self.applications } else { &[] };
        let selected = handler.filter(|_| accept_applications).map(|handler| {
            let mut applications = registered.to_vec();
            applications.push(handler.clone());
            applications
        });
        let applications = selected.as_deref().unwrap_or(registered);
        #[cfg(feature = "broadcast")]
        let configured = app::configured(self.state, &config);
        #[cfg(feature = "broadcast")]
        let dispatch_applications = if configured.is_some() { &[][..] } else { applications };
        #[cfg(not(feature = "broadcast"))]
        let dispatch_applications = applications;
        let owner = compose_dispatch_other(plan.terminal, steps, dispatch_applications);
        #[cfg(feature = "broadcast")]
        let owner = match configured {
            // User Rust contributions retain first claim over configured
            // endpoints. The extra adapter exists only for these endpoints.
            Some(routes) => super::compose_other(app::with_downstream(routes, owner), applications),
            None => owner,
        };
        #[cfg(feature = "axum")]
        let owner = app::compose(
            self.state, self.host, self.config.clone(),
            if accept_applications { &self.routers } else { &[] },
            self.registry.as_deref(), owner,
        );
        // Conditions and configured security layers stay outside WebSocket.
        // It is the first dispatch owner before any application contribution.
        #[cfg(feature = "websocket")]
        let owner = if accept_applications {
            super::compose_websocket_other(owner, self.websocket.clone())
        } else { owner };
        let owner = prepare_layers(owner, layers)
            .map_err(|error| Error::msg(error.to_string()))?;
        Ok(OtherService::new(SelectedOwner { config, owner }))
    }
}

pub(crate) fn prepare(
    state: &AppState,
    host: &Host,
    config: Arc<HostConfig>,
    applications: &[Arc<dyn HostHandler>],
    #[cfg(feature = "axum")] routers: Vec<axum::Router<AppState>>,
    #[cfg(feature = "websocket")] websocket: Option<Arc<crate::pipeline::H1UpgradeRegistry>>,
) -> Result<OtherService> {
    let compiler = Compiler {
        #[cfg(feature = "axum")]
        state,
        #[cfg(feature = "axum")]
        host,
        #[cfg(feature = "axum")]
        config: config.clone(),
        applications,
        #[cfg(feature = "axum")]
        routers,
        registry: state.registry_ref().resolve::<HostAppRegistry>(),
        #[cfg(feature = "websocket")]
        websocket: super::WebSocketStage::prepare(state, host, &config, websocket),
    };
    let fallback = config.fallback().cloned().unwrap_or_else(|| config.clone());
    let base = BackendPlan::prepare(state, host, &fallback);
    let root = claviron_http::ext::PathPrefix::root();
    let layers = match &compiler.registry {
        Some(registry) => registry.resolve_host_layers(&RouteContext {
            state, host, config: config.as_ref(), route: fallback.as_ref(), prefix: &root,
        })?,
        None if !config.extensions.is_empty() => {
            return Err(Error::msg("route extensions require HostAppRegistry"));
        }
        None => Vec::new(),
    };
    let default = compiler.scope(fallback.clone(), base.clone(), true, None, &layers)?;
    if config.entry_rules().is_empty() { return Ok(default); }
    let mut entries = Vec::new();
    for prefix in config.entry_rules() {
        let (owner, conditions) = if let Some(route) = config.routes.find(prefix.as_str()) {
            let resolved = match &compiler.registry {
                Some(registry) => registry.resolve_extensions(&RouteContext {
                    state, host, config: config.as_ref(), route: route.as_ref(), prefix,
                }, false)?,
                None if route.extensions.is_empty() => ResolvedRouteExtensions::default(),
                None => return Err(Error::msg("route extensions require HostAppRegistry")),
            };
            let plan = BackendPlan::prepare(state, host, route);
            let owner = compiler.scope(route.clone(), plan, true,
                resolved.handler.as_ref(),
                &resolved.layers,
            )?;
            let conditions = resolved.conditions;
            (owner, conditions)
        } else { (default.clone(), Vec::new()) };
        if config.entry_rules().len() == 1 && prefix.as_str() == "/" && conditions.is_empty() {
            return Ok(owner);
        }
        entries.push((prefix.clone(), ConfiguredRoute { pipeline: owner, conditions }));
    }
    let rejected = compiler.scope(fallback, base, false, None, &layers)?;
    Ok(OtherService::new(HostRoutes(RouteDispatch {
        entries: Arc::new(RouteTable::new(entries)), fallback: default, rejected,
    })))
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/compile.rs"]
mod tests;
