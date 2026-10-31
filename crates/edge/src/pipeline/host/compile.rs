//! Compile changed host declarations into immutable native dispatch owners.

use std::sync::Arc;

use claviron_http::config::routes::RouteTable;
use claviron_http::dispatch::{ConfiguredRoute, RouteDispatch, RuleContext};
use claviron_http::{RouteBody, RouteFuture};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;
use http::Request;

use super::backend::BackendPlan;
use super::extension::prepare_layers;
use super::files::scope_steps;
use super::{HostHandler, compose_dispatch_other};
use crate::pipeline::apps::{HostAppRegistry, ResolvedRouteExtensions};
use crate::pipeline::ext::{RouteContext, RouteLayer};
use crate::pipeline::other::{OtherHandler, OtherService};
use crate::{HostConfig, config::SelectedRoute};

struct SelectedOwner {
    config: Arc<HostConfig>,
    owner: OtherService,
}

impl OtherHandler for SelectedOwner {
    fn call(
        &self,
        mut request: Request<RouteBody>,
    ) -> RouteFuture {
        request.extensions_mut().insert(SelectedRoute(self.config.clone()));
        self.owner.handle(request)
    }
}

struct HostRoutes(RouteDispatch<OtherService>);

impl OtherHandler for HostRoutes {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        self.0
            .select(&RuleContext {
                method: request.method(),
                uri: request.uri(),
                headers: request.headers(),
            })
            .handle(request)
    }
}

struct Compiler<'a> {
    #[cfg(feature = "broadcast")]
    state: &'a AppState,
    applications: &'a [Arc<dyn HostHandler>],
    layers: &'a [Arc<dyn RouteLayer>],
    registry: Option<Arc<HostAppRegistry>>,
    #[cfg(feature = "websocket")]
    websocket: Option<Arc<super::WebSocketStage>>,
}

impl Compiler<'_> {
    fn scope(
        &self,
        config: Arc<HostConfig>,
        plan: BackendPlan,
        accept_applications: bool,
        handler: Option<&Arc<dyn HostHandler>>,
        layers: &[Arc<dyn RouteLayer>],
    ) -> Result<OtherService> {
        let mut steps = scope_steps(&config);
        #[cfg(feature = "broadcast")]
        if let Some(rest) =
            crate::pipeline::apps::broadcast::rest::prepare(self.state, &config)
        {
            steps.insert(0, rest);
        }
        steps.extend(plan.steps);
        let registered =
            if accept_applications { self.applications } else { &[] };
        let selected = handler.filter(|_| accept_applications).map(|handler| {
            let mut applications = registered.to_vec();
            applications.push(handler.clone());
            applications
        });
        let applications = selected.as_deref().unwrap_or(registered);
        let owner = compose_dispatch_other(plan.terminal, steps, applications);
        // Conditions and configured security layers stay outside WebSocket.
        // It is the first dispatch owner before any application contribution.
        #[cfg(feature = "websocket")]
        let owner = if accept_applications {
            super::compose_websocket_other(owner, self.websocket.clone())
        } else {
            owner
        };
        let mut active_layers = self.layers.to_vec();
        active_layers.extend_from_slice(layers);
        let owner = prepare_layers(owner, &active_layers)
            .map_err(|error| Error::msg(error.to_string()))?;
        Ok(OtherService::new(SelectedOwner { config, owner }))
    }
}

pub(crate) fn prepare(
    state: &AppState,
    host: &Host,
    config: Arc<HostConfig>,
    applications: &[Arc<dyn HostHandler>],
    layers: &[Arc<dyn RouteLayer>],
    #[cfg(feature = "websocket")] websocket: Option<
        Arc<crate::pipeline::H1UpgradeRegistry>,
    >,
) -> Result<OtherService> {
    let compiler = Compiler {
        #[cfg(feature = "broadcast")]
        state,
        applications,
        layers,
        registry: state.registry_ref().resolve::<HostAppRegistry>(),
        #[cfg(feature = "websocket")]
        websocket: super::WebSocketStage::prepare(
            state, host, &config, websocket,
        ),
    };
    let fallback = config.fallback().cloned().unwrap_or_else(|| config.clone());
    let base = BackendPlan::prepare(state, host, &fallback);
    let root = claviron_http::ext::PathPrefix::root();
    let layers = match &compiler.registry {
        Some(registry) => registry.resolve_host_layers(&RouteContext {
            state,
            host,
            config: config.as_ref(),
            route: fallback.as_ref(),
            prefix: &root,
        })?,
        None if !config.extensions.is_empty() => {
            return Err(Error::msg("route extensions require HostAppRegistry"));
        }
        None => Vec::new(),
    };
    let default =
        compiler.scope(fallback.clone(), base.clone(), true, None, &layers)?;
    if config.entry_rules().is_empty() {
        return Ok(default);
    }
    let mut entries = Vec::new();
    for prefix in config.entry_rules() {
        let (owner, conditions) =
            if let Some(route) = config.routes.find(prefix.as_str()) {
                let resolved = match &compiler.registry {
                    Some(registry) => registry.resolve_extensions(
                        &RouteContext {
                            state,
                            host,
                            config: config.as_ref(),
                            route: route.as_ref(),
                            prefix,
                        },
                        false,
                    )?,
                    None if route.extensions.is_empty() => {
                        ResolvedRouteExtensions::default()
                    }
                    None => {
                        return Err(Error::msg(
                            "route extensions require HostAppRegistry",
                        ));
                    }
                };
                let plan = BackendPlan::prepare(state, host, route);
                let owner = compiler.scope(
                    route.clone(),
                    plan,
                    true,
                    resolved.handler.as_ref(),
                    &resolved.layers,
                )?;
                let conditions = resolved.conditions;
                (owner, conditions)
            } else {
                (default.clone(), Vec::new())
            };
        if config.entry_rules().len() == 1
            && prefix.as_str() == "/"
            && conditions.is_empty()
        {
            return Ok(owner);
        }
        entries.push((
            prefix.clone(),
            ConfiguredRoute { pipeline: owner, conditions },
        ));
    }
    let rejected = compiler.scope(fallback, base, false, None, &layers)?;
    Ok(OtherService::new(HostRoutes(RouteDispatch {
        entries: Arc::new(RouteTable::new(entries)),
        fallback: default,
        rejected,
    })))
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/compile.rs"]
mod tests;
