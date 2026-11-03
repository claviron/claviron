//! Compile changed host declarations into immutable native dispatch owners.

use std::sync::Arc;

use claviron_http::config::{HttpSnapshot, routes::RouteTable};
use claviron_http::dispatch::{ConfiguredRoute, RouteDispatch};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;

use super::backend::BackendPlan;
use super::{HostExecution, HostHandler, HostPipelineBuilder};
use crate::HostConfig;
use crate::pipeline::RustRouter;
use crate::pipeline::apps::{
    HostAppRegistry, HostContributions, ResolvedRouteExtensions,
};
use crate::pipeline::extension::{RouteContext, RouteLayer};
use crate::pipeline::layers::{
    AclLayer, DenyLayer, ProxyLayer, RouteSelectionLayer, scope_steps,
};

/// One cold host compilation batch borrows one listener-policy publication.
pub(crate) struct HostCompiler<'a> {
    pub(crate) state: &'a AppState,
    pub(crate) http: Option<&'a HttpSnapshot>,
}

struct Compiler<'a> {
    #[cfg(feature = "rate-limit")]
    quotas: Option<&'a crate::pipeline::layers::HostQuotas>,
    #[cfg(feature = "broadcast")]
    state: &'a AppState,
    router: Option<RustRouter>,
    applications: &'a [Arc<dyn HostHandler>],
    layers: &'a [Arc<dyn RouteLayer>],
    registry: Option<Arc<HostAppRegistry>>,
    proxy: Option<ProxyLayer>,
    deny: Option<DenyLayer>,
    acl: Option<AclLayer>,
    #[cfg(feature = "websocket")]
    websocket: Option<Arc<crate::pipeline::layers::WebSocketStage>>,
}

impl Compiler<'_> {
    fn scope(
        &self,
        config: Arc<HostConfig>,
        plan: BackendPlan,
        accept_applications: bool,
        handler: Option<&Arc<dyn HostHandler>>,
        layers: &[Arc<dyn RouteLayer>],
    ) -> Result<HostExecution> {
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
        let mut extensions = HostPipelineBuilder::new();
        for layer in self.layers.iter().chain(layers) {
            extensions
                .layer(layer.as_ref())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        let mut builder = HostPipelineBuilder::new();
        if let Some(proxy) = &self.proxy {
            builder
                .layer(proxy.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        if let Some(deny) = &self.deny {
            builder
                .layer(deny.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        if let Some(acl) = &self.acl {
            builder
                .layer(acl.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        #[cfg(feature = "rate-limit")]
        if let Some(quotas) = self.quotas {
            let mut builder =
                crate::pipeline::layers::QuotaPolicy::attach(builder, quotas)
                    .map_err(|error| Error::msg(error.to_string()))?;
            builder.append(extensions.with_state());
            return self.finish_scope(
                builder,
                applications,
                steps,
                accept_applications,
            );
        }
        builder.append(extensions);
        self.finish_scope(builder, applications, steps, accept_applications)
    }

    fn finish_scope<S: Default + Send + 'static>(
        &self,
        mut builder: HostPipelineBuilder<S>,
        applications: &[Arc<dyn HostHandler>],
        steps: Vec<Arc<dyn super::DispatchStep>>,
        accept_applications: bool,
    ) -> Result<HostExecution> {
        #[cfg(feature = "websocket")]
        if accept_applications && let Some(websocket) = &self.websocket {
            builder
                .layer(websocket.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        for application in applications {
            builder
                .layer(application.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        if accept_applications && let Some(router) = &self.router {
            if builder.is_empty() {
                return Ok(HostExecution::router(router.clone()));
            }
            builder
                .layer(router.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        for step in steps {
            builder
                .layer(step)
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        Ok(builder.build().into_execution())
    }
}

impl HostCompiler<'_> {
    pub(crate) fn prepare(
        &self,
        host: &Host,
        config: Arc<HostConfig>,
        router: Option<RustRouter>,
        contributions: &HostContributions,
        #[cfg(feature = "rate-limit")] quotas: Option<
            &crate::pipeline::layers::HostQuotas,
        >,
        #[cfg(feature = "websocket")] websocket: Option<
            Arc<crate::pipeline::H1UpgradeRegistry>,
        >,
    ) -> Result<HostExecution> {
        let state = self.state;
        let compiler = Compiler {
            router,
            #[cfg(feature = "rate-limit")]
            quotas,
            #[cfg(feature = "broadcast")]
            state,
            applications: &contributions.applications,
            layers: &contributions.layers,
            registry: state.registry_ref().resolve::<HostAppRegistry>(),
            proxy: self
                .http
                .and_then(|http| ProxyLayer::for_host(http, &config)),
            deny: self
                .http
                .and_then(|http| DenyLayer::from_matcher(&http.deny)),
            acl: AclLayer::for_host(&config),
            #[cfg(feature = "websocket")]
            websocket: crate::pipeline::layers::WebSocketStage::prepare(
                state, host, &config, websocket,
            ),
        };
        let fallback =
            config.fallback().cloned().unwrap_or_else(|| config.clone());
        let root = claviron_http::ext::PathPrefix::root();
        let base = BackendPlan::prepare(&RouteContext {
            state,
            host,
            config: config.as_ref(),
            route: fallback.as_ref(),
            prefix: &root,
        });
        let layers = match &compiler.registry {
            Some(registry) => registry.resolve_host_layers(&RouteContext {
                state,
                host,
                config: config.as_ref(),
                route: fallback.as_ref(),
                prefix: &root,
            })?,
            None if !config.extensions.is_empty() => {
                return Err(Error::msg(
                    "route extensions require HostAppRegistry",
                ));
            }
            None => Vec::new(),
        };
        let default = compiler.scope(
            fallback.clone(),
            base.clone(),
            true,
            None,
            &layers,
        )?;
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
                    let plan = BackendPlan::prepare(&RouteContext {
                        state,
                        host,
                        config: config.as_ref(),
                        route,
                        prefix,
                    });
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
        let mut builder = HostPipelineBuilder::<()>::new();
        builder
            .layer(RouteSelectionLayer::new(RouteDispatch {
                entries: Arc::new(RouteTable::new(entries)),
                fallback: default,
                rejected,
            }))
            .map_err(|error| Error::msg(error.to_string()))?;
        Ok(builder.build().into_execution())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/compile.rs"]
mod tests;
