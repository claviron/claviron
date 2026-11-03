//! Compile changed host declarations into immutable native dispatch owners.

use std::sync::Arc;

use claviron_http::config::{HttpPolicy, HttpSnapshot, routes::RouteTable};
use claviron_http::dispatch::{ConfiguredRoute, RouteDispatch};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;

use super::backend::BackendPlan;
use super::{
    Backend, BackendHandler, HostExecution, HostHandler, HostPipelineBuilder,
};
use crate::HostConfig;
use crate::pipeline::apps::{
    HostAppRegistry, HostContributions, ResolvedRouteExtensions,
};
use crate::pipeline::extension::{RouteContext, RouteLayer};
#[cfg(feature = "access-log")]
use crate::pipeline::layers::{AccessLogLayer, AccessLogState};
use crate::pipeline::layers::{
    AclLayer, CorsLayer, CorsState, DenyLayer, ProxyLayer, ReplyPolicy,
    RequestBodyLayer, RouteSelectionLayer, prepare_headers, scope_steps,
};
use crate::pipeline::{RouteProtocol, RustRouter};

#[derive(Default)]
struct CorsExchange<S> {
    cors: CorsState,
    inner: S,
}

impl<S> AsMut<CorsState> for CorsExchange<S> {
    fn as_mut(&mut self) -> &mut CorsState {
        &mut self.cors
    }
}

struct ScopePlan<'a> {
    applications: &'a [Arc<dyn HostHandler>],
    steps: Vec<Arc<dyn super::DispatchStep>>,
    #[cfg(feature = "broadcast")]
    broadcast: Option<Arc<dyn BackendHandler>>,
    backends: Vec<Arc<dyn BackendHandler>>,
    fallback: Option<Arc<dyn super::DispatchStep>>,
    policy: HttpPolicy,
    accept_applications: bool,
    cors: Option<CorsLayer>,
    body: Option<RequestBodyLayer>,
    reply: Option<ReplyPolicy>,
}

#[cfg(feature = "access-log")]
#[derive(Default)]
struct LoggedState<S> {
    log: AccessLogState,
    admission: S,
}

#[cfg(feature = "access-log")]
impl<S> AsMut<AccessLogState> for LoggedState<S> {
    fn as_mut(&mut self) -> &mut AccessLogState {
        &mut self.log
    }
}

#[cfg(feature = "access-log")]
impl AsMut<()> for LoggedState<()> {
    fn as_mut(&mut self) -> &mut () {
        &mut self.admission
    }
}

#[cfg(all(feature = "access-log", feature = "rate-limit"))]
impl AsMut<()> for LoggedState<crate::pipeline::layers::QuotaState> {
    fn as_mut(&mut self) -> &mut () {
        self.admission.as_mut()
    }
}

#[cfg(all(feature = "access-log", feature = "rate-limit"))]
impl AsMut<crate::pipeline::layers::QuotaState>
    for LoggedState<crate::pipeline::layers::QuotaState>
{
    fn as_mut(&mut self) -> &mut crate::pipeline::layers::QuotaState {
        &mut self.admission
    }
}

/// One cold host compilation batch borrows one listener-policy publication.
pub(crate) struct HostCompiler<'a> {
    pub(crate) state: &'a AppState,
    pub(crate) http: Option<&'a HttpSnapshot>,
    pub(crate) protocol: Option<RouteProtocol>,
}

pub(crate) struct HostCompileTarget<'a> {
    pub(crate) host: &'a Host,
    pub(crate) policy: HttpPolicy,
}

struct Compiler<'a> {
    #[cfg(feature = "access-log")]
    host: &'a Host,
    config: &'a HostConfig,
    policy: HttpPolicy,
    headers: Option<&'a claviron_http::config::host::HostHeadersConfig>,
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
        prefix: &claviron_http::ext::PathPrefix,
    ) -> Result<HostExecution> {
        let steps = scope_steps(&config);
        #[cfg(feature = "broadcast")]
        let broadcast = crate::pipeline::apps::broadcast::rest::prepare(
            self.state, &config,
        );
        let registered =
            if accept_applications { self.applications } else { &[] };
        let selected = handler.filter(|_| accept_applications).map(|handler| {
            let mut applications = registered.to_vec();
            applications.push(handler.clone());
            applications
        });
        let applications = selected.as_deref().unwrap_or(registered);
        let policy = self.config.policy_at(prefix, self.policy);
        let scope = ScopePlan {
            applications,
            steps,
            #[cfg(feature = "broadcast")]
            broadcast,
            backends: plan.steps,
            fallback: plan.fallback,
            policy,
            accept_applications,
            cors: self
                .config
                .setting_at(prefix, |scope| scope.cors.as_ref())
                .cloned()
                .map(CorsLayer::new)
                .transpose()
                .map_err(|error| Error::msg(error.to_string()))?,
            body: RequestBodyLayer::new(policy.requests),
            reply: ReplyPolicy::new(policy.responses),
        };
        let mut extensions = HostPipelineBuilder::new();
        for layer in self.layers.iter().chain(layers) {
            extensions
                .layer(layer.as_ref())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        let mut builder = HostPipelineBuilder::new();
        prepare_headers(
            &mut builder,
            self.headers.into_iter().chain(
                self.config
                    .scopes_at(prefix)
                    .filter_map(|scope| scope.headers.as_ref()),
            ),
        )
        .map_err(|error| Error::msg(error.to_string()))?;
        if let Some(proxy) = &self.proxy {
            builder
                .layer(proxy.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        let mut access = HostPipelineBuilder::new();
        if let Some(deny) = &self.deny {
            access
                .layer(deny.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        if let Some(acl) = &self.acl {
            access
                .layer(acl.clone())
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        #[cfg(feature = "access-log")]
        if let Some(log) = AccessLogLayer::new(
            self.host,
            self.config.setting_at(prefix, |scope| scope.access_log()),
        ) {
            #[cfg(feature = "rate-limit")]
            if let Some(quotas) = self.quotas {
                let mut builder = builder.with_state::<LoggedState<crate::pipeline::layers::QuotaState>>();
                builder
                    .layer(log)
                    .map_err(|error| Error::msg(error.to_string()))?;
                let mut access = access.with_state();
                crate::pipeline::layers::QuotaPolicy::attach_to(
                    &mut access,
                    quotas,
                )
                .map_err(|error| Error::msg(error.to_string()))?;
                return self.finish_policy(
                    builder,
                    access,
                    extensions.with_state(),
                    scope,
                );
            }
            let mut builder = builder.with_state::<LoggedState<()>>();
            builder
                .layer(log)
                .map_err(|error| Error::msg(error.to_string()))?;
            return self.finish_policy(
                builder,
                access.with_state(),
                extensions.with_state(),
                scope,
            );
        }
        #[cfg(feature = "rate-limit")]
        if let Some(quotas) = self.quotas {
            let access =
                crate::pipeline::layers::QuotaPolicy::attach(access, quotas)
                    .map_err(|error| Error::msg(error.to_string()))?;
            return self.finish_policy(
                builder.with_state(),
                access,
                extensions.with_state(),
                scope,
            );
        }
        self.finish_policy(builder, access, extensions, scope)
    }

    fn finish_policy<S: Default + Send + 'static>(
        &self,
        mut builder: HostPipelineBuilder<S>,
        access: HostPipelineBuilder<S>,
        extensions: HostPipelineBuilder<S>,
        mut scope: ScopePlan<'_>,
    ) -> Result<HostExecution> {
        if let Some(reply) = scope.reply.take() {
            builder
                .layer(reply)
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        if let Some(cors) = scope.cors.take() {
            let mut builder = builder
                .map_state(|state: &mut CorsExchange<S>| &mut state.inner);
            let (capture, admission) = cors.split();
            builder
                .layer(capture)
                .map_err(|error| Error::msg(error.to_string()))?;
            builder.append(
                access
                    .map_state(|state: &mut CorsExchange<S>| &mut state.inner),
            );
            builder
                .layer(admission)
                .map_err(|error| Error::msg(error.to_string()))?;
            return self.finish_scope(
                builder,
                extensions
                    .map_state(|state: &mut CorsExchange<S>| &mut state.inner),
                scope,
            );
        }
        builder.append(access);
        self.finish_scope(builder, extensions, scope)
    }

    fn finish_scope<S: Default + Send + 'static>(
        &self,
        mut builder: HostPipelineBuilder<S>,
        extensions: HostPipelineBuilder<S>,
        scope: ScopePlan<'_>,
    ) -> Result<HostExecution> {
        let ScopePlan {
            applications,
            steps,
            backends,
            fallback,
            policy,
            accept_applications,
            body,
            #[cfg(feature = "broadcast")]
            broadcast,
            ..
        } = scope;
        if let Some(body) = body {
            builder
                .layer(body)
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        builder.append(extensions);
        #[cfg(feature = "websocket")]
        if accept_applications && let Some(websocket) = &self.websocket {
            builder
                .layer(Backend::new(websocket.clone(), policy))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        for application in applications {
            builder
                .layer(Backend::new(application.clone(), policy))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        if accept_applications && let Some(router) = &self.router {
            if builder.is_empty() {
                return Ok(HostExecution::router(router.clone(), policy));
            }
            builder
                .layer(Backend::new(router.clone(), policy))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        #[cfg(feature = "broadcast")]
        if let Some(broadcast) = broadcast {
            builder
                .layer(Backend::new(broadcast, policy))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        for step in steps {
            builder
                .layer(step)
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        for backend in backends {
            builder
                .layer(Backend::new(backend, policy))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        if let Some(fallback) = fallback {
            builder
                .layer(fallback)
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
        self.prepare_target(
            HostCompileTarget { host, policy: HttpPolicy::unlimited() },
            config,
            router,
            contributions,
            #[cfg(feature = "rate-limit")]
            quotas,
            #[cfg(feature = "websocket")]
            websocket,
        )
    }

    pub(crate) fn prepare_target(
        &self,
        target: HostCompileTarget<'_>,
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
        let HostCompileTarget { host, policy } = target;
        let state = self.state;
        let compiler = Compiler {
            #[cfg(feature = "access-log")]
            host,
            config: &config,
            policy,
            headers: self.http.map(|http| http.headers.as_ref()),
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
            Some(registry) => registry.resolve_host_layers(
                &RouteContext {
                    state,
                    host,
                    config: config.as_ref(),
                    route: fallback.as_ref(),
                    prefix: &root,
                },
                self.protocol,
            )?,
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
            &root,
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
                            self.protocol,
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
                        prefix,
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
        let rejected =
            compiler.scope(fallback, base, false, None, &layers, &root)?;
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
