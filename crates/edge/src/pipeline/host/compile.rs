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
use crate::Site;
use crate::pipeline::apps::{
    HostAppRegistry, HostContributions, ResolvedRouteExtensions,
};
use crate::pipeline::extension::{RouteContext, RouteLayer};
#[cfg(feature = "access-log")]
use crate::pipeline::layers::{AccessLogLayer, AccessLogState};
use crate::pipeline::layers::{
    AclLayer, CorsLayer, CorsState, DenyLayer, ProxyLayer, RedirectLayer,
    ReplyPolicy, RequestBodyLayer, RouteSelectionLayer, prepare_headers,
    scope_steps,
};
#[cfg(feature = "metrics")]
use crate::pipeline::layers::{MetricsLayer, MetricsState};
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
    before_files: Vec<Arc<dyn BackendHandler>>,
    backends: Vec<Arc<dyn BackendHandler>>,
    fallback: Option<Arc<dyn super::DispatchStep>>,
    policy: HttpPolicy,
    accept_applications: bool,
    cors: Option<CorsLayer>,
    body: Option<RequestBodyLayer>,
    reply: Option<ReplyPolicy>,
    redirect: Option<RedirectLayer>,
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
    pub(crate) listener: Option<&'a claviron_http::config::ListenerConfig>,
}

struct Compiler<'a> {
    host: &'a Host,
    listener: Option<&'a claviron_http::config::ListenerConfig>,
    protocol: Option<RouteProtocol>,
    config: &'a Site,
    transport: &'a claviron_http::config::transport::TransportPolicy,
    policy: HttpPolicy,
    headers: Option<&'a claviron_http::config::host::HostHeadersConfig>,
    #[cfg(feature = "metrics")]
    metrics: bool,
    #[cfg(feature = "rate-limit")]
    quotas: Option<&'a crate::pipeline::layers::HostQuotas>,
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
    /// Whether this scope declares an application owner: the edge's own Link
    /// dispatch or a directive a registered backend claims. A scope has at
    /// most one. The edge knows no backend by name, so this is the first place
    /// the rule can be held.
    fn application_owner(
        &self,
        config: &Site,
        prefix: &claviron_http::ext::PathPrefix,
    ) -> Result<bool> {
        let owners = self
            .registry
            .as_ref()
            .map(|registry| registry.declared_backends(config))
            .unwrap_or_default();
        #[cfg(feature = "orbit")]
        let owners = if config.orbit().is_empty() {
            owners
        } else {
            [vec!["orbit"], owners].concat()
        };
        if owners.len() > 1 {
            return Err(Error::msg(format!(
                "route '{prefix}' declares more than one application owner: {}",
                owners.join(", ")
            )));
        }
        Ok(!owners.is_empty())
    }

    fn scope(
        &self,
        config: Arc<Site>,
        mut plan: BackendPlan,
        accept_applications: bool,
        handler: Option<&Arc<dyn HostHandler>>,
        layers: &[Arc<dyn RouteLayer>],
        prefix: &claviron_http::ext::PathPrefix,
    ) -> Result<HostExecution> {
        let redirect = self
            .config
            .setting_at(prefix, |scope| scope.redirect.as_ref())
            .and_then(|rules| rules.first())
            .map(RedirectLayer::new);
        let owned = self.application_owner(&config, prefix)?;
        let steps = if redirect.is_none() {
            scope_steps(&config, owned)
        } else {
            Vec::new()
        };
        let registered =
            if accept_applications { self.applications } else { &[] };
        let selected = handler.filter(|_| accept_applications).map(|handler| {
            let mut applications = registered.to_vec();
            applications.push(handler.clone());
            applications
        });
        let applications = selected.as_deref().unwrap_or(registered);
        let policy = self.config.policy_at(prefix, self.policy);
        let mut before_files = Vec::new();
        if redirect.is_none()
            && let Some(registry) = &self.registry
        {
            let prepared = registry.resolve_backends(
                &crate::pipeline::backend::BackendContext {
                    state: self.state,
                    host: self.host,
                    host_config: self.config,
                    route: &config,
                    prefix,
                    policy,
                    transport: self.transport,
                    protocol: self.protocol,
                    listener: self.listener,
                },
            )?;
            before_files = prepared.before_files;
            plan.steps.extend(prepared.after_files);
        }
        let scope = ScopePlan {
            applications,
            steps,
            before_files,
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
            redirect,
        };
        let mut extensions = HostPipelineBuilder::new();
        let prepared_scope = crate::pipeline::extension::PreparedScope {
            config: &config,
            policy,
            accepts_applications: accept_applications,
        };
        for layer in layers {
            extensions
                .layer(crate::pipeline::layers::ScopedExtension(
                    layer.as_ref(),
                    &prepared_scope,
                ))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        if let Some(registry) = &self.registry {
            registry
                .prepare_registered_layers(
                    &super::LayerContext {
                        state: self.state,
                        host: self.host,
                        host_config: self.config,
                        prefix,
                        protocol: self.protocol,
                        listener: self.listener,
                        scope: prepared_scope,
                    },
                    &mut extensions,
                )
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        for layer in self.layers {
            extensions
                .layer(crate::pipeline::layers::ScopedExtension(
                    layer.as_ref(),
                    &prepared_scope,
                ))
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
        builder: HostPipelineBuilder<S>,
        extensions: HostPipelineBuilder<S>,
        scope: ScopePlan<'_>,
    ) -> Result<HostExecution> {
        #[cfg(feature = "metrics")]
        if self.metrics {
            let mut measured = HostPipelineBuilder::new();
            measured
                .layer(MetricsLayer)
                .map_err(|error| Error::msg(error.to_string()))?;
            measured.append(builder.map_state(MetricsState::inner_mut));
            return self.finish_dispatch(
                measured,
                extensions.map_state(MetricsState::inner_mut),
                scope,
            );
        }
        self.finish_dispatch(builder, extensions, scope)
    }

    fn finish_dispatch<S: Default + Send + 'static>(
        &self,
        mut builder: HostPipelineBuilder<S>,
        extensions: HostPipelineBuilder<S>,
        scope: ScopePlan<'_>,
    ) -> Result<HostExecution> {
        let ScopePlan {
            applications,
            steps,
            before_files,
            backends,
            fallback,
            policy,
            accept_applications,
            body,
            redirect,
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
        if let Some(redirect) = redirect {
            builder
                .layer(redirect)
                .map_err(|error| Error::msg(error.to_string()))?;
            return Ok(builder.build().into_execution());
        }
        if accept_applications && let Some(router) = &self.router {
            if builder.is_empty() {
                return Ok(HostExecution::router(router.clone(), policy));
            }
            builder
                .layer(Backend::new(router.clone(), policy))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        for backend in before_files {
            builder
                .layer(Backend::new(backend, policy))
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
        config: Arc<crate::config::HostConfig>,
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
            HostCompileTarget { host, listener: None },
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
        host_config: Arc<crate::config::HostConfig>,
        router: Option<RustRouter>,
        contributions: &HostContributions,
        #[cfg(feature = "rate-limit")] quotas: Option<
            &crate::pipeline::layers::HostQuotas,
        >,
        #[cfg(feature = "websocket")] websocket: Option<
            Arc<crate::pipeline::H1UpgradeRegistry>,
        >,
    ) -> Result<HostExecution> {
        let HostCompileTarget { host, listener } = target;
        // Only the upgrade decision reads the connection terms; everything
        // prepared below serves this host's site.
        let config = host_config.site().clone();
        let policy = listener
            .map_or_else(HttpPolicy::unlimited, |listener| listener.policy());
        let protocol = listener.map(RouteProtocol::from).or(self.protocol);
        let state = self.state;
        let compiler = Compiler {
            host,
            listener,
            protocol,
            config: &config,
            transport: &host_config.transport,
            policy,
            headers: self.http.map(|http| http.headers.as_ref()),
            #[cfg(feature = "metrics")]
            metrics: self.http.is_some_and(|http| http.metrics.is_some()),
            router,
            #[cfg(feature = "rate-limit")]
            quotas,
            state,
            applications: &contributions.applications,
            layers: &contributions.layers,
            registry: state.registry_ref().resolve::<HostAppRegistry>(),
            proxy: listener.and_then(ProxyLayer::for_listener),
            deny: self
                .http
                .and_then(|http| DenyLayer::from_matcher(&http.deny)),
            acl: AclLayer::for_host(&config),
            #[cfg(feature = "websocket")]
            websocket: crate::pipeline::layers::WebSocketStage::prepare(
                state,
                host,
                &host_config.transport,
                listener,
                websocket,
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
                &host_config,
                &RouteContext {
                    state,
                    host,
                    config: config.as_ref(),
                    route: fallback.as_ref(),
                    prefix: &root,
                },
                protocol,
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
                            &host_config,
                            &RouteContext {
                                state,
                                host,
                                config: config.as_ref(),
                                route: route.as_ref(),
                                prefix,
                            },
                            false,
                            protocol,
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
