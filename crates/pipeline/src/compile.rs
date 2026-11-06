//! Compile changed host declarations into immutable native dispatch owners.

use std::sync::Arc;

use claviron_http::RustRouter;
use claviron_http::config::transport::TransportPolicy;
use claviron_http::config::{
    HttpPolicy, HttpSnapshot, ListenerConfig, routes::RouteTable,
};
use claviron_http::dispatch::{ConfiguredRoute, RouteDispatch};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;

use crate::extension::{PreparedScope, RouteContext, RouteLayer};
use crate::factory::{BackendContext, RouteProtocol};
#[cfg(feature = "rate-limit")]
use crate::layers::HostQuotas;
#[cfg(feature = "access-log")]
use crate::layers::{AccessLogLayer, AccessLogState};
use crate::layers::{
    AclLayer, CorsLayer, CorsState, DenyLayer, ProxyLayer, RedirectLayer,
    ReplyPolicy, RequestBodyLayer, RouteSelectionLayer, prepare_fallback,
    prepare_headers, scope_steps,
};
#[cfg(feature = "metrics")]
use crate::layers::{MetricsLayer, MetricsState};
use crate::plan::{Backend, HostExecution, HostPipelineBuilder};
use crate::registry::{
    ContributedExtensions, ResolvedRouteExtensions, SiteRegistry,
};
use crate::{BackendHandler, DispatchStep, HostHandler, Site};

/// The owners a scope starts from before its registered backends, and its file
/// fallback.
#[derive(Clone)]
struct BackendPlan {
    steps: Vec<Arc<dyn BackendHandler>>,
    fallback: Option<Arc<dyn DispatchStep>>,
}

impl BackendPlan {
    fn prepare(
        context: &RouteContext<'_>,
        hooks: &dyn CompileHooks,
    ) -> Self {
        if context
            .setting(|scope| scope.redirect.as_ref())
            .is_some_and(|rules| !rules.is_empty())
        {
            return Self { steps: Vec::new(), fallback: None };
        }
        Self {
            steps: hooks.scope_backends(context),
            fallback: prepare_fallback(context.state, context.route),
        }
    }
}

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
    steps: Vec<Arc<dyn DispatchStep>>,
    before_files: Vec<Arc<dyn BackendHandler>>,
    backends: Vec<Arc<dyn BackendHandler>>,
    fallback: Option<Arc<dyn DispatchStep>>,
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
impl AsMut<()> for LoggedState<crate::layers::QuotaState> {
    fn as_mut(&mut self) -> &mut () {
        self.admission.as_mut()
    }
}

#[cfg(all(feature = "access-log", feature = "rate-limit"))]
impl AsMut<crate::layers::QuotaState>
    for LoggedState<crate::layers::QuotaState>
{
    fn as_mut(&mut self) -> &mut crate::layers::QuotaState {
        &mut self.admission
    }
}

/// What a site's preparation needs beyond the site: everything that depends on
/// how it is being served. All of it is borrowed; nothing here is retained by
/// dispatch unless a factory explicitly owns it.
pub struct CompileInput<'a> {
    pub state: &'a AppState,
    /// The canonical host name, when the site is served as a virtual host.
    pub host: Option<&'a Host>,
    pub site: Arc<Site>,
    /// What the connection's terms allow an owner to take over.
    pub transport: &'a TransportPolicy,
    pub listener: Option<&'a ListenerConfig>,
    pub protocol: Option<RouteProtocol>,
    pub http: Option<&'a HttpSnapshot>,
    pub router: Option<RustRouter>,
    pub applications: &'a [Arc<dyn HostHandler>],
    pub layers: &'a [Arc<dyn RouteLayer>],
    pub registry: Option<&'a SiteRegistry>,
    pub contributed: ContributedExtensions,
    pub hooks: &'a dyn CompileHooks,
    #[cfg(feature = "rate-limit")]
    pub quotas: Option<&'a HostQuotas>,
    /// Answers upgrades before the site's applications when one is prepared.
    pub upgrades: Option<Arc<dyn BackendHandler>>,
    /// The bounds the site narrows, for a side with no listener to take them
    /// from. A listener's own policy wins otherwise.
    pub policy: Option<HttpPolicy>,
}

/// What the serving side adds to each scope of a site while it is prepared.
pub trait CompileHooks: Send + Sync {
    /// Owners the scope selects by itself, ahead of the registered backends.
    fn scope_backends(
        &self,
        _context: &RouteContext<'_>,
    ) -> Vec<Arc<dyn BackendHandler>> {
        Vec::new()
    }

}

/// No additions: a site served as itself.
pub struct NoHooks;

impl CompileHooks for NoHooks {}

struct Compiler<'a> {
    host: Option<&'a Host>,
    listener: Option<&'a claviron_http::config::ListenerConfig>,
    protocol: Option<RouteProtocol>,
    config: &'a Site,
    transport: &'a claviron_http::config::transport::TransportPolicy,
    policy: HttpPolicy,
    headers: Option<&'a claviron_http::config::host::HostHeadersConfig>,
    #[cfg(feature = "metrics")]
    metrics: bool,
    #[cfg(feature = "rate-limit")]
    quotas: Option<&'a crate::layers::HostQuotas>,
    state: &'a AppState,
    router: Option<RustRouter>,
    applications: &'a [Arc<dyn HostHandler>],
    layers: &'a [Arc<dyn RouteLayer>],
    registry: Option<&'a SiteRegistry>,
    proxy: Option<ProxyLayer>,
    deny: Option<DenyLayer>,
    acl: Option<AclLayer>,
    upgrades: Option<Arc<dyn BackendHandler>>,
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
            let prepared = registry.resolve_backends(&BackendContext {
                state: self.state,
                host: self.host,
                site: self.config,
                route: &config,
                prefix,
                policy,
                transport: self.transport,
                protocol: self.protocol,
                listener: self.listener,
            })?;
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
        let prepared_scope = PreparedScope {
            config: &config,
            policy,
            accepts_applications: accept_applications,
        };
        for layer in layers {
            extensions
                .layer(crate::layers::ScopedExtension(
                    layer.as_ref(),
                    &prepared_scope,
                ))
                .map_err(|error| Error::msg(error.to_string()))?;
        }
        for layer in self.layers {
            extensions
                .layer(crate::layers::ScopedExtension(
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
                let mut builder = builder
                    .with_state::<LoggedState<crate::layers::QuotaState>>();
                builder
                    .layer(log)
                    .map_err(|error| Error::msg(error.to_string()))?;
                let mut access = access.with_state();
                crate::layers::QuotaPolicy::attach_to(&mut access, quotas)
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
            let access = crate::layers::QuotaPolicy::attach(access, quotas)
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
        if accept_applications && let Some(upgrades) = &self.upgrades {
            builder
                .layer(Backend::new(upgrades.clone(), policy))
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

/// The one owner of a site, after its files.
struct SiteOwner(Arc<dyn BackendHandler>);

impl CompileHooks for SiteOwner {
    fn scope_backends(
        &self,
        _context: &RouteContext<'_>,
    ) -> Vec<Arc<dyn BackendHandler>> {
        vec![self.0.clone()]
    }
}

/// Compile a site in front of one owner that answers whatever its files and
/// policy leave, on no listener in particular: `policy` is what it narrows.
pub fn compile_with_owner(
    state: &AppState,
    site: Arc<Site>,
    policy: HttpPolicy,
    owner: Arc<dyn BackendHandler>,
) -> Result<HostExecution> {
    #[cfg(feature = "rate-limit")]
    let quotas = crate::layers::quotas_of_site(&site)?;
    compile(CompileInput {
        state,
        host: None,
        site,
        transport: &TransportPolicy::default(),
        listener: None,
        protocol: None,
        http: None,
        router: None,
        applications: &[],
        layers: &[],
        registry: None,
        contributed: ContributedExtensions::default(),
        hooks: &SiteOwner(owner),
        #[cfg(feature = "rate-limit")]
        quotas: quotas.as_deref(),
        upgrades: None,
        policy: Some(policy),
    })
}

/// Compile a site served as itself, by a listener that selects no host. Its
/// directives resolve against `registry`; a directive nothing owns fails here.
pub fn compile_site(
    state: &AppState,
    site: Arc<Site>,
    listener: &ListenerConfig,
    registry: Option<&SiteRegistry>,
) -> Result<HostExecution> {
    #[cfg(feature = "rate-limit")]
    let quotas = crate::layers::quotas_of_site(&site)?;
    compile(CompileInput {
        state,
        host: None,
        site,
        transport: &TransportPolicy::default(),
        listener: Some(listener),
        protocol: None,
        http: None,
        router: None,
        applications: &[],
        layers: &[],
        registry,
        contributed: ContributedExtensions::default(),
        hooks: &NoHooks,
        #[cfg(feature = "rate-limit")]
        quotas: quotas.as_deref(),
        upgrades: None,
        policy: None,
    })
}

/// Compile a site into its immutable dispatch owner, for one listener's plan
/// or for every protocol when none is given.
pub fn compile(input: CompileInput<'_>) -> Result<HostExecution> {
    let CompileInput {
        state,
        host,
        site: config,
        transport,
        listener,
        protocol,
        http,
        router,
        applications,
        layers,
        registry,
        contributed,
        hooks,
        #[cfg(feature = "rate-limit")]
        quotas,
        upgrades,
        policy,
    } = input;
    let policy = listener.map(ListenerConfig::policy).or(policy);
    let policy = policy.unwrap_or_else(HttpPolicy::unlimited);
    let protocol = listener.map(RouteProtocol::from).or(protocol);
    let compiler = Compiler {
        host,
        listener,
        protocol,
        config: &config,
        transport,
        policy,
        headers: http.map(|http| http.headers.as_ref()),
        #[cfg(feature = "metrics")]
        metrics: http.is_some_and(|http| http.metrics.is_some()),
        router,
        #[cfg(feature = "rate-limit")]
        quotas,
        state,
        applications,
        layers,
        registry,
        proxy: listener.and_then(ProxyLayer::for_listener),
        deny: http.and_then(|http| DenyLayer::from_matcher(&http.deny)),
        acl: AclLayer::for_host(&config),
        upgrades,
    };
    let fallback = config.fallback().cloned().unwrap_or_else(|| config.clone());
    let root = claviron_http::ext::PathPrefix::root();
    let base = BackendPlan::prepare(
        &RouteContext {
            state,
            host,
            config: config.as_ref(),
            route: fallback.as_ref(),
            prefix: &root,
        },
        hooks,
    );
    let layers = match &compiler.registry {
        Some(registry) => {
            registry
                .resolve_extensions(
                    &RouteContext {
                        state,
                        host,
                        config: config.as_ref(),
                        route: fallback.as_ref(),
                        prefix: &root,
                    },
                    true,
                    &contributed,
                )?
                .layers
        }
        None if !config.extensions.is_empty() => {
            return Err(Error::msg("route extensions require a site registry"));
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
                        &contributed,
                    )?,
                    None if route.extensions.is_empty() => {
                        ResolvedRouteExtensions::default()
                    }
                    None => {
                        return Err(Error::msg(
                            "route extensions require a site registry",
                        ));
                    }
                };
                let plan = BackendPlan::prepare(
                    &RouteContext {
                        state,
                        host,
                        config: config.as_ref(),
                        route,
                        prefix,
                    },
                    hooks,
                );
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

#[cfg(test)]
#[path = "../tests/compile.rs"]
mod tests;
