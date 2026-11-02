use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[cfg(feature = "broadcast")]
pub(crate) mod broadcast;

#[cfg(feature = "axum")]
use axum::Router;
use claviron_http::ext::HostKey;
#[cfg(feature = "websocket")]
use claviron_http::upgrade::{H1UpgradeRegistry, H1UpgradeRoute};
use claviron_runtime::AppState;
use claviron_runtime::registry::{
    Bootable, Component, Error, Reloadable, Resolve, Result, priority,
};
use claviron_types::Host;
use tracing::error;

use crate::VhostResolver;
use crate::config::HostConfig;
use crate::pipeline::host::{HostExecution, HostHandler};
use crate::pipeline::other::OtherService;
use crate::pipeline::routable::Routable;
use crate::pipeline::routable::{HostBindingProvider, RouteBinding};

/// An application-owned Rust router selected inside a host pipeline.
/// Neither choice owns or bypasses the surrounding host layers.
#[derive(Clone)]
pub enum RustRouter {
    #[cfg(feature = "axum")]
    /// An application-owned Axum router.
    Axum(Router),
    /// An application-owned Rust handler/router without Axum.
    Other(OtherService),
}

impl RustRouter {
    pub fn kind(&self) -> &'static str {
        match self {
            #[cfg(feature = "axum")]
            Self::Axum(_) => "axum",
            Self::Other(_) => "other",
        }
    }
}

use crate::pipeline::extension::{
    RouteContext, RouteEffect, RouteExtension, RouteLayer,
};

/// Prepared route-extension effects, resolved once during host composition.
#[derive(Default)]
pub(crate) struct ResolvedRouteExtensions {
    pub(crate) handler: Option<Arc<dyn HostHandler>>,
    pub(crate) conditions:
        Vec<Arc<dyn claviron_http::dispatch::RouteCondition>>,
    pub(crate) layers: Vec<Arc<dyn RouteLayer>>,
}

/// Prepared host executions and declaration identities for one publication.
/// Canonical names and aliases retain the same execution owners. Reload builds
/// changed hosts before publication; in-flight requests retain their old owner.
#[derive(Clone, Default)]
pub struct HostAppSnapshot {
    map: HashMap<Host, HostExecution>,
    configs: HashMap<Host, Arc<HostConfig>>,
    #[cfg(feature = "rate-limit")]
    quotas:
        Arc<HashMap<Host, Option<Arc<crate::pipeline::layers::HostQuotas>>>>,
}

impl HostAppSnapshot {
    /// Look up the prepared execution for an exact canonical name or alias.
    pub fn resolve(
        &self,
        host: &Host,
    ) -> Option<HostExecution> {
        let app = self.map.get(host).cloned();
        match app.as_ref() {
            Some(app) => {
                tracing::debug!(
                    "resolved host execution for host={}, kind={}",
                    host,
                    app.kind()
                )
            }
            None => {
                tracing::debug!("no host execution resolved for host={}", host)
            }
        }
        app
    }

    /// Insert a prepared host execution, rejecting duplicate keys.
    pub fn insert(
        &mut self,
        host: Host,
        app: HostExecution,
    ) -> Result<()> {
        match self.map.entry(host) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                Err(Error::msg(format!(
                    "duplicate vhost router binding for host '{}'",
                    entry.key()
                )))
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(app);
                Ok(())
            }
        }
    }
}

/// Hot-swappable container for the live [`HostAppSnapshot`].
///
/// # Lifecycle
///
/// 1. **Boot:** registers routing preparation with [`VhostResolver`].
///    The ready `HostAppSnapshot` belongs to that table publication.
/// 2. **Reload:** explicit reload rebuilds factories; watched vhost changes
///    rebuild only changed host entries, retaining unchanged router owners.
///    A failed candidate never replaces the published host/router view.
/// 3. **Hot path:** native admission captures the ready application with
///    its `VhostCtx`. Existing connections keep that value across reloads;
///    standalone callers can still use this resolver's lock-free lookup.
///
/// # Where it lives
///
/// Registered as a `Component<AppState>` on the runtime registry under
/// the name `"VhostAppResolver"`. Consumers (the per-conn dispatcher
/// in `crate::service::HostService`) resolve it once at boot and call
/// `resolve(...)` per request.
///
/// # Why two structs (Snapshot + Resolver)?
///
/// Splitting them lets the snapshot be `Clone` and treated as data
/// (cheap to pass around, easy to inspect in tests), while the resolver
/// exposes the synchronization owner (`VhostResolver`). An unbooted resolver
/// returns an empty view instead of retaining a second serving snapshot.
#[derive(Clone, Default)]
pub struct HostAppResolver {
    empty: Arc<HostAppSnapshot>,
    vhosts: Arc<std::sync::OnceLock<std::sync::Weak<VhostResolver>>>,
}

impl HostAppResolver {
    /// Cheap-clone the current snapshot. Each `Arc` clone is one
    /// relaxed atomic increment; the underlying map is shared.
    pub fn snapshot(&self) -> Arc<HostAppSnapshot> {
        if let Some(vhosts) =
            self.vhosts.get().and_then(std::sync::Weak::upgrade)
            && let Some(apps) = vhosts.snapshot().prepared::<HostAppSnapshot>()
        {
            return apps;
        }
        self.empty.clone()
    }

    /// Per-request lookup: snapshot once, then `HashMap::get`. Used by
    /// the connection-handling hot path (`crate::service::HostService`).
    pub fn resolve(
        &self,
        key: &HostKey,
    ) -> Option<HostExecution> {
        self.snapshot().resolve(key.0.as_ref())
    }
}

/// What the host routes are compiled from: the registered applications and
/// the vhost table they cover. The services an application may use (Lua,
/// broadcast, upstream, the HTTP defaults) are looked up while compiling and
/// skipped when absent; `LATE` lets them boot first.
type ResolverDeps = (Arc<HostAppRegistry>, Arc<VhostResolver>);

impl Component<AppState> for HostAppResolver {
    type Deps = ResolverDeps;

    fn name(&self) -> &'static str {
        "VhostAppResolver"
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl Bootable<AppState, ResolverDeps> for HostAppResolver {
    async fn boot(
        &self,
        (apps, vhosts): ResolverDeps,
        state: &AppState,
    ) -> Result<()> {
        let _ = self.vhosts.set(Arc::downgrade(&vhosts));
        let state = state.clone();
        vhosts
            .prepare_publications(
                move |table, previous: Option<&HostAppSnapshot>| {
                    apps.build_table(&state, table, previous)
                        .map_err(crate::Error::other)
                },
            )
            .map_err(|error| Error::msg(error.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Reloadable<AppState> for HostAppResolver {
    fn priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        let (apps, vhosts) =
            <ResolverDeps as Resolve<AppState>>::resolve(state.registry_ref())?;
        // Full reload can change factory dependencies without changing a host.
        // Replace the preparation callback to force this first cold rebuild.
        let state = state.clone();
        vhosts
            .prepare_publications(
                move |table, previous: Option<&HostAppSnapshot>| {
                    apps.build_table(&state, table, previous)
                        .map_err(crate::Error::other)
                },
            )
            .map_err(|error| Error::msg(error.to_string()))?;
        Ok(())
    }
}

/// Cold factory for an application-owned Axum router.
/// The caller owns its application layers; shared host policy still applies.
///
/// [`register_router`]: HostAppRegistry::register_router
#[cfg(feature = "axum")]
pub type AxumRouterFactory =
    dyn Fn(AppState) -> Router<()> + Send + Sync + 'static;

/// Cold factory for another Rust request owner.
/// It receives the canonical host and effective config. Factory errors prevent
/// publication instead of waiting for the first request.
///
/// [`register_other`]: HostAppRegistry::register_other
pub type OtherFactory = dyn Fn(AppState, Host, Arc<HostConfig>) -> Result<OtherService>
    + Send
    + Sync
    + 'static;

/// Explicit whole-host Rust router factories, prepared at boot/reload.
#[derive(Clone)]
enum Binding {
    #[cfg(feature = "axum")]
    Axum(Arc<AxumRouterFactory>),
    Other(Arc<OtherFactory>),
}

impl Binding {
    fn from_route_binding(binding: RouteBinding) -> Self {
        match binding {
            #[cfg(feature = "axum")]
            RouteBinding::Axum(factory) => Self::Axum(factory),
            RouteBinding::Other(factory) => Self::Other(factory),
        }
    }

    fn build_router(
        &self,
        state: &AppState,
        canonical: &Host,
        host_cfg: Arc<HostConfig>,
    ) -> Result<RustRouter> {
        match self {
            Self::Other(factory) => Ok(RustRouter::Other(factory(
                state.clone(),
                canonical.clone(),
                host_cfg,
            )?)),
            #[cfg(feature = "axum")]
            Self::Axum(factory) => Ok(RustRouter::Axum(factory(state.clone()))),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct HostContributions {
    app: Option<Binding>,
    pub(crate) applications: Vec<Arc<dyn HostHandler>>,
    pub(crate) layers: Vec<Arc<dyn RouteLayer>>,
    #[cfg(feature = "websocket")]
    h1_upgrades: Vec<H1UpgradeRoute>,
}

/// Bootstrap-time registry for code-defined vhost bindings.
///
/// # Position in the design
///
/// claviron has two registration channels for vhosts:
///
/// 1. **Config-driven** — `config/*.yaml` declares hosts, TLS aliases,
///    upstreams, statics, ACL, etc. These hosts compile a native
///    host pipeline. Axum is entered only when
///    a Rust contribution explicitly adapts its own router.
///
/// 2. **Code-driven** — the binary calls
///    `register_router` / `register_other` on this registry during
///    bootstrap to attach handwritten Rust handlers to specific
///    hostnames. This is what lets the same binary serve as both an
///    edge server (TLS, h2/h3, statics, ACL — config) and an app
///    server (your business logic — code).
///
/// Both channels use the same host compiler. A code-defined Rust router enters
/// the host pipeline after its active policies and contributions.
///
/// # Why static `&'static str` keys
///
/// The host name passed to each `register_*` call is `&'static str`
/// because these are explicit, code-pinned bindings — not values that
/// change across reloads. Config (`vhosts.yaml`) can add or remove
/// aliases for a canonical host, but the canonical host name itself
/// is the contract between code and config and isn't expected to
/// move.
///
/// # Reloads
///
/// Registrations survive across reloads — they're owned by this
/// `Component`, which is registered once at boot and read on every
/// `HostAppResolver::reload`. Config can change freely; the code
/// bindings stay pinned. If you remove a `register_*` call and
/// rebuild the binary, the old binding disappears on the next start.
///
/// # Example
///
/// ```
/// use claviron_edge::pipeline::{HostAppRegistry, other::OtherService};
///
/// struct Handler;
/// impl claviron_edge::pipeline::other::OtherHandler for Handler {
///     fn call(&self, request: http::Request<claviron_http::RouteBody>) -> claviron_http::RouteFuture {
///         Box::pin(async move {
///             drop(request);
///             Ok(http::Response::new(claviron_http::empty_body()))
///         })
///     }
/// }
/// let registry = HostAppRegistry::default();
/// registry.register_other("assets.example.com", |_, _, _| {
///     Ok(OtherService::new(Handler))
/// });
/// ```
#[derive(Default)]
pub struct HostAppRegistry {
    inner: RwLock<HashMap<String, Binding>>,
    routables: RwLock<Vec<Arc<dyn Routable>>>,
    route_extensions: RwLock<Vec<Arc<dyn RouteExtension>>>,
    binding_providers: RwLock<Vec<Arc<dyn HostBindingProvider>>>,
    #[cfg(feature = "rate-limit")]
    quotas: RwLock<crate::pipeline::layers::QuotaRegistrations>,
}

impl HostAppRegistry {
    /// Bind a prepared limiter to this canonical host/prefix. It runs at the
    /// same head-admission point as config, before the selected Rust router
    /// or backend. Native H2 CONNECT and H3 session admission is separate.
    /// Register before the first host preparation; duplicate config/Rust
    /// definitions are errors. The limiter retains its own KeyProvider.
    #[cfg(feature = "rate-limit")]
    pub fn register_quota<
        K: claviron_http::rate_limit::KeyProvider + 'static,
    >(
        &self,
        host: Host,
        prefix: claviron_http::ext::PathPrefix,
        limiter: claviron_http::rate_limit::RateLimiter<K>,
    ) -> Result<()> {
        self.quotas
            .write()
            .expect("quota registration lock poisoned")
            .register(host, prefix, limiter)
    }
    /// Register a host-independent directive compiler before snapshot build.
    pub fn register_route_extension(
        &self,
        extension: Arc<dyn RouteExtension>,
    ) -> Result<()> {
        let mut extensions = self
            .route_extensions
            .write()
            .expect("route extensions lock poisoned");
        if extensions.iter().any(|existing| existing.name() == extension.name())
        {
            return Err(Error::msg(format!(
                "route extension '{}' is already registered",
                extension.name()
            )));
        }
        extensions.push(extension);
        Ok(())
    }

    /// Register a complete Axum router for this host.
    ///
    /// The factory runs at boot/reload. The router enters the same host scope
    /// compiler as configured backends, after the scope's active policy layers.
    /// Its response, including a 404, ends request dispatch.
    #[cfg(feature = "axum")]
    pub fn register_router<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState) -> Router + Send + Sync + 'static,
    {
        let router: Arc<AxumRouterFactory> =
            Arc::new(move |state: AppState| factory(state));

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.to_string(), Binding::Axum(router));
    }

    /// Register another Rust request owner for this host.
    ///
    /// The factory receives the canonical host and config at boot/reload.
    /// The Rust handler/router enters the same host scope compiler as Axum.
    /// Its response ends dispatch; router selection does not skip host layers.
    /// Factory errors prevent host publication.
    pub fn register_other<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState, Host, Arc<HostConfig>) -> Result<OtherService>
            + Send
            + Sync
            + 'static,
    {
        let f: Arc<OtherFactory> = Arc::new(
            move |state: AppState, canonical: Host, cfg: Arc<HostConfig>| {
                factory(state, canonical, cfg)
            },
        );

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.to_string(), Binding::Other(f));
    }

    /// Register a provider-shaped route contributor.
    ///
    /// Native contributions are evaluated during
    /// [`build`](Self::build), and the resulting binding is frozen into the
    /// `HostAppSnapshot`. The request hot path remains a plain
    /// `Host -> HostExecution` lookup.
    pub fn register_routable<R>(
        &self,
        routable: Arc<R>,
    ) where
        R: Routable + 'static,
    {
        self.routables
            .write()
            .expect("host app routables lock poisoned")
            .push(routable);
    }

    pub fn register_binding_provider<R>(
        &self,
        provider: Arc<R>,
    ) where
        R: HostBindingProvider + 'static,
    {
        self.binding_providers
            .write()
            .expect("host binding providers lock poisoned")
            .push(provider);
    }

    pub(crate) fn resolve_host_layers(
        &self,
        context: &RouteContext<'_>,
    ) -> Result<Vec<Arc<dyn RouteLayer>>> {
        self.resolve_extensions(context, true).map(|resolved| resolved.layers)
    }

    pub(crate) fn resolve_extensions(
        &self,
        context: &RouteContext<'_>,
        inherited_only: bool,
    ) -> Result<ResolvedRouteExtensions> {
        let providers = self
            .routables
            .read()
            .expect("host app routables lock poisoned")
            .clone();
        let mut codecs = HashMap::new();
        for codec in self
            .route_extensions
            .read()
            .expect("route extensions lock poisoned")
            .iter()
        {
            codecs.insert(codec.name(), codec.clone());
        }
        for provider in providers {
            if !provider.match_host(context.host, context.config) {
                continue;
            }
            for codec in provider.route_extensions() {
                let name = codec.name();
                if matches!(
                    name,
                    "php"
                        | "js"
                        | "javascript"
                        | "lua"
                        | "orbit"
                        | "static"
                        | "spa"
                        | "upstream"
                        | "pusher"
                        | "broadcast"
                ) || codecs.insert(name, codec).is_some()
                {
                    return Err(Error::msg(format!(
                        "route extension '{name}' has multiple owners"
                    )));
                }
            }
        }
        let config = context.route;
        let mut handler = None;
        let mut conditions = Vec::new();
        let mut layers = Vec::new();
        let mut directives = std::collections::BTreeMap::new();
        for (name, value) in &context.config.extensions {
            if codecs
                .get(name.as_str())
                .is_some_and(|codec| codec.inherit_from_host())
            {
                directives.insert(name, value);
            }
        }
        if !inherited_only {
            directives.extend(config.extensions.iter());
        }
        for (name, value) in directives {
            let codec = codecs.get(name.as_str()).ok_or_else(|| {
                Error::msg(format!(
                    "unresolved route directive '{name}' at '{}'",
                    context.prefix
                ))
            })?;
            let effect = codec.resolve(context, value)?;
            if codec.inherit_from_host()
                && !matches!(&effect, RouteEffect::Layer(_))
            {
                return Err(Error::msg(format!(
                    "inherited route directive '{name}' must resolve to a layer"
                )));
            }
            match effect {
                RouteEffect::Handler(application) => {
                    if handler.is_some() {
                        return Err(Error::msg(format!(
                            "route '{}' has multiple Rust handler directives",
                            context.prefix
                        )));
                    }
                    handler = Some(application);
                }
                RouteEffect::Condition(condition) => conditions.push(condition),
                RouteEffect::Layer(layer) => layers.push(layer),
            }
        }
        Ok(ResolvedRouteExtensions { handler, conditions, layers })
    }

    fn routable_binding(
        &self,
        state: &AppState,
        canonical: &Host,
        host_cfg: Arc<HostConfig>,
    ) -> Result<HostContributions> {
        let mut applications = Vec::new();
        let mut layers = Vec::new();
        let mut exclusive: Option<(&str, Binding)> = None;
        #[cfg(feature = "websocket")]
        let mut h1_upgrades = Vec::new();

        let local_routables = self
            .routables
            .read()
            .expect("host app routables lock poisoned")
            .clone();
        for routable in &local_routables {
            if !routable.match_host(canonical, host_cfg.as_ref()) {
                continue;
            }
            if let Some(application) = Arc::clone(routable).application(
                state,
                canonical,
                Arc::clone(&host_cfg),
            )? {
                applications.push(application);
            }
            layers.extend(Arc::clone(routable).layers(
                state,
                canonical,
                Arc::clone(&host_cfg),
            )?);
            #[cfg(feature = "websocket")]
            h1_upgrades.extend(
                routable.h1_upgrades(canonical, Arc::clone(&host_cfg))?,
            );
        }

        let local_bindings = self
            .binding_providers
            .read()
            .expect("host binding providers lock poisoned")
            .clone();
        for provider in &local_bindings {
            if !provider.match_host(canonical, host_cfg.as_ref()) {
                continue;
            }
            let Some(binding) =
                provider.binding(canonical, Arc::clone(&host_cfg))?
            else {
                continue;
            };
            if let Some((previous, _)) = exclusive.as_ref() {
                return Err(Error::msg(format!(
                    "multiple exclusive host binding providers matched host '{}': '{}' and '{}'",
                    canonical,
                    previous,
                    provider.name(),
                )));
            }
            exclusive =
                Some((provider.name(), Binding::from_route_binding(binding)));
        }

        Ok(HostContributions {
            app: exclusive.map(|(_, binding)| binding),
            applications,
            layers,
            #[cfg(feature = "websocket")]
            h1_upgrades,
        })
    }

    /// Build the live [`HostAppSnapshot`] from registered factories
    /// and the current config.
    ///
    /// # What it does
    ///
    /// 1. Resolves the [`VhostResolver`] from the runtime registry
    ///    to get the current alias/canonical host table.
    /// 2. For each `register_*` binding, parses the `&'static str`
    ///    key into a [`Host`] and checks it has a canonical entry
    ///    in `vhosts.yaml`. Missing entries are **logged and
    ///    skipped** — registering a key that ops haven't added to
    ///    config is a soft error, not a hard failure.
    /// 3. For every canonical host in the resolver:
    ///    - Prepare its optional code-defined Rust router.
    ///    - Compile that router, native backends and active host layers through
    ///      the same scope compiler.
    /// 4. Stamp the resulting `HostExecution` against **every alias** for
    ///    the canonical host so per-request lookup is one hash hit.
    ///
    /// # When it runs
    ///
    /// `Bootable::boot` calls this once at startup; `Reloadable::reload`
    /// calls it again on config reload. The hot path never invokes
    /// `build` — it only reads the swapped-in `HostAppSnapshot` via
    /// [`HostAppResolver::resolve`].
    pub fn build(
        &self,
        state: &AppState,
        vhosts: &VhostResolver,
    ) -> Result<HostAppSnapshot> {
        let table = vhosts.snapshot();
        self.build_table(state, &table, None)
    }

    fn build_table(
        &self,
        state: &AppState,
        table: &crate::loader::table::VhostTable,
        previous: Option<&HostAppSnapshot>,
    ) -> Result<HostAppSnapshot> {
        #[cfg(feature = "rate-limit")]
        let quotas = {
            let previous = previous
                .map(|snapshot| snapshot.quotas.clone())
                .or_else(|| {
                    state
                        .registry_ref()
                        .resolve::<VhostResolver>()?
                        .snapshot()
                        .prepared::<HostAppSnapshot>()
                        .map(|snapshot| snapshot.quotas.clone())
                });
            let registered = {
                let mut registered = self
                    .quotas
                    .write()
                    .expect("quota registration lock poisoned");
                registered.seal(state.fleet_name(), table)?;
                registered.clone()
            };
            let config_check = state
                .registry_ref()
                .resolve::<claviron_runtime::ConfigCheck>()
                .is_some();
            table
                .canonical_entries()
                .map(|(canonical, record)| {
                    registered
                        .prepare(
                            config_check,
                            canonical,
                            &record.config,
                            previous
                                .as_ref()
                                .and_then(|quotas| quotas.get(canonical)),
                        )
                        .map(|quotas| (canonical.clone(), quotas))
                })
                .collect::<Result<HashMap<_, _>>>()?
        };
        let mut snapshot = HostAppSnapshot {
            map: HashMap::new(),
            configs: HashMap::new(),
            #[cfg(feature = "rate-limit")]
            quotas: Arc::new(quotas),
        };
        let mut custom_bindings = HashMap::<Host, Binding>::new();
        let custom_bindings_snapshot =
            self.inner.read().expect("host app registry lock poisoned").clone();

        for (key, binding) in &custom_bindings_snapshot {
            let canonical: Host =
                key.parse().map_err(|e: String| Error::msg(e))?;

            if table.canonical_config(&canonical).is_none() {
                error!(
                    "registered vhost key '{}' has no canonical config; skipping custom router binding",
                    canonical
                );
                continue;
            }

            custom_bindings.insert(canonical, binding.clone());
        }

        for (canonical, record) in table.canonical_entries() {
            if let Some(previous) = previous
                && previous
                    .configs
                    .get(canonical)
                    .is_some_and(|config| Arc::ptr_eq(config, &record.config))
                && let Some(app) = previous.map.get(canonical)
            {
                for host in table.host_keys_for_canonical(canonical) {
                    snapshot.insert(host, app.clone())?;
                }
                snapshot
                    .configs
                    .insert(canonical.clone(), record.config.clone());
                continue;
            }
            // Only the upgrade features below mutate it.
            #[cfg_attr(
                not(any(
                    all(feature = "lua", feature = "websocket"),
                    feature = "broadcast"
                )),
                allow(unused_mut)
            )]
            let mut routable = self.routable_binding(
                state,
                canonical,
                Arc::clone(&record.config),
            )?;
            #[cfg(all(feature = "lua", feature = "websocket"))]
            super::embed::lua::append_websocket_routes(
                state,
                canonical,
                &record.config,
                &mut routable.h1_upgrades,
            )?;
            #[cfg(all(feature = "lua", feature = "webtransport"))]
            super::embed::lua::append_webtransport_routes(
                state,
                canonical,
                &record.config,
            )?;
            #[cfg(all(feature = "orbit", feature = "webtransport"))]
            super::link::webtransport::bind(state, canonical, &record.config)?;
            #[cfg(feature = "broadcast")]
            broadcast::append_routes(
                state,
                canonical,
                &record.config,
                &mut routable.h1_upgrades,
            )?;
            let binding = match custom_bindings.get(canonical).cloned() {
                Some(binding) => {
                    if routable.app.is_some() {
                        return Err(Error::msg(format!(
                            "host '{}' has a manual exclusive binding and a routable app binding",
                            canonical
                        )));
                    }
                    Some(binding)
                }
                None => routable.app.clone(),
            };

            let router = binding
                .map(|binding| {
                    binding.build_router(
                        state,
                        canonical,
                        record.config.clone(),
                    )
                })
                .transpose()?;
            let app = build_host(
                state,
                canonical,
                record.config.clone(),
                router,
                routable,
                #[cfg(feature = "rate-limit")]
                snapshot.quotas.get(canonical).and_then(Option::as_deref),
            )?;

            snapshot.configs.insert(canonical.clone(), record.config.clone());
            for host in table.host_keys_for_canonical(canonical) {
                snapshot.insert(host, app.clone())?;
            }
        }

        Ok(snapshot)
    }
}

fn build_host(
    state: &AppState,
    canonical: &Host,
    host_cfg: Arc<HostConfig>,
    router: Option<RustRouter>,
    contributions: HostContributions,
    #[cfg(feature = "rate-limit")] quotas: Option<
        &crate::pipeline::layers::HostQuotas,
    >,
) -> Result<HostExecution> {
    #[cfg(feature = "websocket")]
    let mut contributions = contributions;
    #[cfg(feature = "websocket")]
    let websocket = build_h1_upgrade_registry(canonical, std::mem::take(&mut contributions.h1_upgrades))?;
    super::host::compile::prepare(
        state,
        canonical,
        host_cfg,
        router,
        &contributions,
        #[cfg(feature = "rate-limit")]
        quotas,
        #[cfg(feature = "websocket")]
        websocket,
    )
}

#[cfg(feature = "websocket")]
fn build_h1_upgrade_registry(
    canonical: &Host,
    routes: Vec<H1UpgradeRoute>,
) -> Result<Option<Arc<H1UpgradeRegistry>>> {
    if routes.is_empty() {
        return Ok(None);
    }

    let registry = Arc::new(H1UpgradeRegistry::default());
    for route in routes {
        registry.register_route(canonical.clone(), route)?;
    }
    Ok(Some(registry))
}

#[cfg(all(test, feature = "rate-limit"))]
#[path = "../../tests/pipeline/quota.rs"]
mod quota_tests;

#[cfg(all(test, feature = "axum"))]
#[path = "../../tests/pipeline/apps.rs"]
mod application_tests;
