#[cfg(feature = "axum")]
pub mod axum;
pub mod other;
#[cfg(feature = "warp")]
pub mod warp;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
use crate::pipeline::InterceptsNonServing;
use crate::pipeline::backend::BackendFactory;
use crate::pipeline::host::{HostExecution, HostHandler};
use crate::pipeline::routable::Routable;
use crate::pipeline::routable::{
    HostBindingProvider, RouteBinding, RouteProtocol, matches_protocol,
};

pub use claviron_http::RustRouter;

use crate::pipeline::extension::{RouteExtension, RouteLayer};

/// Prepared host executions and declaration identities for one publication.
/// Canonical names and aliases retain the same execution owners. Reload builds
/// changed hosts before publication; in-flight requests retain their old owner.
#[derive(Clone, Default)]
pub struct HostAppSnapshot {
    map: HashMap<Host, HostExecution>,
    listener_plans: HashMap<Host, ListenerPlans>,
    configs: HashMap<Host, Arc<HostConfig>>,
    http: Option<Arc<claviron_http::config::HttpSnapshot>>,
    #[cfg(feature = "tcp")]
    non_serving_interception: Option<Arc<dyn InterceptsNonServing>>,
    #[cfg(feature = "rate-limit")]
    quotas:
        Arc<HashMap<Host, Option<Arc<crate::pipeline::layers::HostQuotas>>>>,
}

pub(crate) type ListenerPlans =
    Arc<[(claviron_http::ListenerId, ListenerHostPlan)]>;

#[derive(Clone)]
pub(crate) enum ListenerHostPlan {
    Application(HostExecution),
    NonServing,
}

impl ListenerHostPlan {
    pub(crate) fn application(&self) -> Option<&HostExecution> {
        match self {
            Self::Application(app) => Some(app),
            Self::NonServing => None,
        }
    }
}

impl HostAppSnapshot {
    #[cfg(feature = "tcp")]
    pub fn non_serving_interception(
        &self
    ) -> Option<Arc<dyn InterceptsNonServing>> {
        self.non_serving_interception.clone()
    }

    pub(crate) fn listener_plans(
        &self,
        host: &Host,
    ) -> Option<&ListenerPlans> {
        self.listener_plans.get(host)
    }
    /// Look up the protocol-independent execution for a canonical name or alias.
    /// Native listener admission selects protocol-specific plans from the same
    /// publication; this unbound lookup excludes restricted contributions.
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
/// in `claviron_transport::edge::service::HostService`) resolve it once at boot and call
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
    /// the connection-handling hot path (`claviron_transport::edge::service::HostService`).
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

/// Explicit whole-host Rust router factories, prepared at boot/reload.
#[derive(Clone)]
enum Binding {
    #[cfg(feature = "axum")]
    Axum(Arc<axum::Factory>),
    #[cfg(feature = "warp")]
    Warp(Arc<warp::Factory>),
    Other(Arc<other::Factory>),
}

impl Binding {
    fn from_route_binding(binding: RouteBinding) -> Self {
        match binding {
            #[cfg(feature = "axum")]
            RouteBinding::Axum(factory) => Self::Axum(factory),
            #[cfg(feature = "warp")]
            RouteBinding::Warp(factory) => Self::Warp(factory),
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
            #[cfg(feature = "warp")]
            Self::Warp(factory) => Ok(RustRouter::Warp(factory(state.clone()))),
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

type NonServingFactory = dyn Fn(&AppState) -> Result<Option<Arc<dyn InterceptsNonServing>>>
    + Send
    + Sync;

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
/// use claviron_edge::pipeline::{HostAppRegistry, apps::other::OtherService};
///
/// struct Handler;
/// impl claviron_edge::pipeline::apps::other::OtherHandler for Handler {
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
    non_serving_interception: RwLock<Option<Arc<NonServingFactory>>>,
    site: claviron_pipeline::SiteRegistry,
    binding_providers: RwLock<Vec<Arc<dyn HostBindingProvider>>>,
    pub(in crate::pipeline) accept_registrations:
        RwLock<Vec<Arc<super::accept::AcceptRegistration>>>,
    #[cfg(feature = "rate-limit")]
    quotas: RwLock<crate::pipeline::layers::QuotaRegistrations>,
}

impl HostAppRegistry {
    /// Prepare the optional interceptor once per plain-listener publication.
    /// Read configuration already staged for this boot/reload. Returning None
    /// installs no hook, request context or body adapter. Errors keep the
    /// previously published listener generation.
    pub fn bind_non_serving_interception_with(
        &self,
        prepare: impl Fn(&AppState) -> Result<Option<Arc<dyn InterceptsNonServing>>>
        + Send
        + Sync
        + 'static,
    ) -> Result<()> {
        let mut slot = self
            .non_serving_interception
            .write()
            .expect("non-serving interception lock poisoned");
        if slot.is_some() {
            return Err(Error::msg(
                "non-serving interception is already bound",
            ));
        }
        *slot = Some(Arc::new(prepare));
        Ok(())
    }

    /// Bind a prepared limiter to this canonical host/prefix. It runs at the
    /// same head-admission point as config, after proxy/Deny/ACL and before
    /// application extension layers, the selected Rust router or backend.
    /// Keys must be available from the accepted connection or incoming head;
    /// later application extensions have not run. Native H2 CONNECT and H3
    /// session admission is separate.
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
        self.site.register_route_extension(extension)
    }

    /// Register a configured execution owner independently of route hooks.
    /// Factories are consulted only while preparing matching host scopes.
    pub fn register_backend_factory(
        &self,
        factory: Arc<dyn BackendFactory>,
    ) -> Result<()> {
        self.site.register_backend_factory(factory)
    }

    /// What this registry holds independently of any host.
    pub fn site(&self) -> &claviron_pipeline::SiteRegistry {
        &self.site
    }

    /// The route extensions a selected host's own providers bring.
    pub(crate) fn contributed_extensions(
        &self,
        host: &Host,
        host_config: &HostConfig,
        protocol: Option<RouteProtocol>,
    ) -> claviron_pipeline::registry::ContributedExtensions {
        let mut contributed =
            claviron_pipeline::registry::ContributedExtensions::default();
        let providers = self
            .routables
            .read()
            .expect("host app routables lock poisoned")
            .clone();
        for provider in providers {
            if !provider.match_host(host, host_config) {
                continue;
            }
            let active = matches_protocol(protocol, |protocol| {
                provider.match_protocol(protocol)
            });
            for codec in provider.route_extensions() {
                if active {
                    contributed.active.push(codec);
                } else {
                    contributed.inactive.insert(codec.name());
                }
            }
        }
        contributed
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

    fn routable_binding(
        &self,
        state: &AppState,
        canonical: &Host,
        host_cfg: Arc<HostConfig>,
        protocol: Option<RouteProtocol>,
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
            if !matches_protocol(protocol, |protocol| {
                routable.match_protocol(protocol)
            }) || !routable.match_host(canonical, host_cfg.as_ref())
            {
                continue;
            }
            routable.prepare_sessions(state, canonical, &host_cfg)?;
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
            if !matches_protocol(protocol, |protocol| {
                provider.match_protocol(protocol)
            }) || !provider.match_host(canonical, host_cfg.as_ref())
            {
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

    fn specializes_on(
        &self,
        host: &Host,
        config: &HostConfig,
        protocol: RouteProtocol,
    ) -> bool {
        self.routables
            .read()
            .expect("host app routables lock poisoned")
            .iter()
            .any(|provider| {
                provider.match_host(host, config)
                    && provider.match_protocol(protocol)
                    && !matches_protocol(None, |protocol| {
                        provider.match_protocol(protocol)
                    })
            })
            || self
                .binding_providers
                .read()
                .expect("host binding providers lock poisoned")
                .iter()
                .any(|provider| {
                    provider.match_host(host, config)
                        && provider.match_protocol(protocol)
                        && !matches_protocol(None, |protocol| {
                            provider.match_protocol(protocol)
                        })
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
        let http = state
            .registry_ref()
            .resolve::<crate::HttpConfigProvider>()
            .map(|provider| {
                let mut input = provider.snapshot().as_ref().clone();
                input.listeners.clear();
                Arc::new(input)
            });
        self.build_table_with_http(state, table, previous, http)
    }

    #[cfg(any(feature = "tcp", feature = "quic"))]
    pub fn prepare_listener(
        &self,
        state: &AppState,
        table: &crate::loader::table::VhostTable,
        input: &claviron_http::config::HttpSnapshot,
        previous: Option<&crate::loader::table::VhostTable>,
    ) -> Result<crate::loader::table::VhostTable> {
        let mut hosts = table.declarations_only();
        let previous =
            previous.and_then(|table| table.prepared::<HostAppSnapshot>());
        let apps = self.build_table_with_http(
            state,
            &hosts,
            previous.as_deref(),
            Some(Arc::new(input.clone())),
        )?;
        hosts.set_prepared(
            std::any::TypeId::of::<HostAppSnapshot>(),
            Arc::new(apps),
        );
        Ok(hosts)
    }

    fn build_table_with_http(
        &self,
        state: &AppState,
        table: &crate::loader::table::VhostTable,
        previous: Option<&HostAppSnapshot>,
        http: Option<Arc<claviron_http::config::HttpSnapshot>>,
    ) -> Result<HostAppSnapshot> {
        let http_unchanged =
            previous.is_some_and(|previous| previous.http == http);
        let compiler = super::host::compile::HostCompiler {
            state,
            http: http.as_deref(),
            protocol: http
                .as_ref()
                .and_then(|http| http.listeners.values().next())
                .map(|listener| RouteProtocol::from(listener.as_ref())),
        };
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
                let sites: Vec<(&Host, &crate::Site)> = table
                    .canonical_entries()
                    .map(|(host, record)| {
                        (host, record.config.as_ref() as &crate::Site)
                    })
                    .collect();
                registered.seal(state.fleet_name(), sites.iter().copied())?;
                registered.clone()
            };
            let config_check = state
                .registry_ref()
                .resolve::<claviron_runtime::ConfigCheck>()
                .is_some();
            table
                .canonical_entries()
                .filter(|(_, record)| {
                    http.as_ref()
                        .and_then(|input| input.listeners.values().next())
                        .is_none_or(|listener| {
                            matches!(
                                listener_plan(&record.config, listener),
                                Some(HostSelection::Application)
                            )
                        })
                })
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
            listener_plans: HashMap::new(),
            configs: HashMap::new(),
            http: http.clone(),
            #[cfg(feature = "tcp")]
            non_serving_interception: {
                if http.as_ref().is_some_and(|input| {
                    input.listeners.values().any(|listener| {
                        RouteProtocol::from(listener.as_ref())
                            == RouteProtocol::Http
                    })
                }) {
                    let prepare = self
                        .non_serving_interception
                        .read()
                        .expect("non-serving interception lock poisoned")
                        .clone();
                    prepare.map(|prepare| prepare(state)).transpose()?.flatten()
                } else {
                    None
                }
            },
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
            if http
                .as_ref()
                .and_then(|input| input.listeners.values().next())
                .is_some_and(|listener| {
                    listener_plan(&record.config, listener).is_none()
                })
            {
                continue;
            }
            if let Some(previous) = previous
                && http_unchanged
                && previous
                    .configs
                    .get(canonical)
                    .is_some_and(|config| Arc::ptr_eq(config, &record.config))
                && let Some(app) = previous.map.get(canonical)
            {
                for host in table.host_keys_for_canonical(canonical) {
                    if let Some(plans) = previous.listener_plans(canonical) {
                        snapshot
                            .listener_plans
                            .insert(host.clone(), plans.clone());
                    }
                    snapshot.insert(host, app.clone())?;
                }
                snapshot
                    .configs
                    .insert(canonical.clone(), record.config.clone());
                continue;
            }
            if let Some(listener) =
                http.as_ref().and_then(|input| input.listeners.values().next())
                && matches!(
                    listener_plan(&record.config, listener),
                    Some(HostSelection::NonServing)
                )
            {
                let plans: ListenerPlans =
                    [(listener.id.clone(), ListenerHostPlan::NonServing)]
                        .into();
                snapshot
                    .configs
                    .insert(canonical.clone(), record.config.clone());
                for host in table.host_keys_for_canonical(canonical) {
                    snapshot.listener_plans.insert(host, plans.clone());
                }
                continue;
            }
            let routable = self.routable_binding(
                state,
                canonical,
                Arc::clone(&record.config),
                None,
            )?;
            #[cfg(all(feature = "orbit", feature = "webtransport"))]
            super::link::webtransport::bind(state, canonical, &record.config)?;
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
            let mut protocol_inputs = HashMap::new();
            for listener in http.iter().flat_map(|http| http.listeners.values())
            {
                let protocol = RouteProtocol::from(listener.as_ref());
                if !matches!(
                    listener_plan(&record.config, listener),
                    Some(HostSelection::Application)
                ) || protocol_inputs.contains_key(&protocol)
                    || !self.specializes_on(canonical, &record.config, protocol)
                {
                    continue;
                }
                let contributions = self.routable_binding(
                    state,
                    canonical,
                    record.config.clone(),
                    Some(protocol),
                )?;
                let selected = if custom_bindings.contains_key(canonical) {
                    if contributions.app.is_some() {
                        return Err(Error::msg(format!(
                            "host '{canonical}' has a manual exclusive binding and a routable app binding",
                        )));
                    }
                    router.clone()
                } else {
                    contributions
                        .app
                        .as_ref()
                        .map(|binding| {
                            binding.build_router(
                                state,
                                canonical,
                                record.config.clone(),
                            )
                        })
                        .transpose()?
                };
                protocol_inputs.insert(
                    protocol,
                    HostInput::new(canonical, selected, contributions)?,
                );
            }
            let (app, listener_plans) = build_host(
                &compiler,
                canonical,
                record.config.clone(),
                router,
                routable,
                &protocol_inputs,
                #[cfg(feature = "rate-limit")]
                snapshot.quotas.get(canonical).and_then(Option::as_deref),
            )?;
            let (app, listener_plans) = self.with_accept_layers(
                canonical,
                &record.config,
                (http.as_deref(), compiler.protocol),
                app,
                listener_plans,
            );

            snapshot.configs.insert(canonical.clone(), record.config.clone());
            for host in table.host_keys_for_canonical(canonical) {
                if let Some(plans) = &listener_plans {
                    snapshot.listener_plans.insert(host.clone(), plans.clone());
                }
                snapshot.insert(host, app.clone())?;
            }
        }

        Ok(snapshot)
    }
}

struct HostInput {
    router: Option<RustRouter>,
    contributions: HostContributions,
    #[cfg(feature = "websocket")]
    websocket: Option<Arc<H1UpgradeRegistry>>,
}

impl HostInput {
    fn new(
        _host: &Host,
        router: Option<RustRouter>,
        contributions: HostContributions,
    ) -> Result<Self> {
        #[cfg(feature = "websocket")]
        let mut contributions = contributions;
        #[cfg(feature = "websocket")]
        let websocket = build_h1_upgrade_registry(
            _host,
            std::mem::take(&mut contributions.h1_upgrades),
        )?;
        Ok(Self {
            router,
            contributions,
            #[cfg(feature = "websocket")]
            websocket,
        })
    }
}

enum HostSelection {
    Application,
    NonServing,
}

fn listener_plan(
    host: &HostConfig,
    listener: &claviron_http::config::ListenerConfig,
) -> Option<HostSelection> {
    use crate::config::NonSslPolicy;
    use claviron_http::config::{ListenerSettings, TcpSecurity};
    if !host.allows_listener(&listener.id)
        || (listener.explicit && host.listeners.is_none())
    {
        return None;
    }
    let compatible = match &listener.settings {
        ListenerSettings::Tcp(tcp) => {
            if matches!(tcp.security, TcpSecurity::Cleartext { .. })
                && host.transport.non_ssl != NonSslPolicy::Serve
            {
                return Some(HostSelection::NonServing);
            }
            tcp.protocols
                .iter()
                .any(|protocol| host.transport.allows_protocol(*protocol))
        }
        ListenerSettings::Quic(_) => host
            .transport
            .allows_protocol(claviron_http::listener::HttpProtocol::H3),
    };
    compatible.then_some(HostSelection::Application)
}

fn build_host(
    compiler: &super::host::compile::HostCompiler<'_>,
    canonical: &Host,
    host_cfg: Arc<HostConfig>,
    router: Option<RustRouter>,
    contributions: HostContributions,
    protocols: &HashMap<RouteProtocol, HostInput>,
    #[cfg(feature = "rate-limit")] quotas: Option<
        &crate::pipeline::layers::HostQuotas,
    >,
) -> Result<(HostExecution, Option<ListenerPlans>)> {
    let common = HostInput::new(canonical, router, contributions)?;
    let shared = if compiler.protocol.is_none() {
        compiler
            .prepare(
                canonical,
                host_cfg.clone(),
                common.router.clone(),
                &common.contributions,
                #[cfg(feature = "rate-limit")]
                quotas,
                #[cfg(feature = "websocket")]
                common.websocket.clone(),
            )
            .map(Some)?
    } else {
        None
    };
    let mut plans = Vec::new();
    for (id, listener) in
        compiler.http.into_iter().flat_map(|http| &http.listeners)
    {
        match listener_plan(&host_cfg, listener) {
            None => continue,
            Some(HostSelection::NonServing) => {
                plans.push((id.clone(), ListenerHostPlan::NonServing));
                continue;
            }
            Some(HostSelection::Application) => {}
        }
        let protocol = RouteProtocol::from(listener.as_ref());
        let input = protocols.get(&protocol).unwrap_or(&common);
        let selected = super::host::compile::HostCompiler {
            state: compiler.state,
            http: compiler.http,
            protocol: Some(protocol),
        };
        let app = selected.prepare_target(
            super::host::compile::HostCompileTarget {
                host: canonical,
                listener: Some(listener),
            },
            host_cfg.clone(),
            input.router.clone(),
            &input.contributions,
            #[cfg(feature = "rate-limit")]
            quotas,
            #[cfg(feature = "websocket")]
            input.websocket.clone(),
        )?;
        plans.push((id.clone(), ListenerHostPlan::Application(app)));
    }
    let shared = shared
        .or_else(|| {
            plans.iter().find_map(|(_, plan)| plan.application().cloned())
        })
        .ok_or_else(|| Error::msg("host has no eligible listener execution"))?;
    Ok((shared, (!plans.is_empty()).then(|| plans.into())))
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
