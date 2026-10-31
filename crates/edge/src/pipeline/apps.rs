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
use crate::pipeline::other::OtherService;
use crate::pipeline::routable::Routable;
use crate::pipeline::host::{HostHandler, compose_other};
use crate::pipeline::routable::{HostBindingProvider, RouteBinding};

/// Rust request execution selected for one host publication.
///
/// Axum keeps its concrete router call. Other accepts a low-level handler or
/// a compiled host plan through OtherService. Both
/// retain shared host admission, response policy and request ownership.
#[derive(Clone)]
pub enum RustRouter {
    #[cfg(feature = "axum")]
    /// An application-owned Axum router.
    Axum(Router),
    /// Another Rust handler or a compiled configured-host plan.
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

use crate::pipeline::ext::{RouteContext, RouteEffect, RouteExtension, RouteLayer};
pub use claviron_http::admission::Admission;

/// Prepared route-extension effects, resolved once during host composition.
#[derive(Default)]
pub(crate) struct ResolvedRouteExtensions {
    pub(crate) handler: Option<Arc<dyn HostHandler>>,
    pub(crate) conditions: Vec<Arc<dyn claviron_http::dispatch::RouteCondition>>,
    pub(crate) layers: Vec<Arc<dyn RouteLayer>>,
}

/// A host's application and what its requests are admitted under.
#[derive(Clone)]
pub struct HostApp {
    pub app: RustRouter,
    pub admission: Admission,
    /// Optional, explicitly configured edge HTTP policy, independent of owner.
    pub exchange_policy: Option<Arc<crate::pipeline::host::ExchangePolicy>>,
    /// Baseline response identity, separate from optional CORS/header policy.
    pub server_header: Option<http::HeaderValue>,
    #[cfg(feature = "rate-limit")]
    pub quotas: Option<Arc<crate::HostQuotas>>,
}

/// An application with no admission limit — a host built outside a
/// snapshot, as tests and embedders do.
impl From<RustRouter> for HostApp {
    fn from(app: RustRouter) -> Self {
        Self {
            app,
            admission: Admission::default(),
            exchange_policy: None,
            server_header: None,
            #[cfg(feature = "rate-limit")]
            quotas: None,
        }
    }
}

/// Immutable snapshot of `host → RustRouter` for a single boot/reload
/// generation.
///
/// # Why immutable?
///
/// The connection-handling hot path resolves an [`RustRouter`] for every
/// incoming request via [`HostAppResolver::resolve`]. To avoid locks on
/// the hot path, the application snapshot is carried by `VhostResolver`'s
/// atomically published table. Changed hosts are prepared before that swap.
/// In-flight requests keep using the old `Arc<HostAppSnapshot>` they
/// loaded; new requests see the new one. No reader ever blocks.
///
/// # Why every host alias gets its own entry
///
/// One canonical host (`example.com`) may serve multiple aliases
/// (`www.example.com`, `m.example.com`, …). The map is keyed by
/// every alias so that resolution is a single `HashMap` lookup
/// — no two-step canonical resolution per request.
///
/// `RustRouter` is `Clone`, and the heavy bits (`Router`, `OtherService`)
/// are `Arc`-backed internally, so duplicating the value across alias
/// keys is cheap.
///
/// # When to use
///
/// You usually don't construct or read this directly — go through
/// [`HostAppResolver`]. Direct use is for tests and the build path in
/// [`HostAppRegistry::build`].
#[derive(Clone, Default)]
pub struct HostAppSnapshot {
    map: HashMap<Host, HostApp>,
    configs: HashMap<Host, Arc<HostConfig>>,
}

impl HostAppSnapshot {
    /// Look up the resolved [`RustRouter`] for an exact host key.
    ///
    /// Returns `None` if no vhost matches. The connection layer treats
    /// `None` as "unknown host" and replies with a default 404 (or
    /// closes the TLS handshake earlier, depending on the path).
    pub fn resolve(
        &self,
        host: &Host,
    ) -> Option<HostApp> {
        let app = self.map.get(host).cloned();
        match app.as_ref() {
            Some(app) => {
                tracing::debug!(
                    "resolved app for host={}, kind={}",
                    host,
                    app.app.kind()
                )
            }
            None => tracing::debug!("no app resolved for host={}", host),
        }
        app
    }

    /// Insert a `host → RustRouter` mapping during snapshot build.
    ///
    /// Returns an error on duplicate keys — the registry must never
    /// stamp two `RustRouter`s onto the same host alias, since dispatch
    /// would be ambiguous. [`HostAppRegistry::build`] is the only
    /// caller; it walks the alias graph once per canonical host so
    /// duplicates only happen on misconfiguration.
    pub fn insert(
        &mut self,
        host: Host,
        app: impl Into<HostApp>,
    ) -> Result<()> {
        let app = app.into();
        if self.map.insert(host.clone(), app).is_some() {
            return Err(Error::msg(format!(
                "duplicate vhost router binding for host '{}'",
                host
            )));
        }
        Ok(())
    }
}

/// Hot-swappable container for the live [`HostAppSnapshot`].
///
/// # Lifecycle
///
/// 1. **Boot:** registers routing preparation with [`VhostResolver`].
///    The ready `HostAppSnapshot` belongs to that table publication.
/// 2. **Reload:** explicit reload rebuilds factories; watched vhost changes
///    rebuild only changed host entries, retaining other admission handles.
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
    ) -> Option<HostApp> {
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

    fn build_app(
        &self,
        state: &AppState,
        canonical: &Host,
        host_cfg: Arc<HostConfig>,
        applications: &[Arc<dyn HostHandler>],
        layers: &[Arc<dyn RouteLayer>],
        #[cfg(feature = "websocket")] h1_upgrades: Vec<H1UpgradeRoute>,
    ) -> Result<RustRouter> {
        let app = match self {
            Self::Other(factory) => RustRouter::Other(factory(
                state.clone(), canonical.clone(), host_cfg.clone(),
            )?),
            #[cfg(feature = "axum")]
            Self::Axum(factory) => RustRouter::Axum(factory(state.clone())),
        };
        #[cfg(feature = "websocket")]
        let websocket = super::host::WebSocketStage::prepare(
            state, canonical, &host_cfg,
            build_h1_upgrade_registry(canonical, h1_upgrades)?,
        );
        let owner = match &app {
            #[cfg(feature = "axum")]
            RustRouter::Axum(router) => super::app::owner(router.clone()),
            RustRouter::Other(owner) => owner.clone(),
        };
        let prepared = compose_other(owner.clone(), applications);
        #[cfg(feature = "websocket")]
        let prepared = super::host::compose_websocket_other(prepared, websocket);
        let layered = super::host::prepare_layers(prepared, layers)
            .map_err(|error| Error::msg(error.to_string()))?;
        let unchanged = match (&owner, &layered) {
            (OtherService::Handler(original), OtherService::Handler(prepared)) => Arc::ptr_eq(original, prepared),
            (OtherService::Noop, OtherService::Noop) => true,
            _ => false,
        };
        Ok(if unchanged { app } else { RustRouter::Other(layered) })
    }
}

#[derive(Default)]
struct RoutableHostBinding {
    app: Option<Binding>,
    applications: Vec<Arc<dyn HostHandler>>,
    layers: Vec<Arc<dyn RouteLayer>>,
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
///    pipeline behind `RustRouter::Other`. Axum is entered only when
///    a Rust contribution explicitly adapts its own router.
///
/// 2. **Code-driven** — the binary calls
///    `register_router` / `register_other` on this registry during
///    bootstrap to attach handwritten Rust handlers to specific
///    hostnames. This is what lets the same binary serve as both an
///    edge server (TLS, h2/h3, statics, ACL — config) and an app
///    server (your business logic — code).
///
/// Both channels coexist: a host registered in code shadows the
/// config-default behaviour for that host, while every other host in
/// `vhosts.yaml` continues to get the default pipeline.
///
/// # Why static `&'static str` keys
///
/// The host name passed to each `register_*` call is `&'static str`
/// because these are explicit, code-pinned bindings — not values that
/// change across reloads. Config (`vhosts.yaml`) can add or remove
/// aliases for a canonical host, but the canonical host name itself
/// is the contract between code and config and isn't expected to
/// move. If you need dynamic host names, source them from env at
/// bootstrap time and `Box::leak` (see usage example below).
///
/// # Reloads
///
/// Registrations survive across reloads — they're owned by this
/// `Component`, which is registered once at boot and read on every
/// `HostAppResolver::reload`. Config can change freely; the code
/// bindings stay pinned. If you remove a `register_*` call and
/// rebuild the binary, the old binding disappears on the next start.
///
/// # Usage in `bootstrap`
///
/// ```ignore
/// use crate::routes;
///
/// let registry = state
///     .registry_ref()
///     .resolve::<HostAppRegistry>()
///     .expect("HostAppRegistry not registered");
///
/// // Standalone API host: your router, your middleware
/// registry.register_router("api.example.com", routes::build_api_router);
///
/// // Resource host: custom Rust handler with explicit file handling
/// registry.register_other("assets.example.com", crate::app::resources::build_other);
///
/// // WebSocket publisher: custom Rust handler; host selected at boot
/// #[cfg(feature = "broadcast")]
/// {
///     if let Ok(host) = std::env::var("PUSHER_HOST") {
///         let host: &'static str = Box::leak(host.into_boxed_str());
///         registry.register_other(host, crate::app::pusher::build_pusher_router);
///     }
/// }
///
/// // Streaming router: standalone axum, no claviron middleware
/// #[cfg(feature = "streams")]
/// registry.register_router("stream.example.com", crate::app::streams::build_stream_router);
/// ```
#[derive(Default)]
pub struct HostAppRegistry {
    inner: RwLock<HashMap<String, Binding>>,
    routables: RwLock<Vec<Arc<dyn Routable>>>,
    route_extensions: RwLock<Vec<Arc<dyn RouteExtension>>>,
    binding_providers: RwLock<Vec<Arc<dyn HostBindingProvider>>>,
    #[cfg(feature = "rate-limit")]
    quotas: RwLock<crate::pipeline::host::QuotaRegistrations>,
}

impl HostAppRegistry {
    /// Bind a prepared limiter to this canonical host/prefix. It runs at the
    /// same head-admission point as config, before Axum, Other or sessions.
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
    /// The factory runs at boot/reload. The caller owns application routing
    /// and layers; host admission, response policy and accounting still apply.
    /// Configured file/backend steps are not installed inside this router.
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
    /// Its handler owns the reply; file handling can be composed explicitly
    /// with the files API. Host admission, response policy and accounting
    /// still apply. Factory errors prevent host publication.
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
    /// `Host -> RustRouter` lookup.
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
    ) -> Result<RoutableHostBinding> {
        let mut applications = Vec::new();
        let mut layers = Vec::new();
        let mut exclusive: Option<(&'static str, Binding)> = None;
        #[cfg(feature = "websocket")]
        let mut h1_upgrades = Vec::new();

        let local_routables = self.routables.read()
            .expect("host app routables lock poisoned").clone();
        for routable in &local_routables {
            if !routable.match_host(canonical, host_cfg.as_ref()) {
                continue;
            }
            if let Some(application) = Arc::clone(routable).application(
                state, canonical, Arc::clone(&host_cfg),
            )? {
                applications.push(application);
            }
            layers.extend(Arc::clone(routable).layers(state, canonical, Arc::clone(&host_cfg))?);
            #[cfg(feature = "websocket")]
            h1_upgrades.extend(routable.h1_upgrades(canonical, Arc::clone(&host_cfg))?);
        }

        let local_bindings = self.binding_providers.read()
            .expect("host binding providers lock poisoned").clone();
        for provider in &local_bindings {
            if !provider.match_host(canonical, host_cfg.as_ref()) {
                continue;
            }
            let Some(binding) = provider.binding(canonical, Arc::clone(&host_cfg))? else {
                continue;
            };
            if let Some((previous, _)) = exclusive.as_ref() {
                return Err(Error::msg(format!(
                    "multiple exclusive host binding providers matched host '{}': '{}' and '{}'",
                    canonical, previous, provider.name(),
                )));
            }
            exclusive = Some((provider.name(), Binding::from_route_binding(binding)));
        }

        Ok(RoutableHostBinding {
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
    ///    - If a code binding exists, invoke its factory and produce
    ///      matching application owner.
    ///    - Otherwise, compile a native configured host pipeline; no Axum
    ///      router is installed unless a Rust contribution explicitly adapts it.
    /// 4. Stamp the resulting `RustRouter` against **every alias** for
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
        // The host defaults every host is admitted under, read once per build.
        let http = state
            .registry_ref()
            .resolve::<crate::HttpConfigProvider>()
            .map(|http| http.snapshot());
        let router = http.as_ref().map(|http| &http.router);
        let default_headers = http.as_ref().map(|http| http.headers.as_ref());
        let default_server = default_headers
            .and_then(|headers| headers.server.as_deref())
            .map(http::HeaderValue::from_str)
            .transpose()
            .map_err(|error| {
                Error::msg(format!("invalid Server header: {error}"))
            })?;
        let mut snapshot = HostAppSnapshot::default();
        #[cfg(feature = "rate-limit")]
        let quotas = {
            let mut quotas =
                self.quotas.write().expect("quota registration lock poisoned");
            quotas.seal(state.fleet_name(), table)?;
            quotas.clone()
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
            #[cfg(feature = "rate-limit")]
            let host_quotas = quotas.prepare(
                state
                    .registry_ref()
                    .resolve::<claviron_runtime::ConfigCheck>()
                    .is_some(),
                canonical,
                &record.config,
                previous
                    .and_then(|snapshot| snapshot.map.get(canonical))
                    .map(|app| &app.quotas),
            )?;
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
            super::embed::lua::append_webtransport_routes(state, canonical, &record.config)?;
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

            #[cfg(feature = "axum")]
            let standalone = matches!(&binding, Some(Binding::Axum(_)));
            #[cfg(not(feature = "axum"))]
            let standalone = false;
            let exchange_policy = crate::pipeline::host::ExchangePolicy::compile(
                &record.config,
                default_headers,
            );
            let server_header = default_server.clone().or_else(|| {
                (!standalone).then(|| {
                    http::HeaderValue::from_static(
                        claviron_http::config::host::DEFAULT_SERVER_HEADER,
                    )
                })
            });
            let app = if let Some(binding) = binding {
                binding.build_app(
                    state,
                    canonical,
                    Arc::clone(&record.config),
                    &routable.applications,
                    &routable.layers,
                    #[cfg(feature = "websocket")]
                    routable.h1_upgrades,
                )?
            } else {
                build_default_app(
                    state,
                    canonical,
                    Arc::clone(&record.config),
                    &routable.applications,
                    &routable.layers,
                    #[cfg(feature = "websocket")]
                    routable.h1_upgrades,
                )?
            };

            // One admission per canonical host, shared by its aliases: they
            // are one host with several names.
            let max_concurrent_requests = match router.as_ref() {
                Some(router) => router.max_concurrent_requests_for(
                    record.config.max_concurrent_requests,
                ),
                None => record
                    .config
                    .max_concurrent_requests
                    .and_then(claviron_http::policy::Limit::value),
            };
            let app = HostApp {
                app,
                admission: Admission::limited(max_concurrent_requests),
                exchange_policy,
                server_header,
                #[cfg(feature = "rate-limit")]
                quotas: host_quotas,
            };
            snapshot.configs.insert(canonical.clone(), record.config.clone());
            for host in table.host_keys_for_canonical(canonical) {
                snapshot.insert(host, app.clone())?;
            }
        }

        Ok(snapshot)
    }
}

fn build_default_app(
    state: &AppState,
    canonical: &Host,
    host_cfg: Arc<HostConfig>,
    applications: &[Arc<dyn HostHandler>],
    layers: &[Arc<dyn RouteLayer>],
    #[cfg(feature = "websocket")] h1_upgrades: Vec<H1UpgradeRoute>,
) -> Result<RustRouter> {
    #[cfg(feature = "websocket")]
    let websocket = build_h1_upgrade_registry(canonical, h1_upgrades)?;
    super::host::compile::prepare(
        state, canonical, host_cfg, applications, layers,
        #[cfg(feature = "websocket")]
        websocket,
    ).map(RustRouter::Other)
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
