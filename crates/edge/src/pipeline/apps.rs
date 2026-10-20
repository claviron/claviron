use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[cfg(feature = "broadcast")]
mod broadcast;
#[cfg(all(
    feature = "lua",
    any(feature = "websocket", feature = "webtransport")
))]
mod lua_socket;
#[cfg(all(feature = "lua", feature = "webtransport"))]
mod lua_webtransport;

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
#[cfg(feature = "upstream")]
use claviron_upstream::UpstreamProvider;
use tracing::error;

use crate::VhostResolver;
use crate::config::HostConfig;
use crate::pipeline::bypass::BypassService;
#[cfg(feature = "upstream")]
use crate::pipeline::bypass::upstream::UpstreamBypass;
#[cfg(feature = "axum")]
use crate::pipeline::merged::{create_default_router, create_router};
#[cfg(feature = "axum")]
use crate::pipeline::routable::Routable;
use crate::pipeline::routable::{HostBindingProvider, RouteBinding};

/// Resolved binding for a single vhost — what runs when a request lands
/// on it.
///
/// # Scope: per-host, not system-wide
///
/// `AppKind` is keyed by [`HostKey`] (`Arc<Host>`) in
/// [`HostAppSnapshot`]: each vhost gets its own resolved variant
/// independently. The choice is local — `example.com` can run on
/// `Axum`, `assets.example.com` on `Bypass`, `ws.example.com` on a
/// different `Bypass` variant, all in the same process. A request only
/// ever touches the `AppKind` of the host it resolves to, so the two
/// variants are alternatives **per request**, not system-wide.
///
/// This is why two variants are enough: the system carries an
/// arbitrary number of `AppKind`s simultaneously (one per host),
/// and the binary fork only needs to answer "for this particular
/// host, axum or bypass?"
///
/// # Variants
///
/// * **`Axum`** — full middleware pipeline built by `create_router`:
///   `axum_file_handler` (statics / try_files / SPA),
///   PHP layers (FCGI / SAPI dispatch by path-prefix), CORS,
///   alt-svc, served-by, tracing, optional metrics.
///   This is the path most hosts take — config-driven behaviour with
///   the standard layer stack.
///
/// * **`Bypass`** — direct dispatch on a [`BypassService`] with no axum
///   router and no middleware stack. Used when the host has a single
///   dominant protocol that doesn't need the standard pipeline: PHP
///   FCGI proxy, in-process PHP SAPI, Pusher WebSocket upgrade, or a
///   user-registered asset handler wrapped via `ResourceAdapter`
///   (file-first probe + custom fallback, e.g. DB-backed images,
///   cache-backed blobs, or free app-generated resources).
///
/// # Why only two variants?
///
/// Within a single host, `AppKind` answers a binary question:
/// "should this request go through the axum pipeline or skip it?"
/// New behaviour fits one of these:
///
/// - **Custom routing inside the axum pipeline** (regex paths,
///   iterator-based dispatch, alternative routers): wrap as a
///   `tower::Service` / `axum::Router` and attach via [`register_app`]
///   (merged with claviron middleware) or [`register_router`] (your own
///   router, no claviron middleware). Lives under `AppKind::Axum`.
///
/// - **Skip the pipeline entirely** (custom protocol, hot-path handler
///   that doesn't want middleware): use [`register_bypass`] which builds
///   a [`BypassService`] variant. Lives under `AppKind::Bypass`.
///
/// Adding a third top-level variant (e.g. "raw router with no
/// middleware but still via axum") would duplicate the bypass path
/// without buying anything — the test would be "is axum involved?",
/// which is exactly the existing fork.
///
/// # The escape hatch
///
/// Together, `Axum` and `Bypass` cover the full spectrum, applied
/// independently per host:
///
/// - **All-in:** config-driven host with the standard pipeline (no
///   `register_*` call needed — comes from `config/*.yaml`).
/// - **Augmented:** axum pipeline + your routes merged via
///   [`register_app`] (gets all claviron middleware).
/// - **Standalone axum:** you own the entire `Router`, no claviron
///   middleware ([`register_router`]).
/// - **Out of axum:** custom [`BypassService`] variant
///   ([`register_bypass`]) — total control, no framework conventions.
///
/// This is the project's "open door": claviron is both an **edge
/// server** (TLS, h2/h3, DNS, ACL, rate limiting, static assets) and
/// an **app server** (your Rust code, the same process). You can
/// adopt it incrementally — start with config-only hosts, add Rust
/// handlers inside the pipeline, escape to bypass when you need the
/// floor. And you can mix: the marketing site on `Axum`, the asset host
/// on `Bypass`, the WebSocket pusher on `Bypass` — same process, same
/// TLS termination, same DNS, different dispatch.
///
/// All variants are dispatched during http stream handling
/// (see `crate::service::HostService`).
///
/// [`register_app`]: HostAppRegistry::register_app
/// [`register_router`]: HostAppRegistry::register_router
/// [`register_bypass`]: HostAppRegistry::register_bypass
#[derive(Clone)]
pub enum AppKind {
    #[cfg(feature = "axum")]
    Axum(Router),
    Bypass(BypassService),
}

impl AppKind {
    pub fn kind(&self) -> &'static str {
        match self {
            #[cfg(feature = "axum")]
            Self::Axum(_) => "axum",
            Self::Bypass(_) => "bypass",
        }
    }
}

#[cfg(feature = "axum")]
use crate::pipeline::merged::routing::RouteContext;
pub use claviron_http::admission::Admission;

/// Prepared route-extension effects, resolved once during host composition.
#[cfg(feature = "axum")]
#[derive(Default)]
pub(crate) struct ResolvedRouteExtensions {
    pub(crate) handler: Option<Router<AppState>>,
    pub(crate) conditions: Vec<Arc<dyn claviron_http::routing::RouteCondition>>,
    pub(crate) layers: Vec<Arc<dyn claviron_http::routing::RouteLayer>>,
}

/// A host's application and what its requests are admitted under.
#[derive(Clone)]
pub struct HostApp {
    pub app: AppKind,
    pub admission: Admission,
    /// Optional, explicitly configured edge HTTP policy, independent of owner.
    pub exchange_policy: Option<Arc<crate::service::ExchangePolicy>>,
    /// Baseline response identity, separate from optional CORS/header policy.
    pub server_header: Option<http::HeaderValue>,
}

/// An application with no admission limit — a host built outside a
/// snapshot, as tests and embedders do.
impl From<AppKind> for HostApp {
    fn from(app: AppKind) -> Self {
        Self {
            app,
            admission: Admission::default(),
            exchange_policy: None,
            server_header: None,
        }
    }
}

/// Immutable snapshot of `host → AppKind` for a single boot/reload
/// generation.
///
/// # Why immutable?
///
/// The connection-handling hot path resolves an [`AppKind`] for every
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
/// `AppKind` is `Clone`, and the heavy bits (`Router`, `BypassService`)
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
    /// Look up the resolved [`AppKind`] for an exact host key.
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

    /// Insert a `host → AppKind` mapping during snapshot build.
    ///
    /// Returns an error on duplicate keys — the registry must never
    /// stamp two `AppKind`s onto the same host alias, since dispatch
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

/// Factory for [`register_app`]: produces a router that **will be
/// merged into** claviron's standard pipeline by `create_router(...)`.
///
/// The returned `Router<AppState>` carries `AppState` so claviron's
/// outer router (which provides the state) can absorb it via
/// `.merge(...)`. After merging, your routes see the same middleware
/// stack every config-driven host gets (file_handler, PHP, CORS,
/// alt-svc, …). Host ACL and early replies precede every app shape.
///
/// [`register_app`]: HostAppRegistry::register_app
#[cfg(feature = "axum")]
pub type MergedRouterFactory =
    dyn Fn(AppState) -> Router<AppState> + Send + Sync + 'static;

/// Factory for [`register_router`]: produces a fully-formed
/// `Router` that is used **as-is**, with no claviron middleware.
///
/// `Router<()>` (state already applied internally) is what `axum`
/// expects from a finished router. You take full responsibility for
/// any application middleware you want — file serving, CORS, etc.
/// HostService still applies ACL and early host replies before entering this router.
///
/// [`register_router`]: HostAppRegistry::register_router
#[cfg(feature = "axum")]
pub type StandaloneRouterFactory =
    dyn Fn(AppState) -> Router<()> + Send + Sync + 'static;

/// Factory for [`register_bypass`]: produces a [`BypassService`] that
/// dispatches **outside the axum pipeline** entirely.
///
/// Unlike the router factories, this one receives the canonical
/// [`Host`] and the resolved [`HostConfig`] — bypass services often
/// need to read host-level config (TLS aliases, prefix paths, db name,
/// pusher socket, …) at build time. `Result` is returned because
/// resolution can legitimately fail (missing dependency in the
/// registry), and the registry surfaces that on boot rather than
/// blowing up at request time.
///
/// [`register_bypass`]: HostAppRegistry::register_bypass
pub type BypassFactory = dyn Fn(AppState, Host, Arc<HostConfig>) -> Result<BypassService>
    + Send
    + Sync
    + 'static;

/// Internal pairing of "what kind of factory was registered" with
/// "the factory itself".
///
/// Three variants because the `register_*` methods accept three
/// different function shapes. [`HostAppRegistry::build`] dispatches on
/// this enum to know how to invoke each factory and which [`AppKind`]
/// to wrap the result in. Not exposed publicly because callers don't
/// need to construct this directly — they go through the typed
/// `register_*` methods.
#[derive(Clone)]
enum Binding {
    #[cfg(feature = "axum")]
    Merged(Vec<Arc<MergedRouterFactory>>),
    #[cfg(feature = "axum")]
    Standalone(Arc<StandaloneRouterFactory>),
    Bypass(Arc<BypassFactory>),
}

impl Binding {
    fn from_route_binding(binding: RouteBinding) -> Self {
        match binding {
            #[cfg(feature = "axum")]
            RouteBinding::Standalone(factory) => Self::Standalone(factory),
            RouteBinding::Bypass(factory) => Self::Bypass(factory),
        }
    }

    fn build_app(
        &self,
        state: &AppState,
        canonical: &Host,
        host_cfg: Arc<HostConfig>,
        #[cfg(feature = "websocket")] h1_upgrades: Vec<H1UpgradeRoute>,
    ) -> Result<AppKind> {
        match self {
            #[cfg(feature = "axum")]
            Self::Merged(factories) => {
                let routers = factories
                    .iter()
                    .map(|factory| factory(state.clone()))
                    .collect();
                #[cfg(feature = "websocket")]
                let h1_upgrade_registry =
                    build_h1_upgrade_registry(canonical, h1_upgrades)?;
                let router = create_router(
                    state.clone(),
                    canonical,
                    Arc::clone(&host_cfg),
                    routers,
                    #[cfg(feature = "websocket")]
                    h1_upgrade_registry,
                )?;
                Ok(AppKind::Axum(router))
            }
            Self::Bypass(factory) => {
                #[cfg(feature = "websocket")]
                if !h1_upgrades.is_empty() {
                    return Err(Error::msg(format!(
                        "host '{}' has H1 upgrade bindings but resolves to bypass",
                        canonical
                    )));
                }
                let svc = factory(
                    state.clone(),
                    canonical.clone(),
                    Arc::clone(&host_cfg),
                )?;
                Ok(AppKind::Bypass(svc))
            }
            #[cfg(feature = "axum")]
            Self::Standalone(factory) => {
                #[cfg(feature = "websocket")]
                if !h1_upgrades.is_empty() {
                    return Err(Error::msg(format!(
                        "host '{}' has H1 upgrade bindings but resolves to standalone router",
                        canonical
                    )));
                }
                let router = factory(state.clone());
                Ok(AppKind::Axum(router))
            }
        }
    }
}

#[derive(Default)]
struct RoutableHostBinding {
    app: Option<Binding>,
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
///    upstreams, statics, ACL, etc. These hosts go straight to
///    `AppKind::Axum` with the standard middleware pipeline. No Rust
///    code involved per host.
///
/// 2. **Code-driven** — the binary calls `register_app` /
///    `register_router` / `register_bypass` on this registry during
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
/// // Main app: merged with claviron middleware (statics, CORS, ACL…)
/// registry.register_app("example.com", routes::build_app_router);
///
/// // Standalone API host: your router, your middleware
/// registry.register_router("api.example.com", routes::build_api_router);
///
/// // Resource host: bypass (file-first probe + custom handler, no axum)
/// registry.register_bypass("assets.example.com", crate::app::resources::build_bypass);
///
/// // WebSocket pusher: bypass; host name from env so ops can override
/// #[cfg(feature = "broadcast")]
/// {
///     if let Ok(host) = std::env::var("PUSHER_HOST") {
///         let host: &'static str = Box::leak(host.into_boxed_str());
///         registry.register_bypass(host, crate::app::pusher::build_pusher_bypass);
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
    #[cfg(feature = "axum")]
    routables: RwLock<Vec<Arc<dyn Routable>>>,
    #[cfg(feature = "axum")]
    route_extensions: RwLock<
        Vec<Arc<dyn claviron_http::routing::RouteExtension<HostConfig>>>,
    >,
    binding_providers: RwLock<Vec<Arc<dyn HostBindingProvider>>>,
}

impl HostAppRegistry {
    /// Register a host-independent directive compiler before snapshot build.
    #[cfg(feature = "axum")]
    pub fn register_route_extension(
        &self,
        extension: Arc<dyn claviron_http::routing::RouteExtension<HostConfig>>,
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

    /// Register a router that **gets merged into claviron's standard
    /// pipeline**.
    ///
    /// # When to use
    ///
    /// You want your Rust routes to coexist with claviron's standard
    /// middleware: static asset handler (`statics:` / `try_files:` /
    /// SPA), PHP layers (FCGI / SAPI), CORS,
    /// alt-svc, served-by header, tracing,
    /// optional metrics. Your routes are merged in via
    /// `Router::merge` — claviron owns the layer stack, you own the
    /// route surface. Host policy applies before this stack, as for bypass.
    ///
    /// Concretely: a marketing site that has a few `/api/*` Rust
    /// endpoints alongside static HTML/CSS/JS and a PHP backend
    /// fits this pattern.
    ///
    /// # When not to use
    ///
    /// - You want to own every middleware decision (no claviron
    ///   stack) → use [`Self::register_router`].
    /// - You don't want axum at all (custom protocol, hot-path
    ///   service) → use [`Self::register_bypass`].
    /// - The host has no Rust code at all (purely config-driven) →
    ///   don't register; the default pipeline is built automatically.
    ///
    /// # Lifecycle
    ///
    /// `vhost_key` is `&'static str` because the binding is pinned
    /// in code; the canonical host name doesn't change across reloads
    /// (aliases for it can change in config, but the canonical name
    /// is the contract). If the registered key has no canonical
    /// config in `vhosts.yaml` at build time, the registration is
    /// **skipped with an error log** and the host stays unregistered
    /// — see [`build`](Self::build) for the exact branch.
    ///
    /// # Where it lands
    ///
    /// Wraps to [`AppKind::Axum`] via `create_router(...)` — your
    /// routes inside, claviron middleware around them.
    #[cfg(feature = "axum")]
    pub fn register_app<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState) -> Router<AppState> + Send + Sync + 'static,
    {
        let router: Arc<MergedRouterFactory> =
            Arc::new(move |state: AppState| factory(state));

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.to_string(), Binding::Merged(vec![router]));
    }

    /// Register a fully-formed router that **bypasses claviron
    /// middleware but still uses axum**.
    ///
    /// # When to use
    ///
    /// You want the routing ergonomics of axum (path params,
    /// extractors, `Router::nest`, …) but you do **not** want
    /// claviron's static handler, PHP layers, CORS — because
    /// you'll provide them yourself or you don't need them. Typical
    /// uses: a pure JSON API host where statics are irrelevant; a
    /// streaming endpoint where you control buffering; a webhook
    /// receiver with bespoke auth. This does not bypass host ACL admission.
    ///
    /// # When not to use
    ///
    /// - You want claviron middleware around your routes →
    ///   [`Self::register_app`].
    /// - You want to skip axum entirely (e.g. raw `tower::Service`,
    ///   custom protocol, no router overhead) → [`Self::register_bypass`].
    /// - The host is config-only → don't register.
    ///
    /// # Lifecycle
    ///
    /// Same as `register_app`: `&'static str` host key, no rebuild
    /// across reloads. Skipped with an error log if the key has no
    /// canonical entry in `vhosts.yaml`.
    ///
    /// # Where it lands
    ///
    /// Wraps to [`AppKind::Axum`] **without** the `create_router`
    /// wrapper — the factory's output goes through the connection
    /// layer as-is. You get axum routing, you skip claviron layers.
    #[cfg(feature = "axum")]
    pub fn register_router<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState) -> Router + Send + Sync + 'static,
    {
        let router: Arc<StandaloneRouterFactory> =
            Arc::new(move |state: AppState| factory(state));

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.to_string(), Binding::Standalone(router));
    }

    /// Register a [`BypassService`] that **skips axum and the
    /// middleware stack entirely**.
    ///
    /// # When to use
    ///
    /// The host has a single dominant protocol that doesn't need
    /// router-style dispatch:
    ///
    /// - **PHP FCGI / SAPI proxy** — every request goes to PHP, no
    ///   route matching at the Rust layer.
    /// - **WebSocket pusher** — single endpoint, persistent upgrade,
    ///   different hot path than HTTP requests.
    /// - **Asset/resource host** — file-first probe (handled by
    ///   `ResourceAdapter`) followed by a custom handler (e.g.
    ///   DB-backed image lookup, cache-backed blobs, free app-generated
    ///   resources); no axum router
    ///   needed because the dispatch is "static disk OR my fn", not
    ///   "match a route table".
    /// - **Custom edge protocol** — anything that wants the bare
    ///   `tower::Service<Request<RouteBody>>` shape.
    ///
    /// # When not to use
    ///
    /// - You want path-based routing → [`Self::register_router`] gives you
    ///   axum without the claviron layers.
    /// - You want claviron's standard pipeline → [`Self::register_app`].
    ///
    /// # Why the factory takes `AppState`, `Host`, `Arc<HostConfig>`
    ///
    /// Bypass services frequently need to read host-level config:
    /// the canonical host (TLS / SNI), the resolved `HostConfig` (db
    /// name, prefix paths, pusher socket, …), and the runtime
    /// registry (to resolve dependencies like `DbService`). Passing
    /// these to the factory at build time avoids re-resolving on
    /// every request.
    ///
    /// `Result<BypassService>` is returned so missing dependencies
    /// surface at boot, not on first request.
    ///
    /// # Lifecycle
    ///
    /// Same as the other two: `&'static str` host key. Skipped with
    /// an error log if the key has no canonical entry in
    /// `vhosts.yaml`.
    ///
    /// # Where it lands
    ///
    /// Wraps to [`AppKind::Bypass`]. The connection layer dispatches
    /// directly to `BypassService::handle` — no axum, no Tower
    /// middleware around it.
    pub fn register_bypass<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState, Host, Arc<HostConfig>) -> Result<BypassService>
            + Send
            + Sync
            + 'static,
    {
        let f: Arc<BypassFactory> = Arc::new(
            move |state: AppState, canonical: Host, cfg: Arc<HostConfig>| {
                factory(state, canonical, cfg)
            },
        );

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.to_string(), Binding::Bypass(f));
    }

    #[cfg(feature = "upstream")]
    pub fn register_upstream_bypass_for_host(
        &self,
        vhost_key: impl Into<String>,
    ) {
        use crate::pipeline::bypass::factories::upstream::bypass_fn;
        let f: Arc<BypassFactory> = Arc::new(
            move |state: AppState, canonical: Host, cfg: Arc<HostConfig>| {
                bypass_fn(state, canonical, cfg)
            },
        );

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.into(), Binding::Bypass(f));
    }

    #[cfg(any(feature = "php_fcgi", feature = "php_embed"))]
    pub fn register_php_bypass_for_host(
        &self,
        vhost_key: impl Into<String>,
    ) {
        use crate::pipeline::bypass::factories::php::bypass_fn;

        let f: Arc<BypassFactory> = Arc::new(
            move |state: AppState, canonical: Host, cfg: Arc<HostConfig>| {
                bypass_fn(state, canonical, cfg)
            },
        );

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.into(), Binding::Bypass(f));
    }

    /// Register a provider-shaped route contributor.
    ///
    /// This is a build-time convenience layer over the three explicit
    /// registration doors. `Routable` providers are evaluated during
    /// [`build`](Self::build), and the resulting binding is frozen into the
    /// `HostAppSnapshot`. The request hot path remains a plain
    /// `Host -> AppKind` lookup.
    #[cfg(feature = "axum")]
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

    #[cfg(feature = "axum")]
    pub(crate) fn layer_routable_routes(
        &self,
        state: &AppState,
        key: &Host,
        host: Arc<HostConfig>,
        downstream: claviron_http::routing::AxumPipeline,
        mut router: Router,
    ) -> Router {
        let routables = self
            .routables
            .read()
            .expect("host app routables lock poisoned")
            .clone();
        for routable in routables {
            if routable.match_host(key, host.as_ref()) {
                router = routable.layer_routes(
                    super::routable::RouteLayerContext {
                        state,
                        host: key,
                        config: host.clone(),
                        downstream: downstream.clone(),
                    },
                    router,
                );
            }
        }
        router
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

    #[cfg(feature = "axum")]
    pub(crate) fn resolve_host_layers(
        &self,
        context: &RouteContext<'_>,
    ) -> Result<Vec<Arc<dyn claviron_http::routing::RouteLayer>>> {
        self.resolve_extensions(context, true).map(|resolved| resolved.layers)
    }

    #[cfg(feature = "axum")]
    pub(crate) fn resolve_extensions(
        &self,
        context: &RouteContext<'_>,
        inherited_only: bool,
    ) -> Result<ResolvedRouteExtensions> {
        use claviron_http::routing::RouteEffect;
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
                RouteEffect::Handler(router) => {
                    if config.owns_handler || handler.is_some() {
                        return Err(Error::msg(format!(
                            "route '{}' has multiple application owners",
                            context.prefix
                        )));
                    }
                    handler = Some(router);
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
        #[cfg(not(feature = "axum"))]
        let _ = state;
        #[cfg(feature = "axum")]
        let mut merged: Vec<(&'static str, Arc<MergedRouterFactory>)> =
            Vec::new();
        let mut exclusive: Option<(&'static str, Binding)> = None;
        #[cfg(feature = "websocket")]
        let mut h1_upgrades = Vec::new();

        #[cfg(feature = "axum")]
        {
            let local_routables = self
                .routables
                .read()
                .expect("host app routables lock poisoned")
                .clone();
            for routable in &local_routables {
                if !routable.match_host(canonical, host_cfg.as_ref()) {
                    continue;
                }

                let name = routable.name();
                #[cfg(feature = "websocket")]
                {
                    let mut upgrades = routable
                        .h1_upgrades(canonical, Arc::clone(&host_cfg))?;
                    if !upgrades.is_empty() {
                        tracing::debug!(
                            host = %canonical,
                            routable = name,
                            count = upgrades.len(),
                            "mounting H1 upgrade routable provider"
                        );
                    }
                    h1_upgrades.append(&mut upgrades);
                }

                if let Some(router) = Arc::clone(routable).routes(
                    state,
                    canonical,
                    Arc::clone(&host_cfg),
                )? {
                    let factory: Arc<MergedRouterFactory> =
                        Arc::new(move |_state: AppState| router.clone());
                    merged.push((name, factory));
                }
            }
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

            let name = provider.name();
            let Some(binding) =
                provider.binding(canonical, Arc::clone(&host_cfg))?
            else {
                continue;
            };
            if let Some((previous, _)) = exclusive.as_ref() {
                return Err(Error::msg(format!(
                    "multiple exclusive host binding providers matched host '{}': '{}' and '{}'",
                    canonical, previous, name
                )));
            }
            #[cfg(feature = "axum")]
            if !merged.is_empty() {
                let merged_names = merged
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>()
                    .join(",");
                return Err(Error::msg(format!(
                    "host binding provider '{}' is exclusive for host '{}' but Rust routables already matched: {}",
                    name, canonical, merged_names
                )));
            }
            exclusive = Some((name, Binding::from_route_binding(binding)));
        }

        let app = if let Some((name, binding)) = exclusive {
            tracing::debug!(
                host = %canonical,
                provider = name,
                "mounting exclusive host binding provider"
            );
            Some(binding)
        } else {
            #[cfg(feature = "axum")]
            if !merged.is_empty() {
                let names = merged
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>()
                    .join(",");
                let factories = merged
                    .into_iter()
                    .map(|(_, factory)| factory)
                    .collect::<Vec<_>>();
                tracing::debug!(
                    host = %canonical,
                    routables = %names,
                    "mounting merged routable providers"
                );
                return Ok(RoutableHostBinding {
                    app: Some(Binding::Merged(factories)),
                    #[cfg(feature = "websocket")]
                    h1_upgrades,
                });
            }
            None
        };

        Ok(RoutableHostBinding {
            app,
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
    ///      the matching [`AppKind`] (`Merged` / `Standalone` /
    ///      `Bypass`).
    ///    - Otherwise, build a default `AppKind::Axum` via
    ///      `create_default_router` (claviron middleware + config-
    ///      driven routes only).
    /// 4. Stamp the resulting `AppKind` against **every alias** for
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
            append_lua_websocket_routes(
                state,
                canonical,
                &record.config,
                &mut routable.h1_upgrades,
            )?;
            #[cfg(all(feature = "lua", feature = "webtransport"))]
            lua_webtransport::append_routes(state, canonical, &record.config)?;
            #[cfg(feature = "broadcast")]
            broadcast::append_routes(
                state,
                canonical,
                &record.config,
                &mut routable.h1_upgrades,
            )?;
            let binding = match custom_bindings.get(canonical).cloned() {
                #[cfg(feature = "axum")]
                Some(Binding::Merged(mut factories)) => {
                    if let Some(routable_app) = routable.app.clone() {
                        match routable_app {
                            Binding::Merged(mut routable_factories) => {
                                factories.append(&mut routable_factories);
                                Some(Binding::Merged(factories))
                            }
                            Binding::Standalone(_) | Binding::Bypass(_) => {
                                return Err(Error::msg(format!(
                                    "host '{}' has manual merged routes and an exclusive routable binding",
                                    canonical
                                )));
                            }
                        }
                    } else {
                        Some(Binding::Merged(factories))
                    }
                }
                // As above: whatever the merged arm did not take is exclusive.
                Some(exclusive) => {
                    if routable.app.is_some() {
                        return Err(Error::msg(format!(
                            "host '{}' has a manual exclusive binding and a routable app binding",
                            canonical
                        )));
                    }
                    Some(exclusive)
                }
                None => routable.app.clone(),
            };
            let binding = match binding {
                #[cfg(feature = "axum")]
                Some(Binding::Merged(factories)) if factories.is_empty() => {
                    None
                }
                other => other,
            };

            #[cfg(feature = "axum")]
            let standalone = matches!(&binding, Some(Binding::Standalone(_)));
            #[cfg(not(feature = "axum"))]
            let standalone = false;
            let exchange_policy = crate::service::ExchangePolicy::compile(
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
                    #[cfg(feature = "websocket")]
                    routable.h1_upgrades,
                )?
            } else {
                build_default_app(
                    state,
                    canonical,
                    Arc::clone(&record.config),
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
    #[cfg(feature = "websocket")] h1_upgrades: Vec<H1UpgradeRoute>,
) -> Result<AppKind> {
    #[cfg(feature = "upstream")]
    if let Some(svc) =
        build_config_upstream_bypass(state, canonical, Arc::clone(&host_cfg))
    {
        #[cfg(feature = "websocket")]
        if !h1_upgrades.is_empty() {
            return Err(Error::msg(format!(
                "host '{}' has H1 upgrade bindings but resolves to bypass",
                canonical
            )));
        }
        return Ok(AppKind::Bypass(svc));
    }

    #[cfg(feature = "axum")]
    {
        #[cfg(feature = "websocket")]
        let h1_upgrade_registry =
            build_h1_upgrade_registry(canonical, h1_upgrades)?;

        Ok(AppKind::Axum(create_default_router(
            state.clone(),
            canonical,
            host_cfg,
            #[cfg(feature = "websocket")]
            h1_upgrade_registry,
        )?))
    }
    // The default host pipeline *is* the router — statics, try_files, the PHP
    // layers. CORS/headers are shared HTTP policy, not part of that router.
    // A build without `axum` has no application router to fall back to,
    // so a host has to say what serves it: `register_php_bypass_for_host`,
    // `register_upstream_bypass_for_host`, `register_bypass`, or an upstream in
    // its own config. Refusing at build is the point; the alternative is a host
    // that boots and then 404s everything.
    #[cfg(not(feature = "axum"))]
    {
        let _ = (state, host_cfg);
        #[cfg(feature = "websocket")]
        let _ = h1_upgrades;

        Err(Error::msg(format!(
            "host '{canonical}' has no bypass binding, and this build has no router to fall back \
             on (feature `axum` is off)"
        )))
    }
}

#[cfg(all(feature = "lua", feature = "websocket"))]
fn append_lua_websocket_routes(
    state: &AppState,
    canonical: &Host,
    host: &HostConfig,
    routes: &mut Vec<H1UpgradeRoute>,
) -> Result<()> {
    let Some(service) =
        state.registry_ref().resolve::<claviron_lua::serve::LuaApplications>()
    else {
        return Ok(());
    };
    let h2_connect = state.registry_ref().resolve::<crate::H2ConnectRegistry>();

    for application in host.lua() {
        for websocket in application.websockets() {
            let path = mounted_websocket_path(
                application.path.as_ref(),
                &websocket.path,
            );
            let handler = Arc::new(lua_socket::WebSocketUpgrade {
                inner: claviron_lua::websocket::LuaWebSocketUpgradeHandler::new(
                    service.clone(),
                    application.application.as_str(),
                ),
            });
            routes.push(H1UpgradeRoute::handler(path.clone(), handler)?);
            if let Some(registry) = h2_connect.as_ref() {
                registry.bind_handler(
                    canonical.clone(),
                    path,
                    Arc::new(LuaWebSocketConnectHandler {
                        service: service.clone(),
                        application: Arc::from(
                            application.application.as_str(),
                        ),
                    }),
                )?;
            }
        }
    }
    Ok(())
}

/// A Lua application's WebSocket over HTTP/2 extended CONNECT (RFC 8441).
#[cfg(all(feature = "lua", feature = "websocket"))]
struct LuaWebSocketConnectHandler {
    service: Arc<claviron_lua::serve::LuaApplications>,
    application: Arc<str>,
}

#[cfg(all(feature = "lua", feature = "websocket"))]
#[async_trait::async_trait]
impl crate::H2ConnectHandler for LuaWebSocketConnectHandler {
    fn name(&self) -> &'static str {
        "lua_websocket_h2"
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &crate::AuthContext,
    ) -> crate::H2ConnectResult {
        if !lua_socket::same_origin(&ctx.headers, &ctx.uri, &ctx.conn.scheme())
        {
            return Err(crate::H2ConnectReject::forbidden());
        }
        // RFC 8441 §5: 13 is the only version defined for this handshake.
        let version = ctx
            .headers
            .get("sec-websocket-version")
            .and_then(|value| value.to_str().ok());
        if version != Some("13") {
            return Err(crate::H2ConnectReject::bad_request());
        }
        let application =
            claviron_lua::websocket::prepare(&self.service, &self.application)
                .map_err(|_| {
                    crate::H2ConnectReject::new(
                        http::StatusCode::SERVICE_UNAVAILABLE,
                    )
                })?;
        Ok(crate::H2ConnectAccept::authenticated(application))
    }

    async fn handle(
        &self,
        _state: AppState,
        ctx: crate::AuthContext,
        authenticated: Option<crate::AnyAuthenticated>,
        req: http::Request<claviron_http::RouteBody>,
    ) -> Result<claviron_http::RouteResponse> {
        let is_websocket =
            req.extensions().get::<hyper::ext::Protocol>().is_some_and(
                |protocol| protocol.as_str().eq_ignore_ascii_case("websocket"),
            );
        if !is_websocket {
            return Ok(claviron_http::err_status(
                http::StatusCode::BAD_REQUEST,
            ));
        }
        let application = authenticated
            .ok_or_else(|| Error::msg("Lua WebSocket application missing"))?
            .take::<Arc<claviron_lua::app::LuaApplication>>()?;
        let sessions = req
            .extensions()
            .get::<claviron_http::session::SessionSpawner>()
            .cloned()
            .ok_or_else(|| {
                Error::msg("H2 CONNECT request carries no session spawner")
            })?;
        let authority = req
            .uri()
            .authority()
            .map(|authority| authority.to_string())
            .unwrap_or_else(|| ctx.host.to_string());
        let url = format!(
            "wss://{authority}{}",
            req.uri().path_and_query().map_or("/", |path| path.as_str())
        );
        let headers = claviron_lua::websocket::header_pairs(&ctx.headers);
        let submitted = sessions.spawn(async move {
            match hyper::upgrade::on(req).await {
                Ok(upgraded) => {
                    claviron_lua::websocket::serve_websocket(
                        upgraded,
                        url,
                        headers,
                        application,
                        "HTTP/2.0",
                    )
                    .await;
                }
                Err(error) => error!(%error, "H2 CONNECT upgrade failed"),
            }
        });
        if let Err(error) = submitted {
            tracing::debug!(%error, "H2 CONNECT session hand-off rejected");
            return Ok(claviron_http::err_status(
                http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(claviron_http::empty_body())
            .expect("static response"))
    }
}

#[cfg(all(
    feature = "lua",
    any(feature = "websocket", feature = "webtransport")
))]
fn mounted_websocket_path(
    mount: Option<&claviron_http::ext::PathPrefix>,
    path: &claviron_http::ext::PathPrefix,
) -> Arc<str> {
    match mount.map(|mount| mount.as_str()) {
        None | Some("/") => Arc::from(path.as_str()),
        Some(mount) if path.as_str() == "/" => Arc::from(mount),
        Some(mount) => Arc::from(format!("{mount}{}", path.as_str())),
    }
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

#[cfg(feature = "upstream")]
fn build_config_upstream_bypass(
    state: &AppState,
    canonical: &Host,
    host_cfg: Arc<HostConfig>,
) -> Option<BypassService> {
    if !host_cfg.can_bypass_axum_for_upstream() {
        return None;
    }

    let upstream_name = host_cfg.upstream_name()?;

    let Some(provider) = state.registry_ref().resolve::<UpstreamProvider>()
    else {
        error!(
            host = %canonical,
            upstream = upstream_name,
            "host declares bypass-capable upstream but UpstreamProvider is not registered"
        );
        return None;
    };

    let Some(app) = provider.app_for_state(upstream_name, state) else {
        error!(
            host = %canonical,
            upstream = upstream_name,
            "host declares unknown bypass-capable upstream app/profile"
        );
        return None;
    };

    tracing::debug!(
        host = %canonical,
        upstream = upstream_name,
        app = app.name(),
        "mounting bypass HTTP upstream"
    );

    let mut files = host_cfg.files.clone();
    files.root = host_cfg.root();
    let policy = claviron_upstream::proxy::service::UpstreamRequestPolicy {
        allow_h1_upgrade: host_cfg.transport.h1.upgrade,
        allow_connect_protocol: host_cfg.transport.h2.connect_protocol,
    };
    Some(BypassService::Upstream(UpstreamBypass::new(
        app,
        state.clone(),
        Arc::new(files),
        policy,
    )))
}

#[cfg(all(test, feature = "lua", feature = "websocket"))]
#[path = "../../tests/pipeline/apps.rs"]
mod websocket_tests;
