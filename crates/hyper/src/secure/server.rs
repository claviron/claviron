//! TLS TCP listener groups; sockets and gates belong to each run generation.
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use claviron_edge::deny::DenyService;
use claviron_edge::transport::VhostResolver;
use claviron_http::ListenerId;
use claviron_http::config::{
    HttpSnapshot, ListenerCapabilities, ListenerConfig, ListenerSettings,
    TcpListenerConfig, TcpSecurity,
};
use claviron_http::listener::{ListenerSnapshot, ListenerSnapshots};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable, Format,
};
use claviron_runtime::registry::{
    Component, Error, Reloadable, Resolve, Runnable, priority,
};
use claviron_runtime::{AppState, Register, RunContext};
use claviron_tls::orbit::{FleetServerSessions, FleetServerSessionsProvider};
use claviron_tls::resolver::CertResolver;
use tracing::info;

use super::tls::{TlsAccept, VhostTlsPolicies};

pub(crate) struct ServerSnapshot {
    pub(crate) listener: Arc<ListenerConfig>,
    pub(crate) config: TcpListenerConfig,
    /// The router's deny matcher, compiled once per snapshot and shared by
    /// every connection, as the cleartext and H3 listeners do.
    pub(crate) deny: Arc<DenyService>,
    pub(crate) tls_accept: TlsAccept,
}
impl ListenerSnapshot for ServerSnapshot {
    fn listener_id(&self) -> &ListenerId {
        &self.listener.id
    }

    fn same_binding(
        &self,
        other: &Self,
    ) -> bool {
        self.listener.same_binding(&other.listener)
    }
}
impl crate::listener::TcpListenerSnapshot for ServerSnapshot {
    fn listener(&self) -> &ListenerConfig {
        &self.listener
    }
}

pub struct HttpsServer {
    pub(crate) state: AppState,
    pub(crate) current: ListenerSnapshots<ServerSnapshot>,
}
/// What the listeners serve with, each a hard requirement.
pub(crate) type ServerDeps =
    (Arc<CertResolver>, Arc<VhostResolver>, Arc<FleetServerSessionsProvider>);

pub(crate) struct Deps {
    pub(crate) cert_resolver: Arc<CertResolver>,
    pub(crate) vhost_resolver: Arc<VhostResolver>,
    pub(crate) tls_sessions: Arc<FleetServerSessions>,
}

impl Deps {
    fn bind(
        (cert_resolver, vhost_resolver, sessions): ServerDeps
    ) -> Result<Arc<Self>, Error> {
        let tls_sessions = sessions.handle().ok_or_else(|| {
            Error::msg("HttpsServer: fleet TLS sessions unavailable")
        })?;
        Ok(Arc::new(Self { cert_resolver, vhost_resolver, tls_sessions }))
    }
}
impl HttpsServer {
    pub fn empty(state: AppState) -> Self {
        Self { state, current: ListenerSnapshots::default() }
    }
    /// A reload or a source change receives no component dependencies; it
    /// resolves the ones `run` is handed.
    fn resolve_deps(
        &self,
        state: &AppState,
    ) -> Result<Arc<Deps>, Error> {
        Deps::bind(<ServerDeps as Resolve<AppState>>::resolve(
            state.registry_ref(),
        )?)
    }

    pub async fn serve(
        self: Arc<Self>,
        deps: ServerDeps,
        context: RunContext,
    ) -> Result<(), Error> {
        // Runtime resources become available after their owners' preparation;
        // this driver needs no boot hook to receive them or bind its sockets.
        let deps = Deps::bind(deps)?;
        let http = self.state.config_reader().snapshot::<Self>()?;
        let groups = build_snapshots(&http, &deps)?;
        let generation = self.current.start(groups.clone())?;
        prepare_vhosts(&http, &deps, &groups)?;
        let server = self.clone();
        crate::listener::serve(
            &self.current,
            generation.groups(),
            &context,
            self.state.run_mode(),
            move |snapshot, stream, peer, permit| {
                server.clone().handle_connection(snapshot, stream, peer, permit)
            },
        )
        .await
    }
}
pub(crate) fn build_snapshots(
    http: &HttpSnapshot,
    deps: &Deps,
) -> Result<BTreeMap<ListenerId, Arc<ServerSnapshot>>, Error> {
    let deny = Arc::new(DenyService::from_set(http.router.deny_set()));
    let mut groups = BTreeMap::new();
    for (id, listener) in &http.listeners {
        let ListenerSettings::Tcp(config) = &listener.settings else {
            continue;
        };
        if !matches!(config.security, TcpSecurity::Tls { .. }) {
            continue;
        }
        let tls_accept = TlsAccept::build(
            deps.cert_resolver.clone(),
            deps.vhost_resolver.clone(),
            deps.tls_sessions.as_ref(),
            listener,
            config,
        )?;
        groups.insert(
            id.clone(),
            Arc::new(ServerSnapshot {
                listener: listener.clone(),
                config: config.clone(),
                deny: deny.clone(),
                tls_accept,
            }),
        );
    }
    Ok(groups)
}

fn prepare_vhosts(
    http: &HttpSnapshot,
    deps: &Deps,
    groups: &BTreeMap<ListenerId, Arc<ServerSnapshot>>,
) -> Result<(), Error> {
    let listeners = http.listeners.clone();
    let certificates = deps.cert_resolver.clone();
    let sessions = deps.tls_sessions.clone();
    let initial: BTreeMap<_, _> = groups
        .iter()
        .map(|(id, group)| (id.clone(), group.tls_accept.prepared_policy()))
        .collect();
    deps.vhost_resolver
        .prepare_publications(
            move |table, previous: Option<&VhostTlsPolicies>| {
                let mut policies = BTreeMap::new();
                for (id, listener) in &listeners {
                    let ListenerSettings::Tcp(config) = &listener.settings
                    else {
                        continue;
                    };
                    if !matches!(config.security, TcpSecurity::Tls { .. }) {
                        continue;
                    }
                    let old = previous
                        .and_then(|previous| previous.0.get(id))
                        .or_else(|| initial.get(id))
                        .map(Arc::as_ref);
                    policies.insert(
                        id.clone(),
                        Arc::new(
                            TlsAccept::prepare_policy(
                                certificates.clone(),
                                &sessions,
                                listener,
                                config,
                                table,
                                old,
                            )
                            .map_err(claviron_edge::Error::other)?,
                        ),
                    );
                }
                Ok(VhostTlsPolicies(policies))
            },
        )
        .map_err(|error| Error::msg(error.to_string()))?;
    Ok(())
}

/// Resolved inputs owned by this transport, excluding sibling listeners.
#[derive(PartialEq, Eq, serde::Deserialize)]
#[serde(try_from = "HttpSnapshot")]
pub struct HttpsServerConfig(HttpSnapshot);

impl std::ops::Deref for HttpsServerConfig {
    type Target = HttpSnapshot;
    fn deref(&self) -> &HttpSnapshot {
        &self.0
    }
}

impl TryFrom<HttpSnapshot> for HttpsServerConfig {
    type Error = claviron_http::config::ConfigError;
    fn try_from(
        mut config: HttpSnapshot
    ) -> std::result::Result<Self, Self::Error> {
        ListenerCapabilities { tls: true, h3: true }.validate(&config)?;
        config.listeners.retain(|_, listener| {
            matches!(&listener.settings, ListenerSettings::Tcp(tcp)
                if matches!(tcp.security, TcpSecurity::Tls { .. }))
        });
        config.watchdog = None;
        #[cfg(feature = "metrics")]
        {
            config.metrics = Arc::default();
        }
        Ok(Self(config))
    }
}

#[async_trait]
impl Configurable for HttpsServer {
    type Config = HttpsServerConfig;
    fn source(&self) -> ConfigFile {
        ConfigFile {
            path: self.state.config_dir().join("http.yaml"),
            format: Format::Yaml,
        }
    }
    /// No file, no listeners, as for every transport reading `http.yaml`.
    fn absent_source(&self) -> Option<HttpsServerConfig> {
        HttpsServerConfig::try_from(HttpSnapshot::default()).ok()
    }

    async fn configure(
        &self,
        _config: &Self::Config,
    ) -> congif::Result<()> {
        // Binding waits until run: sibling runtime dependencies must be ready.
        Ok(())
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for HttpsServer {
    async fn config_changed(
        &self,
        config: &Self::Config,
    ) -> congif::Result<()> {
        let deps =
            self.resolve_deps(&self.state).map_err(congif::Error::msg)?;
        let groups =
            build_snapshots(config, &deps).map_err(congif::Error::msg)?;
        self.current.publish(groups.clone()).map_err(congif::Error::msg)?;
        prepare_vhosts(config, &deps, &groups).map_err(congif::Error::msg)?;
        info!("🔁 HttpsServer listener policies reloaded");
        Ok(())
    }
}
impl Register<AppState> for HttpsServer {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        state.bind_config_environment::<Self>()?;
        claviron_tls::orbit::register(state)?;
        if !state.is_master() {
            state
                .registry_ref()
                .register(Arc::new(Self::empty(state.clone())))?;
        }
        Ok(())
    }
}

impl Component<AppState> for HttpsServer {
    type Deps = ServerDeps;

    fn name(&self) -> &'static str {
        "HttpsServer"
    }
    fn run_priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}
#[async_trait]
impl Reloadable<AppState> for HttpsServer {
    fn priority(&self) -> Option<u8> {
        Some(priority::NORMAL)
    }
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<(), Error> {
        let deps = self.resolve_deps(state)?;
        let http = state.config_reader().snapshot::<Self>()?;
        let groups = build_snapshots(&http, &deps)?;
        self.current.publish(groups.clone())?;
        prepare_vhosts(&http, &deps, &groups)?;
        info!("🔁 HttpsServer listener policies reloaded");
        Ok(())
    }
}
#[async_trait]
impl Runnable<AppState, ServerDeps> for HttpsServer {
    async fn run(
        self: Arc<Self>,
        deps: ServerDeps,
        _state: AppState,
        context: RunContext,
    ) -> Result<(), Error> {
        self.serve(deps, context).await
    }
}
