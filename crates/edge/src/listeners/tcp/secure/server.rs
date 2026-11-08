//! TLS TCP listener groups; sockets and gates belong to each run generation.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::transport::publication::{
    self, ListenerPublication, PreparedListener,
};
use crate::transport::{VhostResolver, VhostTable};
use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use claviron_http::ListenerId;
use claviron_http::config::{
    HttpSnapshot, ListenerCapabilities, ListenerConfig, ListenerSettings,
    TcpListenerConfig, TcpSecurity,
};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable, Format,
};
use claviron_runtime::registry::{
    Component, Error, Reloadable, Resolve, priority,
};
use claviron_runtime::{AppState, Register, RunContext, RunnableInstance};
use claviron_tls::orbit::{FleetServerSessions, FleetServerSessionsProvider};
use claviron_tls::resolver::CertResolver;
use claviron_tls::revocation::Revocations;
use claviron_transport::instances::ListenerInstances;
use claviron_transport::listener::Listener;
use tracing::info;

use super::tls::TlsAccept;

pub(crate) struct ServerSnapshot {
    pub(crate) listener: Arc<ListenerConfig>,
    pub(crate) config: TcpListenerConfig,
    pub(crate) tls_accept: TlsAccept,
    state: AppState,
}
impl claviron_transport::tcp::listener::TcpListenerSnapshot for ServerSnapshot {
    fn listener(&self) -> &ListenerConfig {
        &self.listener
    }
}
impl claviron_transport::tcp::secure::Secure for ServerSnapshot {
    type Host = super::conn::SelectedHost;

    fn config(&self) -> &TcpListenerConfig {
        &self.config
    }
    fn select(
        self: &Arc<Self>,
        server_name: Option<&str>,
    ) -> std::io::Result<(Arc<rustls::ServerConfig>, Self::Host)> {
        let (config, vctx) = self.tls_accept.select(server_name)?;
        Ok((
            config,
            super::conn::SelectedHost {
                vctx,
                listener: self.listener.clone(),
                state: self.state.clone(),
            },
        ))
    }
}

pub struct HttpsServer {
    pub(crate) state: AppState,
    instances: Mutex<ListenerInstances<Listener<HttpsListener>>>,
}
/// What the listeners serve with, each a hard requirement.
pub(crate) type ServerDeps = (
    Arc<CertResolver>,
    Arc<VhostResolver>,
    Arc<FleetServerSessionsProvider>,
    Arc<Revocations>,
);

#[derive(Clone)]
pub(crate) struct Deps {
    pub(crate) cert_resolver: Arc<CertResolver>,
    pub(crate) vhost_resolver: Arc<VhostResolver>,
    pub(crate) tls_sessions: Arc<FleetServerSessions>,
    pub(crate) revocations: Arc<Revocations>,
}

impl Deps {
    fn bind(
        (cert_resolver, vhost_resolver, sessions, revocations): ServerDeps
    ) -> Result<Arc<Self>, Error> {
        let tls_sessions = sessions.handle().ok_or_else(|| {
            Error::msg("HttpsServer: fleet TLS sessions unavailable")
        })?;
        Ok(Arc::new(Self {
            cert_resolver,
            vhost_resolver,
            tls_sessions,
            revocations,
        }))
    }
}
impl HttpsServer {
    pub fn empty(state: AppState) -> Self {
        Self { state, instances: Mutex::default() }
    }
    fn configure_instances(
        &self,
        config: &HttpsServerConfig,
    ) {
        self.instances
            .lock()
            .expect("TLS listener declarations poisoned")
            .reconcile(config.listeners.keys(), |id| {
                Listener::new(HttpsListener {
                    id: id.clone(),
                    state: self.state.clone(),
                    current: Arc::default(),
                    input: ArcSwap::from_pointee(
                        publication::select_listener(config, id)
                            .expect("declared TLS listener"),
                    ),
                })
            });
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

    fn refresh(
        &self,
        config: &HttpsServerConfig,
    ) -> Result<(), Error> {
        // Nothing serves TLS here and nothing is about to: there is nothing
        // to prepare, and without `tls.yaml` nothing to prepare it with.
        if config.listeners.is_empty()
            && self
                .instances
                .lock()
                .expect("TLS listener declarations poisoned")
                .iter()
                .next()
                .is_none()
        {
            return Ok(());
        }
        crate::listeners::tls::require_certificates(
            &self.state,
            config.listeners.keys(),
        )?;
        let deps = self.resolve_deps(&self.state)?;
        deps.vhost_resolver.update_listener(|hosts| {
            let instances = self
                .instances
                .lock()
                .expect("TLS listener declarations poisoned");
            let mut prepared = Vec::new();
            for instance in instances.iter() {
                if config.listeners.contains_key(&instance.id) {
                    let input =
                        publication::select_listener(config, &instance.id)
                            .map_err(crate::Error::other)?;
                    prepared.push((
                        instance.clone(),
                        input.clone(),
                        instance.prepare_input(hosts, &input, &deps, true)?,
                    ));
                }
            }
            for (instance, input, publish) in prepared {
                publish();
                instance.input.store(Arc::new(input));
            }
            Ok(())
        })?;
        self.configure_instances(config);
        Ok(())
    }

    /// Prepare the native TLS handler without binding or booting a server.
    /// The caller supplies what a booted server resolves from its registry;
    /// accepted streams use the same handshake, request and drain path as
    /// `serve`.
    pub fn connection_handler(
        &self,
        config: &HttpsServerConfig,
        listener: &ListenerId,
        cert_resolver: Arc<CertResolver>,
        vhosts: Arc<VhostResolver>,
        tls_sessions: Arc<FleetServerSessions>,
        revocations: Arc<Revocations>,
    ) -> Result<super::ConnectionHandler, Error> {
        let input = publication::select_listener(config, listener)?;
        let hosts = Arc::new(publication::prepare_hosts(
            &self.state,
            &vhosts.snapshot(),
            &input,
            None,
        )?);
        let deps = Deps {
            cert_resolver,
            vhost_resolver: vhosts,
            tls_sessions,
            revocations,
        };
        let snapshot =
            build_snapshots(&self.state, &input, &deps, hosts, None)?
                .remove(listener)
                .ok_or_else(|| {
                    Error::msg(format!("listener '{listener}' is not TLS TCP"))
                })?;
        Ok(super::ConnectionHandler::new(snapshot))
    }

    pub async fn serve(
        self: Arc<Self>,
        deps: ServerDeps,
        context: RunContext,
    ) -> Result<(), Error> {
        use futures_util::stream::{FuturesUnordered, StreamExt};
        let instances: Vec<_> = self
            .instances
            .lock()
            .expect("TLS listener declarations poisoned")
            .iter()
            .cloned()
            .collect();
        let mut running = FuturesUnordered::new();
        for instance in instances {
            running.push(claviron_transport::listener::Adapter::serve(
                instance.adapter(),
                deps.clone(),
                self.state.clone(),
                context.child(),
            ));
        }
        if running.is_empty() {
            context.cancelled().await;
        }
        let mut failure = None;
        while let Some(result) = running.next().await {
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

pub(crate) fn build_snapshots(
    state: &AppState,
    http: &HttpSnapshot,
    deps: &Deps,
    hosts: Arc<VhostTable>,
    previous: Option<&ServerSnapshot>,
) -> Result<BTreeMap<ListenerId, Arc<ServerSnapshot>>, Error> {
    let mut groups = BTreeMap::new();
    for (id, listener) in &http.listeners {
        let ListenerSettings::Tcp(config) = &listener.settings else {
            continue;
        };
        if !matches!(config.security, TcpSecurity::Tls { .. }) {
            continue;
        }
        let tls_accept = TlsAccept::new(
            deps,
            listener,
            config,
            hosts.clone(),
            previous.map(|old| &old.tls_accept),
        )?;
        groups.insert(
            id.clone(),
            Arc::new(ServerSnapshot {
                listener: listener.clone(),
                config: config.clone(),
                tls_accept,
                state: state.clone(),
            }),
        );
    }
    Ok(groups)
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
        config: &Self::Config,
    ) -> congif::Result<()> {
        // Binding waits until run: sibling runtime dependencies must be ready.
        self.configure_instances(config);
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
        self.refresh(config).map_err(congif::Error::msg)?;
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

    /// Declared listeners are refused here, by name, when the process has no
    /// certificate source, before one of them would fail to find its resolver.
    fn validate(
        &self,
        state: &AppState,
    ) -> Result<(), Error> {
        crate::listeners::tls::require_certificates(
            state,
            self.instances
                .lock()
                .expect("TLS listener declarations poisoned")
                .iter()
                .map(|instance| &instance.id),
        )
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
    fn runnables(self: Arc<Self>) -> Vec<RunnableInstance<AppState>> {
        self.instances
            .lock()
            .expect("TLS listener declarations poisoned")
            .iter()
            .map(|instance| instance.instance())
            .collect()
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
        let http = state.config_reader().snapshot::<Self>()?;
        self.refresh(&http)?;
        info!("🔁 HttpsServer listener policies reloaded");
        Ok(())
    }
}
pub(super) struct HttpsListener {
    id: ListenerId,
    pub(super) state: AppState,
    current: Arc<ArcSwapOption<ServerSnapshot>>,
    input: ArcSwap<HttpSnapshot>,
}

impl HttpsListener {
    fn prepare_input(
        &self,
        hosts: &VhostTable,
        input: &HttpSnapshot,
        deps: &Deps,
        refresh_factories: bool,
    ) -> crate::Result<PreparedListener> {
        let previous =
            if refresh_factories { None } else { self.current.load_full() };
        let hosts = Arc::new(
            publication::prepare_hosts(
                &self.state,
                hosts,
                input,
                previous.as_ref().map(|old| old.tls_accept.hosts()),
            )
            .map_err(crate::Error::other)?,
        );
        let snapshot = build_snapshots(
            &self.state,
            input,
            deps,
            hosts,
            previous.as_deref(),
        )
        .map_err(crate::Error::other)?
        .remove(&self.id)
        .ok_or_else(|| {
            crate::Error::other(format!(
                "listener '{}' is not TLS TCP",
                self.id
            ))
        })?;
        publication::resolver(&self.state)
            .map_err(crate::Error::other)?
            .validate_listener_binding(snapshot.listener.clone())?;
        let current = self.current.clone();
        Ok(Box::new(move || current.store(Some(snapshot))))
    }
}

impl ListenerPublication for HttpsListener {
    fn listener_id(&self) -> &ListenerId {
        &self.id
    }

    fn mtls_h2_port(
        &self,
        name: &claviron_types::Host,
        canonical: &claviron_types::Host,
    ) -> Option<u16> {
        let current = self.current.load();
        let snapshot = current.as_ref()?;
        if !snapshot.tls_accept.serves_mtls_h2(name, canonical) {
            return None;
        }
        let mut ports = snapshot
            .listener
            .listen
            .iter()
            .filter_map(|address| {
                address.socket().map(|address| address.port())
            })
            .filter(|&port| port != 0);
        let first = ports.next()?;
        Some(if first == 443 || ports.any(|port| port == 443) {
            443
        } else {
            first
        })
    }

    fn prepare(
        &self,
        hosts: &VhostTable,
        refresh_factories: bool,
    ) -> crate::Result<PreparedListener> {
        let deps = Deps::bind(
            ServerDeps::resolve(self.state.registry_ref())
                .map_err(crate::Error::other)?,
        )
        .map_err(crate::Error::other)?;
        self.prepare_input(hosts, &self.input.load(), &deps, refresh_factories)
    }
}

#[async_trait]
impl Reloadable<AppState> for HttpsListener {
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<(), Error> {
        let input = publication::read_listener(state, &self.id).await?;
        let deps = Deps::bind(ServerDeps::resolve(state.registry_ref())?)?;
        deps.vhost_resolver.update_listener(|hosts| {
            self.prepare_input(hosts, &input, &deps, true)?();
            self.input.store(Arc::new(input));
            Ok(())
        })?;
        Ok(())
    }
}

#[async_trait]
impl claviron_transport::listener::Adapter for HttpsListener {
    type Deps = ServerDeps;

    fn config(&self) -> Arc<ListenerConfig> {
        self.input.load().listeners[&self.id].clone()
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
    async fn serve(
        self: Arc<Self>,
        deps: ServerDeps,
        _: AppState,
        context: RunContext,
    ) -> Result<(), Error> {
        let deps = Deps::bind(deps)?;
        let _generation =
            deps.vhost_resolver.register_listener(self.clone(), || {
                let input = self.input.load();
                let listener =
                    input.listeners.get(&self.id).cloned().ok_or_else(
                        || crate::Error::other("TLS listener not configured"),
                    )?;
                deps.vhost_resolver.start_listener_binding(listener)
            })?;
        claviron_transport::tcp::listener::serve_current(
            || self.current.load_full(),
            self.current.load_full().expect("TLS listener prepared"),
            &context,
            self.state.run_mode(),
            |snapshot, stream, peer, permit| {
                claviron_transport::tcp::secure::serve(
                    stream, peer, snapshot, permit,
                )
            },
        )
        .await
    }
}
