//! Named QUIC listener snapshots and the H3 provider/runnable boundary.
//! Congif prepares config; reload publishes policy for new connections;
//! run owns one fresh bind/admission/drain generation.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::transport::VhostTable;
use crate::transport::instances::ListenerInstances;
use crate::transport::publication::{
    self, ListenerPublication, PreparedListener,
};
use crate::transport::{HostAppResolver, VhostResolver, WebTransportRegistry};
use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use claviron_http::ListenerId;
use claviron_http::config::{
    HttpSnapshot, ListenerCapabilities, ListenerConfig, ListenerSettings,
    QuicListenerConfig,
};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{
    Component, Error, Reloadable, Resolve, Runnable, priority,
};
use claviron_runtime::{AppState, Register, RunContext, RunnableInstance};
use claviron_tls::orbit::{FleetServerSessions, FleetServerSessionsProvider};
use claviron_tls::resolver::CertResolver;
use claviron_tls::revocation::Revocations;
use tracing::info;

use crate::quic::tls::H3TlsAccept;

/// Per-snapshot view: every request handler reads the same Arc, so reloads
/// stay coherent across in-flight QUIC streams.
pub(crate) struct ServerSnapshot {
    pub(crate) listener: Arc<ListenerConfig>,
    pub(crate) config: QuicListenerConfig,
    pub(crate) tls_accept: H3TlsAccept,
}

/// What the listeners serve with, each a hard requirement.
pub(crate) type ServerDeps = (
    Arc<CertResolver>,
    // Preserve application preparation order; requests use the captured publication.
    Arc<HostAppResolver>,
    Arc<VhostResolver>,
    Arc<WebTransportRegistry>,
    Arc<FleetServerSessionsProvider>,
    Arc<Revocations>,
);

pub(crate) struct Deps {
    pub(crate) cert_resolver: Arc<CertResolver>,
    pub(crate) vhost_resolver: Arc<VhostResolver>,
    pub(crate) web_transport: Arc<WebTransportRegistry>,

    pub(crate) tls_sessions: Arc<FleetServerSessions>,
    pub(crate) revocations: Arc<Revocations>,
}

impl Deps {
    pub(super) fn bind(
        (
            cert_resolver,
            _,
            vhost_resolver,
            web_transport,
            sessions,
            revocations,
        ): ServerDeps
    ) -> Result<Arc<Self>, Error> {
        let tls_sessions = sessions.handle().ok_or_else(|| {
            Error::msg("H3Server: fleet TLS sessions not available")
        })?;
        Ok(Arc::new(Self {
            cert_resolver,
            vhost_resolver,
            web_transport,
            tls_sessions,
            revocations,
        }))
    }
}

pub struct H3Server {
    pub(crate) state: AppState,
    instances: Mutex<ListenerInstances<H3Listener>>,
}

impl H3Server {
    /// Lightweight constructor (no I/O, no registry lookups).
    pub fn empty(state: AppState) -> Self {
        Self { state, instances: Mutex::default() }
    }

    fn configure_instances(
        &self,
        config: &H3ServerConfig,
    ) {
        self.instances
            .lock()
            .expect("QUIC listener declarations poisoned")
            .reconcile(config.listeners.keys(), |id| H3Listener {
                id: id.clone(),
                name: format!("Listener:{id}").into(),
                state: self.state.clone(),
                current: Arc::default(),
                input: ArcSwap::from_pointee(
                    publication::select_listener(config, id)
                        .expect("declared QUIC listener"),
                ),
            });
    }

    fn refresh(
        &self,
        config: &H3ServerConfig,
    ) -> Result<(), Error> {
        // Nothing serves TLS here and nothing is about to: there is nothing
        // to prepare, and without `tls.yaml` nothing to prepare it with.
        if config.listeners.is_empty()
            && self
                .instances
                .lock()
                .expect("QUIC listener declarations poisoned")
                .iter()
                .next()
                .is_none()
        {
            return Ok(());
        }
        crate::transport::tls::require_certificates(
            &self.state,
            config.listeners.keys(),
        )?;
        let deps = self.resolve_deps(&self.state)?;
        deps.vhost_resolver.update_listener(|hosts| {
            let instances = self
                .instances
                .lock()
                .expect("QUIC listener declarations poisoned");
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

    /// A reload or a source change receives no component dependencies; it
    /// resolves the ones `run` is handed.
    pub(crate) fn resolve_deps(
        &self,
        state: &AppState,
    ) -> Result<Arc<Deps>, Error> {
        Deps::bind(<ServerDeps as Resolve<AppState>>::resolve(
            state.registry_ref(),
        )?)
    }
}

pub(crate) fn build_snapshots(
    http: &HttpSnapshot,
    deps: &Deps,
    hosts: Arc<VhostTable>,
    previous: Option<&ServerSnapshot>,
) -> Result<BTreeMap<ListenerId, Arc<ServerSnapshot>>, Error> {
    let mut groups = BTreeMap::new();
    for (id, listener) in &http.listeners {
        let ListenerSettings::Quic(config) = &listener.settings else {
            continue;
        };
        let tls_accept = H3TlsAccept::build(
            deps,
            &config.quic,
            id,
            hosts.clone(),
            previous.map(|old| &old.tls_accept),
        )?;
        groups.insert(
            id.clone(),
            Arc::new(ServerSnapshot {
                listener: listener.clone(),
                config: config.clone(),
                tls_accept,
            }),
        );
    }
    Ok(groups)
}

/// The QUIC listeners of `http.yaml`: the shared document, validated whole,
/// narrowed to what this transport owns.
#[derive(PartialEq, Eq, serde::Deserialize)]
#[serde(try_from = "HttpSnapshot")]
pub struct H3ServerConfig(HttpSnapshot);

impl std::ops::Deref for H3ServerConfig {
    type Target = HttpSnapshot;
    fn deref(&self) -> &HttpSnapshot {
        &self.0
    }
}

impl TryFrom<HttpSnapshot> for H3ServerConfig {
    type Error = claviron_http::config::ConfigError;
    fn try_from(
        mut config: HttpSnapshot
    ) -> std::result::Result<Self, Self::Error> {
        ListenerCapabilities::ALL.validate(&config)?;
        config.listeners.retain(|_, listener| {
            matches!(listener.settings, ListenerSettings::Quic(_))
        });
        Ok(Self(config))
    }
}

#[async_trait]
impl Configurable for H3Server {
    type Config = H3ServerConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(self.state.config_dir().join("http.yaml"))
    }

    /// An absent `http.yaml` declares no listeners, as it does for the other
    /// transports reading the same document.
    fn absent_source(&self) -> Option<H3ServerConfig> {
        H3ServerConfig::try_from(HttpSnapshot::default()).ok()
    }

    async fn configure(
        &self,
        config: &H3ServerConfig,
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
impl ConfigObserver for H3Server {
    async fn config_changed(
        &self,
        config: &H3ServerConfig,
    ) -> congif::Result<()> {
        self.refresh(config).map_err(congif::Error::msg)?;
        info!("🔁 H3Server reconfigured");
        Ok(())
    }
}

impl Register<AppState> for H3Server {
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

impl Component<AppState> for H3Server {
    type Deps = ServerDeps;

    fn name(&self) -> &'static str {
        "H3Server"
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
        crate::transport::tls::require_certificates(
            state,
            self.instances
                .lock()
                .expect("QUIC listener declarations poisoned")
                .iter()
                .map(|instance| &instance.id),
        )
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn runnables(self: Arc<Self>) -> Vec<RunnableInstance<AppState>> {
        self.instances
            .lock()
            .expect("QUIC listener declarations poisoned")
            .iter()
            .map(|instance| {
                RunnableInstance::new(instance.name.clone(), instance.clone())
            })
            .collect()
    }
}

#[async_trait]
impl Reloadable<AppState> for H3Server {
    fn priority(&self) -> Option<u8> {
        Some(priority::NORMAL)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.refresh(&config)?;
        info!("🔁 H3Server reloaded");
        Ok(())
    }
}

impl H3Server {
    pub async fn serve(
        self: Arc<Self>,
        deps: ServerDeps,
        context: RunContext,
    ) -> Result<(), Error> {
        use futures_util::stream::{FuturesUnordered, StreamExt};
        let instances: Vec<_> = self
            .instances
            .lock()
            .expect("QUIC listener declarations poisoned")
            .iter()
            .cloned()
            .collect();
        let mut running = FuturesUnordered::new();
        for instance in instances {
            running.push(instance.run(
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

pub(super) struct H3Listener {
    id: ListenerId,
    name: Arc<str>,
    pub(super) state: AppState,
    pub(super) current: Arc<ArcSwapOption<ServerSnapshot>>,
    input: ArcSwap<HttpSnapshot>,
}

impl H3Listener {
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
        let snapshot = build_snapshots(input, deps, hosts, previous.as_deref())
            .map_err(crate::Error::other)?
            .remove(&self.id)
            .ok_or_else(|| {
                crate::Error::other(format!(
                    "listener '{}' is not QUIC",
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

impl ListenerPublication for H3Listener {
    fn listener_id(&self) -> &ListenerId {
        &self.id
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
impl Reloadable<AppState> for H3Listener {
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
impl Runnable<AppState, ServerDeps> for H3Listener {
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
    fn execution_spec(
        &self,
        _: &AppState,
    ) -> claviron_runtime::ExecutionSpec {
        self.input.load().listeners[&self.id].execution_spec()
    }
    async fn run(
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
                        || crate::Error::other("QUIC listener not configured"),
                    )?;
                deps.vhost_resolver.start_listener_binding(listener)
            })?;
        self.serve_groups(
            self.current.load_full().expect("QUIC listener prepared"),
            deps.clone(),
            context,
        )
        .await
    }
}
