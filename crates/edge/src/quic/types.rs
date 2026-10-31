//! Named QUIC listener snapshots and the H3 provider/runnable boundary.
//! Congif prepares config; reload publishes policy for new connections;
//! run owns one fresh bind/admission/drain generation.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::deny::DenyService;
use crate::transport::{HostAppResolver, VhostResolver, WebTransportRegistry};
use async_trait::async_trait;
use claviron_http::ListenerId;
use claviron_http::config::{
    HttpSnapshot, ListenerCapabilities, ListenerConfig, ListenerSettings,
    QuicListenerConfig,
};
use claviron_http::listener::{ListenerSnapshot, ListenerSnapshots};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{
    Component, Error, Reloadable, Resolve, Runnable, priority,
};
use claviron_runtime::{AppState, Register, RunContext};
use claviron_tls::orbit::{FleetServerSessions, FleetServerSessionsProvider};
use claviron_tls::resolver::CertResolver;
use claviron_tls::revocation::Revocations;
use tracing::info;

use crate::quic::tls::{H3TlsAccept, VhostTlsPolicies};

/// Per-snapshot view: every request handler reads the same Arc, so reloads
/// stay coherent across in-flight QUIC streams.
pub(crate) struct ServerSnapshot {
    pub(crate) listener: Arc<ListenerConfig>,
    pub(crate) config: QuicListenerConfig,
    /// Captured listener request-default matcher, shared with HostService.
    /// Admission occurs before body reading and native WebTransport acceptance.
    pub(crate) deny: Arc<DenyService>,
    pub(crate) tls_accept: H3TlsAccept,
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

/// What the listeners serve with, each a hard requirement.
pub(crate) type ServerDeps = (
    Arc<CertResolver>,
    Arc<HostAppResolver>,
    Arc<VhostResolver>,
    Arc<WebTransportRegistry>,
    Arc<FleetServerSessionsProvider>,
    Arc<Revocations>,
);

pub(crate) struct Deps {
    pub(crate) cert_resolver: Arc<CertResolver>,
    pub(crate) app_resolver: Arc<HostAppResolver>,
    pub(crate) vhost_resolver: Arc<VhostResolver>,
    pub(crate) web_transport: Arc<WebTransportRegistry>,

    pub(crate) tls_sessions: Arc<FleetServerSessions>,
    pub(crate) revocations: Arc<Revocations>,
}

impl Deps {
    fn bind(
        (
            cert_resolver,
            app_resolver,
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
            app_resolver,
            vhost_resolver,
            web_transport,
            tls_sessions,
            revocations,
        }))
    }
}

pub struct H3Server {
    pub(crate) state: AppState,
    pub(crate) current: ListenerSnapshots<ServerSnapshot>,
}

impl H3Server {
    /// Lightweight constructor (no I/O, no registry lookups).
    pub fn empty(state: AppState) -> Self {
        Self { state, current: ListenerSnapshots::default() }
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
) -> Result<BTreeMap<ListenerId, Arc<ServerSnapshot>>, Error> {
    let deny = Arc::new(DenyService::from_set(http.router.deny_set()));
    let mut groups = BTreeMap::new();
    for (id, listener) in &http.listeners {
        let ListenerSettings::Quic(config) = &listener.settings else {
            continue;
        };
        let tls_accept = H3TlsAccept::build(
            deps.cert_resolver.clone(),
            deps.revocations.clone(),
            deps.vhost_resolver.clone(),
            &config.quic,
            deps.tls_sessions.as_ref(),
            &listener.id,
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

pub(crate) fn prepare_vhosts(
    deps: &Deps,
    groups: &BTreeMap<ListenerId, Arc<ServerSnapshot>>,
) -> Result<(), Error> {
    let initial: BTreeMap<_, _> = groups
        .iter()
        .map(|(id, group)| (id.clone(), group.tls_accept.prepared_policy()))
        .collect();
    let certificates = deps.cert_resolver.clone();
    deps.vhost_resolver
        .prepare_publications(
            move |table, previous: Option<&VhostTlsPolicies>| {
                let mut policies = BTreeMap::new();
                for (id, template) in &initial {
                    let old = previous
                        .and_then(|previous| previous.0.get(id))
                        .or(Some(template))
                        .map(Arc::as_ref);
                    let policy = H3TlsAccept::prepare_policy(
                        template,
                        table,
                        old,
                        certificates.clone(),
                        id,
                    )
                    .map_err(crate::Error::other)?;
                    policies.insert(id.clone(), Arc::new(policy));
                }
                Ok(VhostTlsPolicies(policies))
            },
        )
        .map_err(|error| Error::msg(error.to_string()))
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
        config.watchdog = None;
        #[cfg(feature = "metrics")]
        {
            config.metrics = Arc::default();
        }
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
        _config: &H3ServerConfig,
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
impl ConfigObserver for H3Server {
    async fn config_changed(
        &self,
        _config: &H3ServerConfig,
    ) -> congif::Result<()> {
        let deps =
            self.resolve_deps(&self.state).map_err(congif::Error::msg)?;
        let snapshot = build_snapshots(
            self.state
                .config_reader()
                .snapshot::<Self>()
                .map_err(congif::Error::msg)?
                .as_ref(),
            deps.as_ref(),
        )
        .map_err(congif::Error::msg)?;
        self.current.publish(snapshot.clone()).map_err(congif::Error::msg)?;
        prepare_vhosts(&deps, &snapshot).map_err(congif::Error::msg)?;
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

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: std::sync::Arc<Self>
    ) -> Option<std::sync::Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
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
        let deps = self.resolve_deps(state)?;
        let snapshot = build_snapshots(
            state.config_reader().snapshot::<Self>()?.as_ref(),
            deps.as_ref(),
        )?;
        self.current.publish(snapshot.clone())?;
        prepare_vhosts(&deps, &snapshot)?;
        info!("🔁 H3Server reloaded");
        Ok(())
    }
}

impl H3Server {
    /// Runnable adapter for the group lifecycle in `crate::quic::listener`.
    pub async fn serve(
        self: Arc<Self>,
        deps: ServerDeps,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        self.run_listeners(Deps::bind(deps)?, context).await
    }
}

#[async_trait]
impl Runnable<AppState, ServerDeps> for H3Server {
    async fn run(
        self: Arc<Self>,
        deps: ServerDeps,
        _state: AppState,
        context: RunContext,
    ) -> Result<(), Error> {
        self.serve(deps, context).await
    }
}
