//! Listener snapshots and the provider lifecycle for the cleartext driver.
//!
//! Connection driving is `conn.rs` and request handling is `handler.rs`, the
//! same split the TLS driver uses, so one side is learned by reading the other.
use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::path::PathBuf;
use std::sync::Arc;

use crate::deny::DenyService;
use crate::transport::VhostResolver;
use async_trait::async_trait;
use claviron_http::ListenerId;
use claviron_http::acme::Http01ChallengeDirectory;
use claviron_http::config::{
    H1Config, HttpSnapshot, ListenerCapabilities, ListenerConfig,
    ListenerSettings, TcpConfig, TcpSecurity,
};
use claviron_http::listener::{ListenerSnapshot, ListenerSnapshots};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable, Format,
};
use claviron_runtime::registry::{Component, Reloadable, Runnable, priority};
use claviron_runtime::{AppState, Register, RunContext};
use tracing::info;

/// One cleartext listener, as this driver needs it.
///
/// The settings are carried destructured rather than as the whole
/// `TcpListenerConfig`. This driver only ever builds snapshots for
/// `TcpSecurity::Cleartext`, and holding the enum meant re-establishing that at
/// request time with an `unreachable!` — a field cannot be the wrong variant.
/// Keeping only `tcp` and `h1` alongside them also means the snapshot holds one
/// copy of each setting instead of a second `security` nobody reads.
pub(super) struct ServerSnapshot {
    pub(super) listener: Arc<ListenerConfig>,
    pub(super) tcp: TcpConfig,
    pub(super) h1: H1Config,
    pub(super) redirect_port: NonZeroU16,
    pub(super) http01_directory: Option<Arc<PathBuf>>,
    pub(super) deny: Arc<DenyService>,
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
impl crate::tcp::listener::TcpListenerSnapshot for ServerSnapshot {
    fn listener(&self) -> &ListenerConfig {
        &self.listener
    }
}

pub struct HttpServer {
    state: AppState,
    current: ListenerSnapshots<ServerSnapshot>,
}
impl HttpServer {
    pub fn empty(state: AppState) -> Self {
        Self { current: ListenerSnapshots::default(), state }
    }

    pub async fn serve(
        self: Arc<Self>,
        vhosts: Arc<VhostResolver>,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let generation = self.current.start(build_snapshots(
            self.state.config_reader().snapshot::<Self>()?.as_ref(),
            &self.state,
        )?)?;
        let state = self.state.clone();
        crate::tcp::listener::serve(
            &self.current,
            generation.groups(),
            &context,
            self.state.run_mode(),
            move |snapshot, stream, peer, permit| {
                Self::handle_connection(
                    stream,
                    peer,
                    snapshot,
                    vhosts.clone(),
                    state.clone(),
                    permit,
                )
            },
        )
        .await
    }
}

#[cfg(test)]
#[path = "../../../tests/tcp/unit/plain.rs"]
mod tests;

pub(super) fn build_snapshots(
    http: &HttpSnapshot,
    state: &AppState,
) -> claviron_runtime::registry::Result<BTreeMap<ListenerId, Arc<ServerSnapshot>>>
{
    let deny = Arc::new(DenyService::from_set(http.router.deny_set()));
    let mut groups = BTreeMap::new();
    for (id, listener) in &http.listeners {
        let ListenerSettings::Tcp(config) = &listener.settings else {
            continue;
        };
        let TcpSecurity::Cleartext { redirect_port, acme } = &config.security
        else {
            if !cfg!(feature = "tls") {
                return Err(claviron_runtime::registry::Error::msg(format!(
                    "listener '{id}' requires the Hyper tls feature"
                )));
            }
            continue;
        };
        let http01_directory = if *acme {
            Some(
                state
                    .registry_ref()
                    .resolve::<Http01ChallengeDirectory>()
                    .ok_or_else(|| {
                        claviron_runtime::registry::Error::msg(
                            "HTTP-01 is enabled but AcmeService did not register its challenge directory"
                        )
                    })?
                    .snapshot()
                    .ok_or_else(|| {
                        claviron_runtime::registry::Error::msg(
                            "HTTP-01 is enabled but acme.yaml has not published challenges_dir"
                        )
                    })?
            )
        } else {
            None
        };
        groups.insert(
            id.clone(),
            Arc::new(ServerSnapshot {
                listener: listener.clone(),
                tcp: config.tcp.clone(),
                h1: config.h1.clone(),
                redirect_port: *redirect_port,
                http01_directory,
                deny: deny.clone(),
            }),
        );
    }
    Ok(groups)
}

/// Resolved inputs owned by this transport, excluding sibling listeners.
#[derive(PartialEq, Eq, serde::Deserialize)]
#[serde(try_from = "HttpSnapshot")]
pub struct HttpServerConfig(HttpSnapshot);

impl std::ops::Deref for HttpServerConfig {
    type Target = HttpSnapshot;
    fn deref(&self) -> &HttpSnapshot {
        &self.0
    }
}

impl TryFrom<HttpSnapshot> for HttpServerConfig {
    type Error = claviron_http::config::ConfigError;
    fn try_from(
        mut config: HttpSnapshot
    ) -> std::result::Result<Self, Self::Error> {
        ListenerCapabilities { tls: cfg!(feature = "tls"), h3: true }
            .validate(&config)?;
        config.listeners.retain(|_, listener| {
            matches!(&listener.settings, ListenerSettings::Tcp(tcp)
                if matches!(tcp.security, TcpSecurity::Cleartext { .. }))
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
impl Configurable for HttpServer {
    type Config = HttpServerConfig;
    fn source(&self) -> ConfigFile {
        ConfigFile {
            path: self.state.config_dir().join("http.yaml"),
            format: Format::Yaml,
        }
    }
    /// No file, no listeners, as for every transport reading `http.yaml`.
    fn absent_source(&self) -> Option<HttpServerConfig> {
        HttpServerConfig::try_from(HttpSnapshot::default()).ok()
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
impl ConfigObserver for HttpServer {
    async fn config_changed(
        &self,
        config: &Self::Config,
    ) -> congif::Result<()> {
        self.current
            .publish(
                build_snapshots(config, &self.state)
                    .map_err(congif::Error::msg)?,
            )
            .map_err(congif::Error::msg)?;
        info!("🔁 HttpServer listener policies reloaded");
        Ok(())
    }
}

impl Register<AppState> for HttpServer {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        state.bind_config_environment::<Self>()?;
        if !state.is_master() {
            state
                .registry_ref()
                .register(Arc::new(Self::empty(state.clone())))?;
        }
        Ok(())
    }
}

impl Component<AppState> for HttpServer {
    /// Every connection is routed through the vhost table.
    type Deps = (Arc<VhostResolver>,);

    fn name(&self) -> &'static str {
        "HttpServer"
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
impl Reloadable<AppState> for HttpServer {
    fn priority(&self) -> Option<u8> {
        Some(priority::NORMAL)
    }
    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.current.publish(build_snapshots(
            state.config_reader().snapshot::<Self>()?.as_ref(),
            &self.state,
        )?)?;
        info!("🔁 HttpServer listener policies reloaded");
        Ok(())
    }
}
#[async_trait]
impl Runnable<AppState, (Arc<VhostResolver>,)> for HttpServer {
    async fn run(
        self: Arc<Self>,
        (vhosts,): (Arc<VhostResolver>,),
        _state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        self.serve(vhosts, context).await
    }
}
