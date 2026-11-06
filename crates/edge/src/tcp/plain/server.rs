//! Listener snapshots and the provider lifecycle for the cleartext driver.
//!
//! Connection driving is `conn.rs` and request handling is `handler.rs`, the
//! same split the TLS driver uses, so one side is learned by reading the other.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::transport::instances::ListenerInstances;
use crate::transport::publication::{
    self, ListenerPublication, PreparedListener,
};
use crate::transport::{VhostResolver, VhostTable};
use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use claviron_http::ListenerId;
use claviron_http::config::{
    H1Config, HttpSnapshot, ListenerCapabilities, ListenerConfig,
    ListenerSettings, TcpConfig, TcpSecurity,
};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable, Format,
};
use claviron_runtime::registry::{Component, Reloadable, Runnable, priority};
use claviron_runtime::{
    AppState, ExecutionSpec, Register, RunContext, RunnableInstance,
};
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
    pub(super) hosts: Arc<VhostTable>,
    pub(super) non_serving: crate::pipeline::NonServingPipeline,
}
impl crate::tcp::listener::TcpListenerSnapshot for ServerSnapshot {
    fn listener(&self) -> &ListenerConfig {
        &self.listener
    }
}
impl claviron_transport::tcp::plain::Cleartext for ServerSnapshot {
    type Requests = super::handler::H1Service;

    fn tcp(&self) -> &TcpConfig {
        &self.tcp
    }
    fn h1(&self) -> &H1Config {
        &self.h1
    }
    fn connection(
        self: &Arc<Self>,
        facts: Arc<claviron_http::ext::ConnCtx>,
    ) -> Self::Requests {
        super::handler::H1Service::new(self.clone(), facts)
    }
}

pub struct HttpServer {
    state: AppState,
    instances: Mutex<ListenerInstances<HttpListener>>,
}
impl HttpServer {
    pub fn empty(state: AppState) -> Self {
        Self { state, instances: Mutex::default() }
    }

    fn configure_instances(
        &self,
        config: &HttpServerConfig,
    ) {
        let mut instances =
            self.instances.lock().expect("HTTP listener declarations poisoned");
        instances.reconcile(config.listeners.keys(), |id| HttpListener {
            id: id.clone(),
            name: format!("Listener:{id}").into(),
            state: self.state.clone(),
            current: Arc::default(),
            input: ArcSwap::from_pointee(
                publication::select_listener(config, id)
                    .expect("declared HTTP listener"),
            ),
        });
    }

    fn refresh(
        &self,
        config: &HttpServerConfig,
    ) -> claviron_runtime::registry::Result<()> {
        let vhosts = publication::resolver(&self.state)?;
        vhosts.update_listener(|hosts| {
            let instances = self
                .instances
                .lock()
                .expect("HTTP listener declarations poisoned");
            let mut prepared = Vec::new();
            for instance in instances.iter() {
                if config.listeners.contains_key(&instance.id) {
                    let input =
                        publication::select_listener(config, &instance.id)
                            .map_err(crate::Error::other)?;
                    prepared.push((
                        instance.clone(),
                        input.clone(),
                        instance.prepare_input(hosts, &input, true)?,
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

    /// Prepare the native cleartext handler without binding or booting a server.
    /// Accepted streams use the same request and drain path as `serve`.
    pub fn connection_handler(
        &self,
        config: &HttpServerConfig,
        listener: &claviron_http::ListenerId,
        vhosts: Arc<VhostResolver>,
    ) -> claviron_runtime::registry::Result<super::ConnectionHandler> {
        let input = publication::select_listener(config, listener)?;
        let hosts = Arc::new(publication::prepare_hosts(
            &self.state,
            &vhosts.snapshot(),
            &input,
            None,
        )?);
        let snapshot = build_snapshots(&input, hosts)?
            .remove(listener)
            .ok_or_else(|| {
                claviron_runtime::registry::Error::msg(format!(
                    "cleartext listener '{listener}' is not configured"
                ))
            })?;
        Ok(super::ConnectionHandler::new(snapshot))
    }

    pub async fn serve(
        self: Arc<Self>,
        vhosts: Arc<VhostResolver>,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        use futures_util::stream::{FuturesUnordered, StreamExt};
        let instances: Vec<_> = self
            .instances
            .lock()
            .expect("HTTP listener declarations poisoned")
            .iter()
            .cloned()
            .collect();
        let mut running = FuturesUnordered::new();
        for instance in instances {
            running.push(instance.run(
                (vhosts.clone(),),
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

pub(super) fn build_snapshots(
    http: &HttpSnapshot,
    hosts: Arc<VhostTable>,
) -> claviron_runtime::registry::Result<BTreeMap<ListenerId, Arc<ServerSnapshot>>>
{
    let mut groups = BTreeMap::new();
    for (id, listener) in &http.listeners {
        let ListenerSettings::Tcp(config) = &listener.settings else {
            continue;
        };
        let TcpSecurity::Cleartext { redirect_port } = &config.security else {
            if !cfg!(feature = "tls") {
                return Err(claviron_runtime::registry::Error::msg(format!(
                    "listener '{id}' requires the Hyper tls feature"
                )));
            }
            continue;
        };
        groups.insert(
            id.clone(),
            Arc::new(ServerSnapshot {
                listener: listener.clone(),
                tcp: config.tcp.clone(),
                h1: config.h1.clone(),
                hosts: hosts.clone(),
                non_serving: crate::pipeline::NonServingPipeline::new(
                    *redirect_port,
                    hosts
                        .prepared::<crate::pipeline::HostAppSnapshot>()
                        .and_then(|apps| apps.non_serving_interception()),
                ),
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
impl ConfigObserver for HttpServer {
    async fn config_changed(
        &self,
        config: &Self::Config,
    ) -> congif::Result<()> {
        self.refresh(config).map_err(congif::Error::msg)?;
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
    /// Ordinary requests resolve through the vhost table.
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
    fn runnables(self: Arc<Self>) -> Vec<RunnableInstance<AppState>> {
        self.instances
            .lock()
            .expect("HTTP listener declarations poisoned")
            .iter()
            .map(|instance| {
                RunnableInstance::new(instance.name.clone(), instance.clone())
            })
            .collect()
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
        let config = state.config_reader().snapshot::<Self>()?;
        self.refresh(&config)?;
        info!("🔁 HttpServer listener policies reloaded");
        Ok(())
    }
}

struct HttpListener {
    id: ListenerId,
    name: Arc<str>,
    state: AppState,
    current: Arc<ArcSwapOption<ServerSnapshot>>,
    input: ArcSwap<HttpSnapshot>,
}

impl HttpListener {
    fn prepare_input(
        &self,
        hosts: &VhostTable,
        input: &HttpSnapshot,
        refresh_factories: bool,
    ) -> crate::Result<PreparedListener> {
        let previous =
            if refresh_factories { None } else { self.current.load_full() };
        let hosts = Arc::new(
            publication::prepare_hosts(
                &self.state,
                hosts,
                input,
                previous.as_ref().map(|old| old.hosts.as_ref()),
            )
            .map_err(crate::Error::other)?,
        );
        let snapshot = build_snapshots(input, hosts)
            .map_err(crate::Error::other)?
            .remove(&self.id)
            .ok_or_else(|| {
                crate::Error::other(format!(
                    "listener '{}' is not cleartext TCP",
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

impl ListenerPublication for HttpListener {
    fn listener_id(&self) -> &ListenerId {
        &self.id
    }
    fn prepare(
        &self,
        hosts: &VhostTable,
        refresh_factories: bool,
    ) -> crate::Result<PreparedListener> {
        self.prepare_input(hosts, &self.input.load(), refresh_factories)
    }
}

#[async_trait]
impl Reloadable<AppState> for HttpListener {
    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let input = publication::read_listener(state, &self.id).await?;
        publication::resolver(state)?.update_listener(|hosts| {
            self.prepare_input(hosts, &input, true)?();
            self.input.store(Arc::new(input));
            Ok(())
        })?;
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<VhostResolver>,)> for HttpListener {
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
    fn execution_spec(
        &self,
        _: &AppState,
    ) -> ExecutionSpec {
        self.input.load().listeners[&self.id].execution_spec()
    }
    async fn run(
        self: Arc<Self>,
        (vhosts,): (Arc<VhostResolver>,),
        _: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let _generation = vhosts.register_listener(self.clone(), || {
            let input = self.input.load();
            let listener =
                input.listeners.get(&self.id).cloned().ok_or_else(|| {
                    crate::Error::other("HTTP listener not configured")
                })?;
            vhosts.start_listener_binding(listener)
        })?;
        crate::tcp::listener::serve(
            &self.current,
            self.current.load_full().expect("HTTP listener prepared"),
            &context,
            self.state.run_mode(),
            move |snapshot, stream, peer, permit| {
                super::ConnectionHandler::new(snapshot)
                    .serve(stream, peer, permit)
            },
        )
        .await
    }
}
