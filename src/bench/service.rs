//! One benchmark service for every process shape. Its routes are a
//! [`HostRoutable`]: on the `vhost.yaml` hosts `bench.yaml` selects where the
//! process serves hosts, and host-free in a runner.

use std::path::PathBuf;
use std::sync::Arc;

use claviron_pipeline::routing::{HostRoutable, Routable};
use claviron_runtime::congif::{self, ConfigFile, ConfigStore, Configurable};
use claviron_runtime::registry::{Component, Reloadable};
use claviron_runtime::{AppState, Register, registry};
use claviron_types::Host;

// Default source identity; register_with_path explicitly overrides it.
const CONFIG_FILE: &str = "bench.yaml";

/// The `vhost.yaml` hosts this service routes; it declares none itself.
#[derive(serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    vhosts: Vec<Host>,
}

pub struct BenchService {
    config_path: PathBuf,
    // Only boot/reload route selection uses these keys. Handlers read no config
    // and retain neither request bodies nor per-client benchmark state.
    hosts: ConfigStore<Vec<Host>>,
}

impl BenchService {
    /// Select a Congif source explicitly; paths remain process-cwd-relative.
    pub fn register_with_path(
        state: &AppState,
        config_path: impl Into<PathBuf>,
    ) -> registry::Result<()> {
        Self::join(state, config_path.into())
    }

    fn join(
        state: &AppState,
        config_path: PathBuf,
    ) -> registry::Result<()> {
        state.bind_config_environment::<Self>()?;
        let service =
            Arc::new(Self { config_path, hosts: ConfigStore::new(Vec::new()) });
        claviron_pipeline::routing::register_host_routable(state, service)?;
        tracing::info!("native bench routes registered");
        Ok(())
    }

    fn prepare_hosts(
        &self,
        config: &Option<Config>,
    ) -> congif::Result<()> {
        let hosts = match config {
            Some(config) => config.vhosts.clone(),
            None => Vec::new(),
        };
        self.hosts.replace(hosts, |_| Ok(()))?;
        Ok(())
    }
}

impl Register<AppState> for BenchService {
    fn register(state: &AppState) -> registry::Result<()> {
        Self::join(state, state.config_dir().join(CONFIG_FILE))
    }
}

impl Routable for BenchService {
    fn routes(
        self: Arc<Self>,
        state: &AppState,
    ) -> registry::Result<Option<axum::Router<AppState>>> {
        Ok(Some(crate::bench::routes_with_state(state).with_state(())))
    }
}

impl HostRoutable for BenchService {
    fn match_host(
        &self,
        key: &Host,
    ) -> bool {
        self.hosts.snapshot().contains(key)
    }
}

impl Component<AppState> for BenchService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "BenchService"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl Configurable for BenchService {
    type Config = Option<Config>;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    fn absent_source(&self) -> Option<Self::Config> {
        Some(None)
    }

    async fn configure(
        &self,
        config: &Self::Config,
    ) -> congif::Result<()> {
        self.prepare_hosts(config)
    }
}

#[async_trait::async_trait]
impl Reloadable<AppState> for BenchService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.prepare_hosts(config.as_ref())?;
        Ok(())
    }
}
