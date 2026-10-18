//! Hosting-aware benchmark registration. Edge owns listeners, TLS and policy;
//! this adapter declares its hosts and contributes the shared native handlers.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use claviron_edge::{Host, HostConfig, Routable, VhostDocument};
use claviron_runtime::congif::{self, ConfigFile, ConfigStore, Configurable};
use claviron_runtime::registry::{Component, Reloadable};
use claviron_runtime::{AppState, Register, registry};
use serde::Deserialize;

// Default source identity; register_with_path explicitly overrides it.
const CONFIG_FILE: &str = "bench.yaml";

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    hosting: VhostDocument,
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
        state.bind_config_environment::<Self>()?;
        let service = Arc::new(Self {
            config_path: config_path.into(),
            hosts: ConfigStore::new(Vec::new()),
        });
        claviron_edge::register_routable(state, service)?;
        // Read the reader's prepared generation, not a booted resource. This
        // also works when the resolver reloads before this service does.
        claviron_edge::register_hosts(state, |state| {
            let config = state
                .config_reader()
                .snapshot::<Self>()
                .map_err(claviron_edge::Error::other)?;
            match config.as_ref() {
                Some(config) => config.hosting.declarations(),
                None => Ok(Vec::new()),
            }
        })
    }

    fn prepare_hosts(
        &self,
        config: &Option<Config>,
    ) -> congif::Result<()> {
        let hosts = match config {
            Some(config) => config
                .hosting
                .declarations()
                .map_err(|error| congif::Error::msg(error.to_string()))?
                .into_iter()
                .map(|(host, _)| host)
                .collect(),
            None => Vec::new(),
        };
        self.hosts.replace(hosts, |_| Ok(()))?;
        Ok(())
    }
}

#[async_trait]
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

#[async_trait]
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

impl Routable for BenchService {
    fn match_host(
        &self,
        key: &Host,
        _host: &HostConfig,
    ) -> bool {
        self.hosts.snapshot().contains(key)
    }

    fn routes(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> registry::Result<Option<Router<AppState>>> {
        Ok(Some(crate::bench::routes().with_state(())))
    }
}

impl Register<AppState> for BenchService {
    fn register(state: &AppState) -> registry::Result<()> {
        Self::register_with_path(state, state.config_dir().join(CONFIG_FILE))
    }
}
