//! HTTP layer config service.
//!
//! Congif prepares `http.yaml` before boot and reload. The whole file is read
//! once per preparation batch; downstream
//! transport servers resolve one coherent snapshot of named listener groups
//! and request defaults. Config is validated before atomic publication.
//! No per-service file I/O, no global config struct, no read skew between
//! consumers.

use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_http::config::{
    HttpSnapshot, HttpYaml, ListenerCapabilities, RouterConfig,
};
#[cfg(feature = "metrics")]
use claviron_http::metrics::HttpMetricsConfig;
use claviron_http::watchdog::WatchdogConfig;
use claviron_runtime::AppState;
use claviron_runtime::Register;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable, Format,
};
use claviron_runtime::registry::{Component, Reloadable, priority};
use tracing::{debug, info};

/// Locate `http.yaml` inside the runtime config directory.
pub fn http_config_path(state: &AppState) -> PathBuf {
    state.config_dir().join("http.yaml")
}

pub struct HttpConfigProvider {
    config_path: PathBuf,
    snapshot: ArcSwap<HttpSnapshot>,
    capabilities: ListenerCapabilities,
}

impl HttpConfigProvider {
    pub fn empty(config_path: PathBuf) -> Self {
        Self::with_capabilities(config_path, ListenerCapabilities::ALL)
    }

    pub fn with_capabilities(
        config_path: PathBuf,
        capabilities: ListenerCapabilities,
    ) -> Self {
        Self {
            config_path,
            snapshot: ArcSwap::from_pointee(HttpSnapshot::default()),
            capabilities,
        }
    }

    /// Host-router defaults only; proxy trust is listener-owned.
    pub fn router(&self) -> Arc<RouterConfig> {
        self.snapshot.load().router.clone()
    }

    /// Hold this snapshot while constructing a transport generation, so
    /// request policy and all listener settings come from the same candidate.
    pub fn snapshot(&self) -> Arc<HttpSnapshot> {
        self.snapshot.load_full()
    }

    /// HTTP metrics publication and freshness policy.
    #[cfg(feature = "metrics")]
    pub fn metrics(&self) -> Arc<HttpMetricsConfig> {
        self.snapshot.load().metrics.clone()
    }

    /// HOL watchdog policy. `None` when the yaml has no `watchdog:` block.
    pub fn watchdog(&self) -> Option<Arc<WatchdogConfig>> {
        self.snapshot.load().watchdog.clone()
    }

    /// Validate and atomically publish an in-memory HTTP configuration.
    pub fn replace_from_yaml(
        &self,
        yaml: &str,
    ) -> claviron_runtime::registry::Result<()> {
        let parsed: HttpYaml = serde_yaml::from_str(yaml)?;
        let candidate = parsed.resolve().map_err(|error| {
            claviron_runtime::registry::Error::msg(error.to_string())
        })?;
        self.capabilities.validate(&candidate).map_err(|error| {
            claviron_runtime::registry::Error::msg(error.to_string())
        })?;
        self.snapshot.store(Arc::new(candidate));
        Ok(())
    }
}

#[async_trait]
impl Configurable for HttpConfigProvider {
    type Config = HttpSnapshot;

    fn namespace(&self) -> String {
        "http".to_owned()
    }

    fn source(&self) -> ConfigFile {
        ConfigFile { format: Format::Yaml, path: self.config_path.clone() }
    }

    /// No file, no listeners: the edge still boots and can be given one.
    fn absent_source(&self) -> Option<HttpSnapshot> {
        Some(HttpSnapshot::default())
    }

    async fn configure(
        &self,
        config: &HttpSnapshot,
    ) -> congif::Result<()> {
        self.snapshot.store(Arc::new(config.clone()));
        debug!("📦 HttpService loaded {}", self.config_path.display());
        Ok(())
    }

    fn validate_config(
        &self,
        config: &HttpSnapshot,
    ) -> congif::Result<()> {
        self.capabilities.validate(config).map_err(congif::Error::validation)
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = HttpSnapshot>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for HttpConfigProvider {
    async fn config_changed(
        &self,
        config: &HttpSnapshot,
    ) -> congif::Result<()> {
        self.configure(config).await
    }
}

impl Register<AppState> for HttpConfigProvider {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        register_http_config_with_capabilities(state, ListenerCapabilities::ALL)
    }
}

impl Component<AppState> for HttpConfigProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "HttpService"
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<AppState> for HttpConfigProvider {
    fn priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.snapshot.store(state.config_reader().snapshot::<Self>()?);
        info!("🔁 HttpService reloaded {}", self.config_path.display());
        Ok(())
    }
}

pub fn register_http_config(
    state: &AppState
) -> claviron_runtime::registry::Result<()> {
    <HttpConfigProvider as Register<AppState>>::register(state)
}

/// Register the sole config reader with the transports installed by the host.
pub fn register_http_config_with_capabilities(
    state: &AppState,
    capabilities: ListenerCapabilities,
) -> claviron_runtime::registry::Result<()> {
    state.bind_config_environment::<HttpConfigProvider>()?;
    let config = HttpConfigProvider::with_capabilities(
        http_config_path(state),
        capabilities,
    );
    let config = Arc::new(config);
    state.registry_ref().register(config.clone())?;
    // The watchdog is not a metric: it reads what this process has in flight
    // and says so when that stops moving. A build that cannot be scraped still
    // needs to notice its own wedge.
    let watchdog = Arc::clone(&config);
    claviron_http::watchdog::register(state, move || watchdog.watchdog())?;
    #[cfg(feature = "metrics")]
    claviron_http::metrics::register(state, move || config.metrics())?;
    Ok(())
}
