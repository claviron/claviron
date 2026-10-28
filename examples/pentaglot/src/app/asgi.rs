//! This host's lifecycle adapter for the independent ASGI engine.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use claviron::axum::Router;
use claviron::edge::{Host, HostConfig, Routable};
use claviron::runtime::congif::{self, ConfigFile, Configurable};
use claviron::runtime::{AppState, Bootable, Component, Finalizable, registry};
use gufi_asgi::asgi::{
    PythonApplication, PythonApplicationHandle, RunningPythonApplication,
};
use gufi_asgi::routing::PythonService;
use serde::Deserialize;

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    target: String,
    module_path: PathBuf,
    venv: Option<PathBuf>,
}

pub struct Asgi {
    source: PathBuf,
    handle: Arc<RwLock<Option<PythonApplicationHandle>>>,
    running: Mutex<Option<RunningPythonApplication>>,
}

impl Asgi {
    pub fn new(state: &AppState) -> Self {
        Self {
            source: state.config_dir().join("python.yaml"),
            handle: Arc::new(RwLock::new(None)),
            running: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Configurable for Asgi {
    type Config = Config;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.source)
    }

    async fn configure(
        &self,
        config: &Config,
    ) -> congif::Result<()> {
        PythonApplication::new(config.target.clone())
            .map_err(congif::Error::msg)?;
        if !config.module_path.is_dir() {
            return Err(congif::Error::msg(
                "Python module_path must be an existing directory",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Component<AppState> for Asgi {
    type Deps = ();

    fn name(&self) -> &'static str {
        "Pentaglot ASGI"
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, ()> for Asgi {
    async fn boot(
        &self,
        _: (),
        state: &AppState,
    ) -> registry::Result<()> {
        if state.is_master()
            || state
                .registry_ref()
                .resolve::<claviron::runtime::ConfigCheck>()
                .is_some()
        {
            return Ok(());
        }
        let config = state.config_reader().snapshot::<Self>()?;
        let mut definition = PythonApplication::new(config.target.clone())
            .map_err(|error| registry::Error::msg(error.to_string()))?
            .module_path(&config.module_path);
        if let Some(venv) = &config.venv {
            definition = definition.virtual_environment(venv);
        }
        // Native boot and join may block; neither runs on an async reactor.
        let running = tokio::task::spawn_blocking(move || definition.boot())
            .await
            .map_err(|error| registry::Error::msg(error.to_string()))?
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        *self.handle.write().expect("ASGI handle lock") =
            Some(running.handle());
        *self.running.lock().expect("ASGI owner lock") = Some(running);
        tracing::info!("Pentaglot ASGI lifespan started");
        Ok(())
    }
}

#[async_trait]
impl Finalizable<AppState> for Asgi {
    async fn finalize(
        &self,
        _: &AppState,
    ) -> registry::Result<()> {
        *self.handle.write().expect("ASGI handle lock") = None;
        let running = self.running.lock().expect("ASGI owner lock").take();
        if let Some(running) = running {
            tokio::task::spawn_blocking(move || running.shutdown())
                .await
                .map_err(|error| registry::Error::msg(error.to_string()))?
                .map_err(|error| registry::Error::msg(error.to_string()))?;
            tracing::info!("Pentaglot ASGI lifespan stopped");
        }
        Ok(())
    }
}

impl Routable for Asgi {
    fn match_host(
        &self,
        _key: &Host,
        _host: &HostConfig,
    ) -> bool {
        true
    }

    fn routes(
        self: Arc<Self>,
        _state: &AppState,
        _key: &Host,
        _host: Arc<HostConfig>,
    ) -> registry::Result<Option<Router<AppState>>> {
        // Keep the admitted URL intact, just like the other mounted applications.
        let service = PythonService::new(Arc::clone(&self.handle));
        Ok(Some(
            Router::new()
                .route_service("/python", service.clone())
                .route_service("/python/{*path}", service),
        ))
    }
}
