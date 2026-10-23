//! Benchmark transport hosts are declared only in benchmark builds.

use std::path::PathBuf;
use std::sync::Arc;

use claviron::edge::{self, VhostDocument};
use claviron::runtime::AppState;
use claviron::runtime::congif::{self, ConfigFile, Configurable};
use claviron::runtime::registry::{self, Component};
use serde::Deserialize;

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Config {
    hosting: VhostDocument,
}

struct BenchHosts {
    path: PathBuf,
}

#[congif::async_trait]
impl Configurable for BenchHosts {
    type Config = Option<Config>;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.path)
    }

    fn absent_source(&self) -> Option<Self::Config> {
        Some(None)
    }

    async fn configure(
        &self,
        _config: &Self::Config,
    ) -> congif::Result<()> {
        Ok(())
    }
}

impl Component<AppState> for BenchHosts {
    type Deps = ();

    fn name(&self) -> &'static str {
        "BenchHosts"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
}

pub(super) fn register(state: &AppState) -> registry::Result<()> {
    state.bind_config_environment::<BenchHosts>()?;
    state.registry_ref().register(Arc::new(BenchHosts {
        path: state.config_dir().join("bench.yaml"),
    }))?;
    edge::register_hosts(state, |state| {
        let config = state
            .config_reader()
            .snapshot::<BenchHosts>()
            .map_err(edge::Error::other)?;
        match config.as_ref() {
            Some(config) => config.hosting.declarations(),
            None => Ok(Vec::new()),
        }
    })
}
