//! Native modules: compiled libraries loaded into the process, which say for
//! themselves what they are. `include/claviron_module.h` is the contract.
//! See `ARCHITECTURE.md`.

#![cfg(unix)]

pub(crate) mod abi;
mod application;
pub mod config;
mod pool;
mod provider;
mod registrar;
mod request;
mod service;

use std::sync::Arc;

use claviron_runtime::{AppState, registry};

pub use config::{CONFIG_FILE, ModulesConfig};
pub use provider::ModuleProvider;

/// Bring up the libraries `modules.yaml` lists. Without that file, nothing here
/// is installed and the feature costs nothing.
pub fn register(state: &AppState) -> registry::Result<()> {
    if !state.config_dir().join(CONFIG_FILE).exists() {
        return Ok(());
    }
    state.registry_ref().register(Arc::new(ModuleProvider::new(state)))?;
    Ok(())
}

/// Let this site registry prepare `module:` routes: a site, a host's or a
/// runner's, selects an application a library registered by its name.
pub fn register_routes(
    state: &AppState,
    site: &claviron_pipeline::SiteRegistry,
) -> registry::Result<()> {
    let Some(provider) = state.registry_ref().resolve::<ModuleProvider>()
    else {
        return Ok(());
    };
    site.register_backend(claviron_pipeline::factory::factory::<String, _>(
        "module",
        claviron_pipeline::factory::BackendPlacement::AfterFiles,
        move |_, name| provider.backend(&name).map(|backend| vec![backend]),
    ))
}
