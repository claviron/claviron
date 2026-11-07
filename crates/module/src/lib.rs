//! Native modules: compiled libraries loaded into the process, which say for
//! themselves what they are. `include/claviron_module.h` is the contract.
//! See `ARCHITECTURE.md`.

#![cfg(unix)]

pub(crate) mod abi;
pub mod config;
mod provider;
mod registrar;
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
