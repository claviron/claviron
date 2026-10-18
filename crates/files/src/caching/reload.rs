//! Reload empties the static file cache.
//!
//! A hit is deliberately blind: it does not stat the file, because not
//! touching the disk on the hot path is the point of the cache, and
//! `validate: false` says so per rule. What the cache cannot see on its own
//! it is told about here. A reload (`SIGHUP`) drops both tiers, so the first
//! request for each file after it reads the disk again and the operator has
//! one gesture for "the files changed", the same one that reloads the hosts.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use claviron_runtime::congif::{self, ConfigFile, Configurable};
use claviron_runtime::registry::{
    Bootable, Component, Reloadable, Result, priority,
};
use claviron_runtime::{AppState, Register};
use tracing::{debug, info};

use super::{FILE_CACHE, MEMORY_CACHE, unreserve_cache_bytes};
#[cfg(feature = "orbit")]
use crate::config::ArenaBoot;
use crate::config::FilesConfig;

/// Drops every cached representation and returns how many each tier held.
///
/// Two phases, like the collector: the keys are gathered first and each
/// entry is removed on its own afterwards. Releasing an entry's bytes reports
/// the map's size to metrics, which reads every shard, so it must not run
/// while a shard is held — `retain` did exactly that and locked the worker's
/// reload against itself.
pub(crate) fn invalidate_all() -> (usize, usize) {
    let mut memory = 0;
    let paths: Vec<PathBuf> =
        MEMORY_CACHE.iter().map(|entry| entry.key().clone()).collect();
    for path in paths {
        if let Some((_, old)) = MEMORY_CACHE.remove(&path) {
            unreserve_cache_bytes(old.len);
            memory += 1;
        }
    }

    let mut files = 0;
    let paths: Vec<PathBuf> =
        FILE_CACHE.iter().map(|entry| entry.key().clone()).collect();
    for path in paths {
        if let Some((_, old)) = FILE_CACHE.remove(&path) {
            if old.encoded_file.exists()
                && let Err(error) = std::fs::remove_file(&old.encoded_file)
            {
                debug!(
                    "failed to delete {}: {error}",
                    old.encoded_file.display()
                );
            }
            files += 1;
        }
    }

    (memory, files)
}

/// The owner of `files.yaml`, registered so Congif prepares the policy before
/// boot and the runtime's reload reaches the cache. Its boot picks up the
/// fleet arena when the runtime has one; the per-process tiers need none.
pub struct FileCacheProvider {
    config_path: PathBuf,
    /// The arena this owner registered, so the configured boot policy reaches
    /// it before the arena boots.
    #[cfg(feature = "orbit")]
    arena: Option<Arc<claviron_orbit::ArenaProvider>>,
}

#[cfg(feature = "orbit")]
fn arena_boot_policy(boot: ArenaBoot) -> claviron_orbit::ArenaBootPolicy {
    match boot {
        ArenaBoot::Preserve => claviron_orbit::ArenaBootPolicy::Preserve,
        ArenaBoot::Reset => claviron_orbit::ArenaBootPolicy::ResetWhenQuiescent,
    }
}

/// An absent `files.yaml` leaves the documented defaults standing. Policy is
/// applied in `configure`; a manual reload re-applies the prepared snapshot
/// and empties the cache, so there is no source watch — a save would otherwise
/// empty the cache by itself.
#[async_trait]
impl Configurable for FileCacheProvider {
    type Config = FilesConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    fn absent_source(&self) -> Option<FilesConfig> {
        Some(FilesConfig::default())
    }

    fn validate_config(
        &self,
        config: &FilesConfig,
    ) -> congif::Result<()> {
        config.validate().map_err(congif::Error::invalid)
    }

    async fn configure(
        &self,
        config: &FilesConfig,
    ) -> congif::Result<()> {
        crate::config::install(config.clone());
        #[cfg(feature = "orbit")]
        if let Some(arena) = &self.arena {
            arena
                .resources()
                .set_boot_policy(arena_boot_policy(config.arena.boot));
        }
        debug!(boot = ?config.arena.boot, path = %self.config_path.display(), "static file policy applied");
        Ok(())
    }
}

/// The fleet arena small bodies are served from, which `register` installs
/// beside this provider. Nothing without `orbit`.
#[cfg(feature = "orbit")]
type ArenaDeps = (Arc<claviron_orbit::ArenaProvider>,);
#[cfg(not(feature = "orbit"))]
type ArenaDeps = ();

impl Component<AppState> for FileCacheProvider {
    type Deps = ArenaDeps;

    fn name(&self) -> &'static str {
        "FileCache"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, ArenaDeps> for FileCacheProvider {
    async fn boot(
        &self,
        _arena: ArenaDeps,
        _state: &AppState,
    ) -> Result<()> {
        #[cfg(feature = "orbit")]
        if let Some(arena) = _arena.0.handle() {
            super::arena::install(arena);
            tracing::debug!(
                "🗂️  static file cache serves small bodies from the fleet arena"
            );
            if !_state.is_worker() {
                super::arena::report_budget(_state.worker_count());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for FileCacheProvider {
    /// After the hosts and their policies; nothing here depends on it, but
    /// a request that lands mid-reload should meet the new policy and an
    /// empty cache together.
    fn priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        crate::config::install(config.as_ref().clone());
        let (memory, files) = invalidate_all();
        #[cfg(feature = "orbit")]
        let fleet = super::arena::reset();
        #[cfg(not(feature = "orbit"))]
        let fleet = 0;
        info!(
            "🗂️  static file cache emptied on reload: {fleet} in the fleet arena, {memory} in memory, {files} on disk"
        );
        Ok(())
    }
}

pub fn register(state: &AppState) -> Result<()> {
    <FileCacheProvider as Register<AppState>>::register(state)
}

impl Register<AppState> for FileCacheProvider {
    fn register(state: &AppState) -> Result<()> {
        if state.registry_ref().resolve::<FileCacheProvider>().is_some() {
            return Ok(());
        }
        state.bind_config_environment::<Self>()?;
        // The arena is registered with the default policy; `configure`
        // declares the operator's before anything boots.
        #[cfg(feature = "orbit")]
        claviron_orbit::register_arena_with_policy(
            state,
            arena_boot_policy(FilesConfig::default().arena.boot),
        )?;
        state.registry_ref().register(Arc::new(FileCacheProvider {
            config_path: state.config_dir().join("files.yaml"),
            #[cfg(feature = "orbit")]
            arena: state
                .registry_ref()
                .resolve::<claviron_orbit::ArenaProvider>(),
        }))?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/caching/reload.rs"]
mod tests;
