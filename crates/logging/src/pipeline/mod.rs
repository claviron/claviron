//! The log pipeline: declared channels, an Orbit ring as the buffer, one
//! writer per fleet. See this crate's ARCHITECTURE.md.

mod channels;
mod config;
#[cfg(feature = "otlp")]
mod otlp;
mod record;
mod ring;
mod writer;

use std::sync::Arc;

pub use channels::{EmitError, LogChannels};
use claviron_runtime::AppState;
use claviron_runtime::registry::{self};
pub use config::{ChannelConfig, ConfigError, FileFormat, LoggingConfig};
use orbitive::Fleet;
pub use record::{ChannelId, Level, Record, Value};

/// This process's channels, once registered: for producers that are not
/// handed the registry, such as a task's output.
static INSTALLED: std::sync::OnceLock<Arc<LogChannels>> =
    std::sync::OnceLock::new();

/// The channels this process registered, once the pipeline is registered.
pub fn installed() -> Option<Arc<LogChannels>> {
    INSTALLED.get().cloned()
}

/// What the pipeline's configuration file is called in the config directory.
pub const CONFIG_FILE: &str = "logging.yaml";

/// Register the pipeline: the ring, [`LogChannels`] and the writer in every
/// process; the writer declares through `Scoped` that it runs in the master
/// (or in a standalone process). `logging.yaml` is [`LogChannels`]' Congif
/// source, prepared before boot and in the reload batch; without the file
/// there are no channels until a reload or a source change declares some, and
/// the writer follows the table.
pub fn register(state: &AppState) -> registry::Result<()> {
    if state.registry_ref().resolve::<LogChannels>().is_some() {
        return Ok(());
    }
    register_channels(
        state,
        LogChannels::new(state.config_dir().join(CONFIG_FILE)),
    )
}

/// Compose already parsed channel policy without an additional config file.
pub fn register_with_config(
    state: &AppState,
    config: LoggingConfig,
) -> registry::Result<()> {
    let channels = LogChannels::new(std::path::PathBuf::new());
    config.validate(std::path::Path::new("logging")).map_err(|error| {
        claviron_runtime::registry::Error::msg(error.to_string())
    })?;
    channels.apply(&config).map_err(|error| {
        claviron_runtime::registry::Error::msg(error.to_string())
    })?;
    register_channels(state, channels)
}

fn register_channels(
    state: &AppState,
    channels: LogChannels,
) -> registry::Result<()> {
    claviron_orbit::shm::register_reusable_ring::<ring::LogFrame>(
        state,
        "LogRingShm",
        "log-ring",
    )?;
    state.bind_config_environment::<LogChannels>()?;
    let channels = Arc::new(channels);
    let _ = INSTALLED.set(Arc::clone(&channels));
    state.registry_ref().register(Arc::clone(&channels))?;
    state
        .registry_ref()
        .register(Arc::new(writer::LogWriter::new(channels)))?;
    Ok(())
}

/// The fleet's shared-memory handle. A process-local fleet has no lane
/// another process can read and no doorbell to wake a writer, so the
/// pipeline refuses it rather than write where nothing reads.
fn shared_fleet(
    provider: &claviron_orbit::FleetProvider
) -> registry::Result<Arc<Fleet>> {
    let fleet = provider.handle().ok_or_else(|| {
        registry::Error::msg("the log pipeline needs the Orbit fleet")
    })?;
    if !fleet.is_shm() {
        return Err(registry::Error::msg(
            "the log pipeline needs a shared-memory Orbit fleet",
        ));
    }
    Ok(fleet)
}
