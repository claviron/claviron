//! The producer's side: declared channels by name, and `emit`.
//!
//! Ids are stable for the life of the process: a channel keeps the id it was
//! first given, one declared later takes the next, and one no longer declared
//! leaves its id unused. A producer that resolved a name once never writes
//! into another channel after the file changed, and the writer learns of the
//! change through the same table.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{self, Bootable, Reloadable};
use claviron_runtime::{AppState, Component};
use orbitive::Fleet;
use tokio::sync::Notify;

use super::config::{ChannelConfig, ConfigError, LoggingConfig};
use super::record::{self, ChannelId, Record};
use super::ring::{
    FRAME_KIND_PART, FRAME_KIND_RECORD, LogFrame, MAX_FRAMES, PART_HEADER,
    PAYLOAD_CAPACITY,
};

/// Every process's handle on the pipeline, and the owner of `logging.yaml`
/// for it. Resolve a channel once, then [`emit`](Self::emit) to it: a copy
/// into this process's lane and a doorbell, nothing that waits.
pub struct LogChannels {
    config_path: PathBuf,
    table: ArcSwap<ChannelTable>,
    /// A permit left for the writer when the table changed, so it reopens
    /// its sinks from the new one.
    changed: Notify,
    fleet: OnceLock<Arc<Fleet>>,
}

/// The declared channels, by id and by name.
#[derive(Default)]
pub(crate) struct ChannelTable {
    ids: HashMap<String, ChannelId>,
    access: Option<ChannelId>,
    /// By id. A channel no longer declared is `None`; its id stays taken.
    pub(crate) channels: Vec<Option<(String, ChannelConfig)>>,
}

impl ChannelTable {
    /// This table after `config`: every id already given is kept.
    fn with(
        &self,
        config: &LoggingConfig,
        path: &Path,
    ) -> Result<Self, ConfigError> {
        let mut channels: Vec<Option<(String, ChannelConfig)>> =
            self.channels.iter().map(|_| None).collect();
        let mut ids = HashMap::with_capacity(config.channels.len());
        for (name, channel) in &config.channels {
            let id = match self.ids.get(name) {
                Some(id) => *id,
                None => {
                    let next = channels.len();
                    if next > usize::from(u16::MAX) {
                        return Err(ConfigError::TooManyChannels {
                            path: path.to_owned(),
                            count: next + 1,
                        });
                    }
                    channels.push(None);
                    ChannelId(next as u16)
                }
            };
            channels[usize::from(id.0)] = Some((name.clone(), channel.clone()));
            ids.insert(name.clone(), id);
        }
        let access =
            config.access_log.as_ref().and_then(|name| ids.get(name).copied());
        Ok(Self { ids, access, channels })
    }
}

impl std::fmt::Debug for LogChannels {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("LogChannels")
            .field("channels", &self.table.load().ids.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("the log pipeline has not booted")]
    NotBooted,
    #[error("no declared log channel has id {0}")]
    UnknownChannel(u16),
    #[error("record cannot fit the log frame budget")]
    RecordCapacity,
    #[error("log ring write failed: {0}")]
    Ring(#[from] std::io::Error),
}

impl LogChannels {
    /// No channels yet; `configure` declares them.
    pub(crate) fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            table: ArcSwap::from_pointee(ChannelTable::default()),
            changed: Notify::new(),
            fleet: OnceLock::new(),
        }
    }

    /// The current table, for the writer.
    pub(crate) fn table(&self) -> Arc<ChannelTable> {
        self.table.load_full()
    }

    /// Resolves once the table has changed since the last time it did.
    pub(crate) async fn changed(&self) {
        self.changed.notified().await;
    }

    pub(super) fn apply(
        &self,
        config: &LoggingConfig,
    ) -> Result<(), ConfigError> {
        let table = self.table.load().with(config, &self.config_path)?;
        self.table.store(Arc::new(table));
        self.changed.notify_one();
        Ok(())
    }

    /// The channel `access_log` names, if it names one.
    pub fn access(&self) -> Option<ChannelId> {
        self.table.load().access
    }

    /// The declared channel of that name. An undeclared one is `None`:
    /// nothing routes a record somewhere the operator did not say.
    pub fn channel(
        &self,
        name: &str,
    ) -> Option<ChannelId> {
        self.table.load().ids.get(name).copied()
    }

    pub fn emit(
        &self,
        channel: ChannelId,
        record: &Record<'_>,
    ) -> Result<(), EmitError> {
        if self
            .table
            .load()
            .channels
            .get(usize::from(channel.0))
            .is_none_or(Option::is_none)
        {
            return Err(EmitError::UnknownChannel(channel.0));
        }
        let fleet = self.fleet.get().ok_or(EmitError::NotBooted)?;
        let part = PAYLOAD_CAPACITY - PART_HEADER;
        let encoded = record::encode(
            channel,
            record,
            SystemTime::now(),
            MAX_FRAMES * part,
        )
        .ok_or(EmitError::RecordCapacity)?;
        if encoded.len() > MAX_FRAMES * part {
            return Err(EmitError::RecordCapacity);
        }
        if encoded.len() <= PAYLOAD_CAPACITY {
            fleet.publish_notified_parked::<LogFrame>(
                FRAME_KIND_RECORD,
                0,
                Bytes::from(encoded),
            )?;
            return Ok(());
        }
        let count = encoded.len().div_ceil(part);
        if count > MAX_FRAMES {
            return Err(EmitError::RecordCapacity);
        }
        let count =
            u8::try_from(count).map_err(|_| EmitError::RecordCapacity)?;
        let parts = encoded
            .chunks(part)
            .enumerate()
            .map(|(index, bytes)| {
                let mut frame = Vec::with_capacity(PART_HEADER + bytes.len());
                frame.push(index as u8);
                frame.push(count);
                frame.extend_from_slice(bytes);
                Bytes::from(frame)
            })
            .collect();
        fleet.publish_batch_notified_parked::<LogFrame>(
            FRAME_KIND_PART,
            0,
            parts,
        )?;
        Ok(())
    }
}

impl Component<AppState> for LogChannels {
    type Deps = (Arc<claviron_orbit::FleetProvider>,);

    fn name(&self) -> &'static str {
        "LogChannels"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        (!self.config_path.as_os_str().is_empty()).then_some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        (!self.config_path.as_os_str().is_empty()).then_some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<claviron_orbit::FleetProvider>,)> for LogChannels {
    async fn boot(
        &self,
        (fleet,): (Arc<claviron_orbit::FleetProvider>,),
        _state: &AppState,
    ) -> registry::Result<()> {
        let fleet = super::shared_fleet(&fleet)?;
        let _ = self.fleet.set(fleet);
        Ok(())
    }
}

#[async_trait]
impl Configurable for LogChannels {
    type Config = LoggingConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// No file, no channels — until a reload or a source change declares some.
    fn absent_source(&self) -> Option<LoggingConfig> {
        Some(LoggingConfig::default())
    }

    fn validate_config(
        &self,
        config: &LoggingConfig,
    ) -> congif::Result<()> {
        config.validate(&self.config_path).map_err(congif::Error::validation)
    }

    async fn configure(
        &self,
        config: &LoggingConfig,
    ) -> congif::Result<()> {
        self.apply(config).map_err(congif::Error::validation)
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for LogChannels {
    async fn config_changed(
        &self,
        config: &LoggingConfig,
    ) -> congif::Result<()> {
        self.apply(config).map_err(congif::Error::validation)
    }
}

#[async_trait]
impl Reloadable<AppState> for LogChannels {
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.apply(&config)
            .map_err(|error| registry::Error::msg(error.to_string()))
    }
}
