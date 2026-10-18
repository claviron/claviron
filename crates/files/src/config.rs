use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use serde::Deserialize;

/// Global policy for the static file service.
///
/// Arena geometry is deliberately absent: byte and slot counts are part of
/// the fleet's compiled SHM layout, so every process must agree on them.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FilesConfig {
    pub arena: ArenaConfig,
    pub memory: MemoryCacheConfig,
    pub disk: DiskCacheConfig,
    pub gc: GcConfig,
}

impl FilesConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.memory.max_body_size == 0
            || self.memory.max_body_size
                > crate::caching::MAX_CACHE_BODY_BYTES_LIMIT
        {
            return Err(format!(
                "files config memory.max_body_size must be between 1 and {} bytes",
                crate::caching::MAX_CACHE_BODY_BYTES_LIMIT
            ));
        }
        if self.memory.max_bytes == 0 {
            return Err(
                "files config memory.max_bytes must be greater than zero"
                    .into(),
            );
        }
        if self.disk.directory.as_os_str().is_empty() {
            return Err("files config disk.directory must not be empty".into());
        }
        if self.disk.retention.is_zero() {
            return Err(
                "files config disk.retention must be greater than zero".into(),
            );
        }
        if self.gc.max_remove == 0 {
            return Err(
                "files config gc.max_remove must be greater than zero".into()
            );
        }
        Ok(())
    }
}

static ACTIVE: Lazy<ArcSwap<FilesConfig>> =
    Lazy::new(|| ArcSwap::from_pointee(FilesConfig::default()));

pub(crate) fn install(config: FilesConfig) {
    ACTIVE.store(Arc::new(config));
}

pub(crate) fn active() -> Arc<FilesConfig> {
    ACTIVE.load_full()
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ArenaConfig {
    pub boot: ArenaBoot,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryCacheConfig {
    /// Largest body retained by the fleet or process-memory tier.
    #[serde(default = "default_max_body_size", deserialize_with = "byte_size")]
    pub max_body_size: u64,
    /// Per-process byte budget for the fallback process-memory tier.
    #[serde(default = "default_max_bytes", deserialize_with = "byte_size")]
    pub max_bytes: u64,
}

impl Default for MemoryCacheConfig {
    fn default() -> Self {
        Self {
            max_body_size: default_max_body_size(),
            max_bytes: default_max_bytes(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct DiskCacheConfig {
    /// Cwd-relative root for encoded large-file representations.
    pub directory: PathBuf,
    /// How long unchanged disk-cache metadata remains eligible for reuse.
    #[serde(default = "default_disk_retention", deserialize_with = "duration")]
    pub retention: Duration,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("storage/cache/http"),
            retention: default_disk_retention(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct GcConfig {
    /// Maximum entries removed by one scheduled cache_gc occurrence.
    pub max_remove: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self { max_remove: 128 }
    }
}

/// What the file service does with a compatible arena found at boot.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ArenaBoot {
    /// Keep published immutable records across master/runtime replacement.
    #[default]
    Preserve,
    /// Empty only after the SHM lifecycle proves that no user is active.
    Reset,
}

fn default_max_body_size() -> u64 {
    1024 * 1024
}

fn default_max_bytes() -> u64 {
    256 * 1024 * 1024
}

fn default_disk_retention() -> Duration {
    Duration::from_secs(365 * 24 * 60 * 60)
}

fn byte_size<'de, D: serde::Deserializer<'de>>(
    deserializer: D
) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Size {
        Bytes(u64),
        Human(String),
    }

    match Size::deserialize(deserializer)? {
        Size::Bytes(bytes) => Ok(bytes),
        Size::Human(value) => claviron_helpers::de::deserialize_byte_size(
            serde::de::value::StrDeserializer::<D::Error>::new(&value),
        ),
    }
}

fn duration<'de, D: serde::Deserializer<'de>>(
    deserializer: D
) -> Result<Duration, D::Error> {
    claviron_helpers::de::deserialize_duration(deserializer)
}
