//! Static-route caching and compression rules.
//!
//! Declared in `claviron-http` rather than beside `HostConfig` so the files
//! service can read them without the host's crate. `claviron-files` keeps
//! the runtime side (`CachePolicy`, `compose_static_policy`,
//! `apply_client_cache_headers`).

use claviron_helpers::de::deserialize_duration_secs;
use regex::Regex;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct RulesConfig {
    #[serde(with = "serde_regex")]
    pub test: Regex,
    pub caching: Option<CachingConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CachingConfig {
    /// Client-side TTL (HTTP caching). Aliases: `expires`, `expire`, `client_ttl`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_duration_secs",
        alias = "expires",
        alias = "expire",
        alias = "client_ttl"
    )]
    pub client: Option<u64>,

    /// Server-side in-memory TTL. Aliases: `remember`, `memory`, `mem_cache`, `server_ttl`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_duration_secs",
        alias = "remember",
        alias = "memory",
        alias = "mem_cache",
        alias = "server_ttl"
    )]
    pub server: Option<u64>,

    /// Validate file mtime on cache hit.
    #[serde(default = "default_true")]
    pub validate: bool,

    /// Add `immutable` directive to Cache-Control (for hash-based assets).
    #[serde(default)]
    pub immutable: bool,

    /// Optional response compression config.
    #[serde(default)]
    pub compression: Option<CompressionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompressionConfig {
    #[serde(default)]
    #[allow(dead_code)]
    pub algorithm: CompressionAlgorithm,

    #[serde(default = "default_compression_level")]
    pub level: u8,

    #[serde(default = "default_min_size")]
    pub min_size: byte_unit::Byte,
}

#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionAlgorithm {
    #[default]
    Brotli,
    Zstd,
}

fn default_compression_level() -> u8 {
    5
}

fn default_min_size() -> byte_unit::Byte {
    byte_unit::Byte::from_u64(1024)
}

#[inline]
fn default_true() -> bool {
    true
}
