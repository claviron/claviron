use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use bytes::Bytes;
use claviron_http::config::caching::CompressionAlgorithm;
use dashmap::DashMap;
use once_cell::sync::Lazy;

#[cfg(feature = "orbit")]
pub(crate) mod arena;
pub(crate) mod condition;
#[cfg(feature = "tasks")]
pub(crate) mod gc;
pub(crate) mod metrics;
pub mod policy;
pub mod reload;
pub(crate) mod service;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ContentEncoding {
    Brotli,
    Zstd,
}

impl ContentEncoding {
    #[inline]
    pub(crate) fn from_algorithm(algorithm: CompressionAlgorithm) -> Self {
        match algorithm {
            CompressionAlgorithm::Brotli => Self::Brotli,
            CompressionAlgorithm::Zstd => Self::Zstd,
        }
    }

    /// The arena stores this beside an encoded body; it is the only place
    /// the encoding leaves the process as a number.
    #[cfg(feature = "orbit")]
    #[inline]
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Brotli => 1,
            Self::Zstd => 2,
        }
    }

    #[cfg(feature = "orbit")]
    #[inline]
    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Brotli),
            2 => Some(Self::Zstd),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn token(self) -> &'static str {
        match self {
            Self::Brotli => "br",
            Self::Zstd => "zstd",
        }
    }

    #[inline]
    pub(crate) fn extension(self) -> &'static str {
        match self {
            Self::Brotli => "br",
            Self::Zstd => "zst",
        }
    }

    #[inline]
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Brotli => "Brotli",
            Self::Zstd => "Zstd",
        }
    }
}

// RAM cache for small files (<1MB), file cache for larger (≥1MB)
// Large files leverage OS page cache for speed
/// Bodies below this are cached whole; at or above it they take the large
/// tier or stream from disk. Compile-time, like the arena geometry it has to
/// fit: `CLAVIRON_FILES_MAX_CACHE_BODY_BYTES` in the application's
/// `.cargo/config.toml`, default 1 MiB.
pub(crate) const MAX_CACHE_BODY_BYTES_LIMIT: u64 = bytes_from_env(
    option_env!("CLAVIRON_FILES_MAX_CACHE_BODY_BYTES"),
    1024 * 1024,
);

/// A record carries the identity body and its encoded twin, so the arena
/// must take twice the threshold, or the fleet tier would quietly refuse what
/// the threshold admits. A build that disagrees with itself does not build.
#[cfg(feature = "orbit")]
const _: () = assert!(
    2 * (MAX_CACHE_BODY_BYTES_LIMIT as usize)
        <= claviron_orbit::arena::ARENA_RECORD_MAX,
    "CLAVIRON_FILES_MAX_CACHE_BODY_BYTES must be at most an eighth of ORBIT_ARENA_BYTES"
);

/// Decimal, `_` allowed as a separator; anything else stops the build.
const fn bytes_from_env(
    value: Option<&str>,
    default: u64,
) -> u64 {
    let Some(value) = value else {
        return default;
    };
    let bytes = value.as_bytes();
    assert!(
        !bytes.is_empty(),
        "CLAVIRON_FILES_MAX_CACHE_BODY_BYTES cannot be empty"
    );
    let mut parsed = 0_u64;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte != b'_' {
            assert!(
                byte >= b'0' && byte <= b'9',
                "CLAVIRON_FILES_MAX_CACHE_BODY_BYTES must be a decimal byte count"
            );
            parsed = match parsed.checked_mul(10) {
                Some(shifted) => {
                    match shifted.checked_add((byte - b'0') as u64) {
                        Some(next) => next,
                        None => panic!(
                            "CLAVIRON_FILES_MAX_CACHE_BODY_BYTES overflows"
                        ),
                    }
                }
                None => panic!("CLAVIRON_FILES_MAX_CACHE_BODY_BYTES overflows"),
            };
        }
        index += 1;
    }
    assert!(parsed > 0, "CLAVIRON_FILES_MAX_CACHE_BODY_BYTES must be positive");
    parsed
}

// Functional RAM cache accounting. Metrics may mirror this value, but cache
// admission must not depend on the metrics subsystem being enabled.
static TOTAL_CACHE_BYTES: AtomicU64 = AtomicU64::new(0);

// Tier 1: RAM cache (<1MB)
static MEMORY_CACHE: Lazy<DashMap<PathBuf, Arc<CachedEntry>>> =
    Lazy::new(DashMap::new);

// Tier 2: File cache (≥1MB) - OS page cache
//
// TODO(shared cache): A future shared-cache integration can replace this
// DashMap + manual `storage/cache/http/{aa}/{bb}/{hash}.{br,zst}` write/read with
// a claviron-cache `CacheStore` lookup (file driver, cacache backend).
// Key = absolute file path (`PathBuf::display()` or `to_string_lossy()`);
// cacache SHA-256s the key internally, so any string is fine. Once
// migrated:
//   * `serve_file_cache_sync` and its blocking `std::fs::read` go away
//     (CacheStore::get is async by construction → tokio-friendly).
//   * `compute_file_hash` / `file_cache_path` / `encoded_hash` field become
//     obsolete (cacache handles content addressing).
//   * GC sweep in `gc.rs` for orphan encoded files moves to cacache's own
//     verify/integrity tooling.
// Memory tier (<1 MiB) stays in-process — latency-sensitive, cacache
// trip is overkill for small bodies.
static FILE_CACHE: Lazy<DashMap<PathBuf, Arc<FileCachedEntry>>> =
    Lazy::new(DashMap::new);

/// RAM cache entry (<1MB)
pub(crate) struct CachedEntry {
    mtime: SystemTime,
    len: u64,
    body: Arc<Bytes>,
    content_type: String,
    last_modified: String,
    expires_at: Instant,
    etag: String,
    encoded_body: Option<Arc<Bytes>>,
    encoding: Option<ContentEncoding>,
    #[allow(dead_code)]
    // Reserved for future metrics (compression ratio tracking)
    encoded_len: u64,
}

/// File cache entry (≥1MB)
/// Metadata in RAM, compressed body on disk (OS page cache)
pub(crate) struct FileCachedEntry {
    pub(crate) mtime: SystemTime,
    #[allow(dead_code)] // Reserved for future metrics (file size tracking)
    len: u64, // Original file size
    content_type: String,
    last_modified: String,
    pub(crate) expires_at: Instant,
    etag: String,
    pub(crate) encoded_file: PathBuf, // storage/cache/http/{aa}/{bb}/{hash}.{br,zst}
    encoding: ContentEncoding,
    #[allow(dead_code)]
    // Reserved for future GC (orphan encoded file cleanup by hash)
    encoded_hash: blake3::Hash, // BLAKE3 hash of original content
    #[allow(dead_code)]
    // Reserved for future metrics (compression ratio tracking)
    encoded_len: u64, // Compressed file size
}

#[inline]
fn unreserve_cache_bytes(len: u64) {
    TOTAL_CACHE_BYTES.fetch_sub(len, Ordering::Relaxed);
    metrics::sync_memory_cache();
}

/// Generate file cache path from content hash
#[inline]
pub(crate) fn file_cache_path(
    hash: &blake3::Hash,
    encoding: ContentEncoding,
    directory: &std::path::Path,
) -> PathBuf {
    let hex = hash.to_hex();
    let hex = hex.as_str();
    directory.join(format!(
        "{}/{}/{}.{}",
        &hex[0..2],
        &hex[2..4],
        hex,
        encoding.extension()
    ))
}

/// Compute BLAKE3 hash of file content (for cache key and GC)
#[inline]
pub(crate) fn compute_file_hash(data: &[u8]) -> blake3::Hash {
    blake3::hash(data)
}

#[cfg(feature = "tasks")]
/// Return (entry_count, total_bytes) for the in-memory static file cache.
#[inline]
pub fn cache_stats() -> (usize, u64) {
    let count = MEMORY_CACHE.len();
    let bytes = TOTAL_CACHE_BYTES.load(Ordering::Relaxed);
    (count, bytes)
}
