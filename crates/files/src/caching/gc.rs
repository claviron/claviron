use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Instant;

use byte_unit::Byte;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};

/// Reasons for removal in GC Phase A. Phase C uses `trim` (over budget) which is
/// logged directly without this enum for now.
#[derive(Copy, Clone, Debug)]
enum Reason {
    Orphan,      // file missing on disk
    Expired,     // TTL expired
    Invalidated, // original file changed (mtime mismatch)
}

use claviron_runtime::AppState;
use claviron_tasks::{TaskResult, task};

use super::{
    FILE_CACHE, MEMORY_CACHE, TOTAL_CACHE_BYTES, cache_stats,
    unreserve_cache_bytes,
};

#[task(
    name = "cache_gc",
    scope = "workers",
    config = "timing: { kind: interval, every: 1h, skip_on_boot: true }\nattempts: 1\nsilent: true\n"
)]
pub(crate) async fn cache_gc_task(
    _state: AppState,
    _cancel: CancellationToken,
) -> TaskResult<()> {
    trace!("Executing cache GC task...");

    let config = crate::config::active();
    let max_remove = config.gc.max_remove;
    let (before_count, before_bytes) = cache_stats();
    sweep_ram_cache(max_remove, config.memory.max_bytes);
    sweep_file_cache(max_remove);
    let (after_count, after_bytes) = cache_stats();
    let removed = before_count.saturating_sub(after_count);

    if removed > 0 {
        let before = format!(
            "{:.0}",
            Byte::from_u64(before_bytes)
                .get_appropriate_unit(byte_unit::UnitType::Binary)
        );
        let after = format!(
            "{:.0}",
            Byte::from_u64(after_bytes)
                .get_appropriate_unit(byte_unit::UnitType::Binary)
        );
        info!(
            "♻️  cache_gc: removed {} entries; bytes {} → {}",
            removed, before, after,
        );
    }

    Ok(())
}

/// Remove expired entries and trim if we're over budget.
#[inline]
pub fn sweep_ram_cache(
    max_remove: usize,
    max_bytes: u64,
) {
    let now = Instant::now();
    let mut removed: usize = 0;

    // Local snapshot of total to minimize atomic loads
    let mut total = TOTAL_CACHE_BYTES.load(Ordering::Relaxed);

    // Phase A: decide what to remove (no mutations while holding iter guards)
    let mut to_remove: Vec<(PathBuf, Reason)> = Vec::new();
    let mut trim_candidates: Vec<(Instant, PathBuf)> = Vec::new();

    for item in MEMORY_CACHE.iter() {
        if removed + to_remove.len() >= max_remove {
            break;
        }
        let k = item.key().clone();
        let v = item.value();

        // Orphan (file missing)
        if !k.exists() {
            to_remove.push((k, Reason::Orphan));
            continue;
        }
        // Expired
        if v.expires_at <= now {
            to_remove.push((k, Reason::Expired));
            continue;
        }
        // Over budget? mark as candidate (sorted later)
        if total > max_bytes {
            trim_candidates.push((v.expires_at, k));
        }
    }

    // Phase B: remove orphans/expired
    for (k, why) in to_remove.drain(..) {
        if removed >= max_remove {
            break;
        }
        if let Some((_k, old)) = MEMORY_CACHE.remove(&k) {
            let reason = match why {
                Reason::Orphan => "orphan",
                Reason::Expired => "expired",
                Reason::Invalidated => "invalidated",
            };
            debug!(
                "🔥 cache GC rm: {} ({} bytes) [reason: {}]",
                k.display(),
                old.len,
                reason
            );
            unreserve_cache_bytes(old.len);
            total = total.saturating_sub(old.len);
            removed += 1;
        }
    }

    // Phase C: trim by earliest expiry if still over budget
    if total > max_bytes && removed < max_remove && !trim_candidates.is_empty()
    {
        trim_candidates.sort_by_key(|(exp, _)| *exp);
        for (_exp, k) in trim_candidates {
            if removed >= max_remove {
                break;
            }
            if total <= max_bytes {
                break;
            }
            if let Some((_k, old)) = MEMORY_CACHE.remove(&k) {
                debug!(
                    "🔥 cache GC rm: {} ({} bytes) [reason: trim]",
                    k.display(),
                    old.len
                );
                unreserve_cache_bytes(old.len);
                total = total.saturating_sub(old.len);
                removed += 1;
            }
        }
    }
}

/// Sweep FILE_CACHE for expired/invalidated entries and orphan encoded files
#[inline]
pub fn sweep_file_cache(max_remove: usize) {
    let now = Instant::now();
    let mut removed: usize = 0;
    let mut to_remove: Vec<(PathBuf, Reason)> = Vec::new();

    // Phase A: decide what to remove (no mutations while holding iter guards)
    for item in FILE_CACHE.iter() {
        if removed + to_remove.len() >= max_remove {
            break;
        }
        let k = item.key().clone();
        let v = item.value();

        // Orphan: original file missing on disk
        if !k.exists() {
            to_remove.push((k, Reason::Orphan));
            continue;
        }

        // Expired
        if v.expires_at <= now {
            to_remove.push((k, Reason::Expired));
            continue;
        }

        // Invalidated: mtime changed (file modified)
        if let Ok(meta) = std::fs::metadata(&k)
            && let Ok(mtime) = meta.modified()
            && mtime != v.mtime
        {
            to_remove.push((k, Reason::Invalidated));
            continue;
        }
    }

    // Phase B: remove entries and encoded files
    for (k, why) in to_remove.drain(..) {
        if removed >= max_remove {
            break;
        }
        if let Some((_k, old)) = FILE_CACHE.remove(&k) {
            let reason = match why {
                Reason::Orphan => "orphan",
                Reason::Expired => "expired",
                Reason::Invalidated => "invalidated",
            };
            debug!("🔥 file cache GC rm: {} [reason: {}]", k.display(), reason);

            // Delete encoded file from disk
            if old.encoded_file.exists() {
                if let Err(e) = std::fs::remove_file(&old.encoded_file) {
                    debug!(
                        "⚠️  failed to delete {}: {}",
                        old.encoded_file.display(),
                        e
                    );
                } else {
                    debug!("🗑️  deleted {}", old.encoded_file.display());
                }
            }

            removed += 1;
        }
    }
}
