//! Candidate selection for a route that lists more than one FPM entry.
//!
//! `PhpRuntime.fpm` accepts one name or many. One name is dispatched
//! directly; many come here.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::ExecuteError;
use crate::service::{FpmEntry, PhpFcgiService};

static RR_TIE: AtomicUsize = AtomicUsize::new(0);

/// Pick the entry with the most free capacity from the given list.
///
/// Ranking:
///
/// 1. Pooled entries by free slots (`max_live - live`), ties broken
///    round-robin so equally-loaded pools alternate.
/// 2. A one-shot entry only when no pool in the list has a slot left —
///    or when the list names no pool at all. One-shot is "always
///    available, never warm": it dials per request, so it can absorb the
///    overflow, but preferring it while a warm pool has room would leave
///    that pool cold forever.
///
/// Every candidate is resolved through `get_entry`, which instantiates
/// it on first touch. That is load-bearing, not incidental: `entries` is
/// lazy and nothing else builds a balanced route's candidates, so
/// ranking them by a read of the already-built map would find an empty
/// map and report the whole list missing.
pub(crate) async fn pick_spacious_entry(
    svc: &PhpFcgiService,
    names: &[String],
) -> Result<(String, FpmEntry), ExecuteError> {
    let mut best_free: Option<usize> = None;
    let mut pooled: Vec<(&str, FpmEntry)> = Vec::new();
    let mut oneshot: Option<(&str, FpmEntry)> = None;

    for n in names {
        let Some(entry) = svc.get_entry(n).await else {
            continue;
        };

        let free = match &entry {
            // Ranked on what this worker may still take, not on the
            // entry's total: `stats().live` is this worker's census, so
            // the ceiling it is subtracted from has to be this worker's.
            FpmEntry::Pooled(pool) => {
                Some(pool.opts().max_live.saturating_sub(pool.stats().live))
            }
            FpmEntry::OneShot(_) => None,
        };

        match free {
            Some(free) => match best_free {
                Some(best) if free < best => continue,
                Some(best) if free == best => pooled.push((n.as_str(), entry)),
                _ => {
                    best_free = Some(free);
                    pooled.clear();
                    pooled.push((n.as_str(), entry));
                }
            },
            None => {
                oneshot.get_or_insert((n.as_str(), entry));
            }
        }
    }

    // No warm slot anywhere — spill to one-shot if the operator listed
    // one. Without one, fall through to a full pool and let `acquire`
    // queue the caller on its own `acquire_timeout`; that is a wait the
    // operator configured, not an error.
    let pool_has_room = matches!(best_free, Some(free) if free > 0);
    if !pool_has_room && let Some((name, entry)) = oneshot {
        return Ok((name.to_string(), entry));
    }

    if pooled.is_empty() {
        return Err(ExecuteError::Missing);
    }

    let idx = if pooled.len() == 1 {
        0
    } else {
        RR_TIE.fetch_add(1, Ordering::Relaxed) % pooled.len()
    };
    let (name, entry) = pooled.swap_remove(idx);
    Ok((name.to_string(), entry))
}
