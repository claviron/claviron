//! Function-table intercepts — replacement handlers for existing PHP
//! functions installed into `CG(function_table)` after module startup.
//!
//! Each submodule owns one intercepted function. The mechanism is the
//! same across all of them (lookup → swap `internal_function.handler`);
//! the consumer logic differs:
//!
//! * **Replication intercepts** — capture the call, broadcast to peer
//!   workers via the SHM bus, delegate to PHP's real handler. Peers
//!   apply the broadcast event under the replay guard so they
//!   don't re-publish.
//!
//!   - `apcu_delete` / `apcu_clear_cache` — opt-in `apcu-intercept`
//!   - `opcache_reset` / `opcache_invalidate` — default `opcache-intercept`
//!
//! * **Request-isolation intercepts** — no broadcast, route side-effects
//!   into per-request state.
//!
//!   - [`putenv`] — write to `ClavironContext::env_overlay`, libc
//!     `environ` untouched
//!
//! ## Intentionally NOT intercepted
//!
//! - `apcu_inc` / `apcu_dec` — race-condition risk under broadcast
//!   propagation delay (worker A increments + broadcasts, worker B
//!   independently increments before broadcast arrives → counter
//!   policy can be violated even though math converges).
//! - `apcu_store` / `apcu_add` — arbitrary-size payloads (objects,
//!   serialized arrays) push beyond SHM slot capacity and serialize
//!   per-call cost is high. Intent ambiguity (some stores are
//!   per-request memoization, not cluster cache).
//! - `apcu_fetch` / `apcu_exists` — read-only, broadcast meaningless.

#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
use std::cell::Cell;

#[cfg(feature = "apcu-intercept")]
pub(crate) mod apcu_clear_cache;
#[cfg(feature = "apcu-intercept")]
pub(crate) mod apcu_delete;
#[cfg(feature = "opcache-intercept")]
pub(crate) mod opcache_invalidate;
#[cfg(feature = "opcache-intercept")]
pub(crate) mod opcache_reset;
pub(crate) mod putenv;
pub(crate) mod sleep;

/// Install only when the shared SAPI plan enabled peer invalidation.
pub(crate) fn ensure_cache_invalidation_intercepts_for_current_thread() {
    #[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
    {
        if crate::bus::bus().is_none() {
            return;
        }
        #[cfg(feature = "apcu-intercept")]
        {
            apcu_delete::ensure_apcu_delete_intercept_for_current_thread();
            apcu_clear_cache::ensure_apcu_clear_cache_intercept_for_current_thread();
        }
        #[cfg(feature = "opcache-intercept")]
        {
            opcache_reset::ensure_opcache_reset_intercept_for_current_thread();
            opcache_invalidate::ensure_opcache_invalidate_intercept_for_current_thread();
        }
    }
}

/// Work-boundary hooks fired before an ordinary SAPI script and before a
/// persistent application receives its next request or event. Each intercept
/// that needs a TSRM-bound executor (for example, draining a peer invalidation
/// queue) plugs in here.
pub(crate) fn pre_execute_work_hooks() {
    #[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
    {
        if crate::bus::bus().is_none() {
            return;
        }
        #[cfg(feature = "apcu-intercept")]
        {
            apcu_delete::drain_apply_queue();
            apcu_clear_cache::drain_apply_queue();
        }
        #[cfg(feature = "opcache-intercept")]
        {
            opcache_reset::drain_apply_queue();
            opcache_invalidate::drain_apply_queue();
        }
    }
}

// ---------------------------------------------------------------------
// Replay guard — shared by all replication intercepts.
//
// Set while applying a *received* broadcast event from a peer worker
// so the local intercept does NOT re-publish to the bus and trigger
// a fan-out loop:
//
//   Worker A: apcu_delete("foo") → intercept → publish(foo) → original
//   Worker B: drain(foo) → apply → original    ← guard set
//                                  (no publish, no loop)
//
// Per-thread because apply happens in whichever pool thread serves
// the next request after the bus runnable enqueues.
// ---------------------------------------------------------------------

#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
thread_local! {
    static BROADCAST_REPLAY: Cell<bool> = const { Cell::new(false) };
}

/// Run `f` with the broadcast-replay guard set. The guard is restored
/// to its previous state on return, even if `f` panics or guards nest.
#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
pub(crate) fn with_replay_guard<R, F: FnOnce() -> R>(f: F) -> R {
    struct Guard(bool);
    impl Drop for Guard {
        fn drop(&mut self) {
            BROADCAST_REPLAY.with(|c| c.set(self.0));
        }
    }
    let previous = BROADCAST_REPLAY.with(|c| c.replace(true));
    let _g = Guard(previous);
    f()
}

#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
pub(crate) fn replay_in_progress() -> bool {
    BROADCAST_REPLAY.with(Cell::get)
}
