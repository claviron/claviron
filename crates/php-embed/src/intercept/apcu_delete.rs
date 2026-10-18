//! `apcu_delete` intercept — broadcasts cache invalidation to peer
//! workers so the fleet's view of "what's stale" stays in sync.
//!
//! > **Invalidate = let time flow.**
//!
//! # Why we started here: invalidate is how time flows
//!
//! In a multi-worker cache, each worker holds its own snapshot taken
//! at a slightly different moment. When two workers disagree, the
//! disagreement is rarely *error* — it's *temporal displacement*.
//! One worker is just looking at a slightly earlier slice of the
//! same underlying truth (the database).
//!
//! What's seen across workers is not wrong; it's delayed. And time,
//! by passing, resolves the delay on its own — provided the system
//! has a mechanism to **let time flow forward**. That mechanism is
//! invalidation:
//!
//!   - `apcu_delete(key)`        → "this key's snapshot is stale; refresh"
//!   - `apcu_clear_cache()`      → "the whole APCu snapshot is stale"
//!   - `opcache_invalidate(file)` → "this opcache entry is stale"
//!   - `opcache_reset()`         → "the entire opcache is stale"
//!
//! Without these signals, a worker can be frozen on yesterday's
//! snapshot indefinitely. Brief inconsistency between workers is
//! acceptable; *permanent staleness* is the silent failure mode we
//! actually fear. Invalidation is the system's heartbeat — by
//! continuously declaring "this is no longer fresh", we keep the
//! fleet's clocks unstuck.
//!
//! That is why this — `apcu_delete` — was the first intercept we
//! built. It is the smallest, most concrete instance of the broader
//! contract: *cache is presentation, the database is truth, and time
//! flowing forward is what reconciles them.*
//!
//! # How it works
//!
//! Replication direction:
//!
//! ```text
//!   worker A (caller)         SHM bus           worker B..N (peers)
//!   ──────────────────       ─────────         ───────────────────
//!   apcu_delete("foo")  ──▶  publish(foo) ──▶  drain → apply queue
//!     │                                          │
//!     └─ delegate to original handler             └─ next request:
//!                                                    drain_apply_queue()
//!                                                    → apcu_delete("foo")
//!                                                    (under replay guard;
//!                                                     no re-publish)
//! ```
//!
//! The `with_replay_guard` thread-local + the fleet's source-node skip
//! together provide two layers of recursion safety so a broadcast
//! cannot fan out into an infinite delete-publish loop.

use std::ffi::c_char;
use std::sync::{Arc, OnceLock};

use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, Function};
use tracing::{debug, info, warn};

/// Original `apcu_delete` handler captured at module-startup override
/// time. Set exactly once by [`ensure_apcu_delete_intercept_for_current_thread`];
/// read from each intercepted call to delegate semantics back to APCu.
static ORIGINAL_APCU_DELETE: OnceLock<
    unsafe extern "C" fn(
        execute_data: *mut ext_php_rs::ffi::_zend_execute_data,
        retval: *mut ext_php_rs::ffi::_zval_struct,
    ),
> = OnceLock::new();

/// Apply queue — events received from peer workers, waiting to be
/// applied to this worker's local APCu. Bus runnable pushes; PHP
/// threads pop on request entry (they're the only ones with a TSRM-
/// bound context capable of calling `apcu_delete`).
///
/// Mutex<VecDeque> is fine: contention is incidental (events are
/// sparse, drain holds the lock briefly), and `std::sync::mpsc`
/// receivers aren't shareable across worker threads.
static APPLY_QUEUE: OnceLock<
    std::sync::Mutex<std::collections::VecDeque<crate::ipc::OwnedInvalEvent>>,
> = OnceLock::new();

fn apply_queue() -> &'static std::sync::Mutex<
    std::collections::VecDeque<crate::ipc::OwnedInvalEvent>,
> {
    APPLY_QUEUE.get_or_init(|| {
        std::sync::Mutex::new(std::collections::VecDeque::new())
    })
}

/// Bus runnable enqueues received events here; the next PHP thread
/// to enter `run_request` will drain and apply them. Called from
/// outside any PHP context (Tokio task on whatever runtime worker).
pub(crate) fn enqueue_apply(event: crate::ipc::OwnedInvalEvent) {
    if let Ok(mut q) = apply_queue().lock() {
        if q.contains(&event) {
            debug!(target: "claviron::apcu_intercept", "PHP cache invalidation coalesced: {event:?} (already pending)");
        } else {
            q.push_back(event);
        }
    }
}

/// Drain queued peer events and apply each to local APCu. Called
/// from within a TSRM-bound PHP request (after `php_request_startup`,
/// before script execution) so `Function::try_call` has a valid
/// executor.
///
/// Each apply runs under [`with_replay_guard`] so the apcu_delete
/// intercept on this same thread skips re-publishing — combined
/// with the fleet event transport skipping the source node, two
/// layers of recursion safety.
pub(crate) fn drain_apply_queue() {
    let drained: Vec<crate::ipc::OwnedInvalEvent> = match apply_queue().lock() {
        Ok(mut q) => q.drain(..).collect(),
        Err(_) => return,
    };
    if drained.is_empty() {
        return;
    }
    let Some(apcu_delete) = Function::try_from_function("apcu_delete") else {
        warn!(
            target: "claviron::apcu_intercept",
            count = drained.len(),
            "apply: apcu_delete not registered; events dropped"
        );
        return;
    };

    for event in drained {
        match event {
            crate::ipc::OwnedInvalEvent::Delete(key) => {
                let key_str = key.as_str();
                let result = super::with_replay_guard(|| {
                    apcu_delete.try_call(vec![&key_str])
                });
                match result {
                    Ok(retval) => {
                        debug!(
                            target: "claviron::apcu_intercept",
                            kind = "applied",
                            key = %key,
                            success = retval.is_true(),
                            "broadcast delete applied: apcu_delete key={key} replay=true deleted={}",
                            retval.is_true()
                        );
                    }
                    Err(e) => {
                        warn!(
                            target: "claviron::apcu_intercept",
                            kind = "applied",
                            key = %key,
                            error = %e,
                            "broadcast delete call failed: apcu_delete key={key} replay=true error={e}"
                        );
                    }
                }
            }
            crate::ipc::OwnedInvalEvent::DeletePattern(regex) => {
                // Apply requires reconstructing an APCUIterator on
                // this side. Out of scope for the first apply pass
                // — log and skip; pattern propagation lands when we
                // extract the regex source on the producer side.
                debug!(
                    target: "claviron::apcu_intercept",
                    kind = "applied",
                    regex = %regex,
                    "broadcast pattern delete deferred: pattern={regex} (apply not implemented)"
                );
            }
            other => {
                // Defensive: the event consumer fans out
                // by variant, so this queue should only receive
                // Delete / DeletePattern. Anything else means the
                // dispatcher leaked an event into the wrong queue.
                warn!(
                    target: "claviron::apcu_intercept",
                    event = ?other,
                    "apcu_delete apply queue: unexpected variant {other:?}; dispatcher bug?"
                );
            }
        }
    }
}

/// PHP-callable: replacement for `apcu_delete($arg)` that observes
/// the call before delegating to APCu's real handler. Captures all
/// three argument forms — string key, array of string keys,
/// `APCUIterator` (logged by class name only) — and publishes the
/// invalidation to the SHM bus so peer workers learn about it.
///
/// Skips the publish path when [`APCU_BROADCAST_REPLAY`] is set
/// (we're applying an event we received from a peer; re-publishing
/// would loop).
extern "C" fn claviron_apcu_delete_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    // Best-effort capture: peek at the first arg as a raw zval and
    // dispatch on its runtime type. Pre-parser: the parser's `parse()`
    // consumes the args (zends ARG_INFO state machine), and we want
    // the original handler below to see them untouched.
    let captured = capture_apcu_delete_arg(ex);

    // Delegate to APCu's real handler so PHP semantics stay intact.
    // Missing original (override skipped at boot) → return false to
    // keep PHP's contract for "delete failed" rather than panicking.
    match ORIGINAL_APCU_DELETE.get() {
        Some(original) => unsafe {
            original(ex as *mut ExecuteData, retval as *mut Zval);
        },
        None => {
            retval.set_bool(false);
            return;
        }
    }

    // Broadcast successful invalidations to peer workers, unless
    // we're already applying a peer's broadcast (replay guard).
    let replay = super::replay_in_progress();
    if !replay && let Some(bus) = crate::bus::bus() {
        publish_apcu_events(bus, &captured, retval);
    }

    match captured {
        ApcuDeleteArg::String(key) => {
            // String form: APCu returns bool — true on success.
            info!(
                target: "claviron::apcu_intercept",
                arg = "string",
                key = %key,
                success = retval.is_true(),
                replay,
                "apcu_delete intercepted: key={key} replay={replay} deleted={}",
                retval.is_true()
            );
        }
        ApcuDeleteArg::Array(keys) => {
            // Array form: APCu returns an array of keys that could
            // NOT be deleted. Empty array == full success (which PHP
            // truthiness reports as `false`, so `is_true()` is the
            // wrong probe here).
            let failed: Vec<String> = retval
                .array()
                .map(|h| h.values().filter_map(|v| v.string()).collect())
                .unwrap_or_default();
            info!(
                target: "claviron::apcu_intercept",
                arg = "array",
                count = keys.len(),
                keys = ?keys,
                failed_count = failed.len(),
                failed = ?failed,
                success = failed.is_empty(),
                replay,
                "apcu_delete intercepted: keys={keys:?} replay={replay} failed={failed:?}"
            );
        }
        ApcuDeleteArg::Iterator(class) => {
            info!(
                target: "claviron::apcu_intercept",
                arg = "iterator",
                class = %class,
                success = retval.is_true(),
                replay,
                "apcu_delete intercepted: iterator={class} replay={replay} success={}",
                retval.is_true()
            );
        }
        ApcuDeleteArg::Other(ty) => {
            info!(
                target: "claviron::apcu_intercept",
                arg = "other",
                php_type = %ty,
                success = retval.is_true(),
                replay,
                "apcu_delete intercepted: php_type={ty} replay={replay} success={}",
                retval.is_true()
            );
        }
        ApcuDeleteArg::None => {
            info!(
                target: "claviron::apcu_intercept",
                arg = "none",
                replay,
                "apcu_delete intercepted: no args replay={replay}"
            );
        }
    }
}

#[derive(Debug)]
enum ApcuDeleteArg {
    String(String),
    Array(Vec<String>),
    Iterator(String),
    Other(String),
    None,
}

/// Read the first positional argument of an `apcu_delete` call as a
/// raw zval and classify it without consuming the parser state, so
/// the original handler still sees a fresh arg frame when we delegate.
fn capture_apcu_delete_arg(ex: &mut ExecuteData) -> ApcuDeleteArg {
    // SAFETY: `zend_call_arg` is doc-hidden but pub; the pointer it
    // returns has the lifetime of `ex`. We only borrow the Zval for
    // classification before the original handler runs (which mutates
    // its own retval, not the arg slot) so no aliasing.
    let zval = unsafe { ex.zend_call_arg(0) };
    let Some(zval) = zval else {
        return ApcuDeleteArg::None;
    };

    if zval.is_string() {
        return zval
            .string()
            .map(ApcuDeleteArg::String)
            .unwrap_or_else(|| ApcuDeleteArg::Other("string".to_string()));
    }

    if let Some(arr) = zval.array() {
        let keys: Vec<String> =
            arr.values().filter_map(|v| v.string()).collect();
        return ApcuDeleteArg::Array(keys);
    }

    if let Some(obj) = zval.object() {
        let class =
            obj.get_class_name().unwrap_or_else(|_| "<unknown>".to_string());
        return ApcuDeleteArg::Iterator(class);
    }

    ApcuDeleteArg::Other(format!("{:?}", zval.get_type()))
}

/// Forward an apcu_delete call to peer workers via the SHM bus.
///
/// String → one [`InvalEvent::Delete`]. Array → one Delete per
/// successful key (failed keys, present in the retval array, are
/// not broadcast — the peer worker would have nothing to do). The
/// `APCUIterator` form publishes a `DeletePattern(class)` placeholder;
/// real pattern propagation needs the iterator's regex which we
/// don't extract yet.
fn publish_apcu_events(
    bus: &Arc<crate::bus::PhpInvalidationBus>,
    captured: &ApcuDeleteArg,
    retval: &Zval,
) {
    use crate::invalidation::ipc::InvalEvent;
    match captured {
        ApcuDeleteArg::String(key) => {
            if retval.is_true() {
                bus.publish(InvalEvent::Delete(key));
            }
        }
        ApcuDeleteArg::Array(keys) => {
            // APCu returns an array of keys that could NOT be
            // deleted. Broadcast only the keys that succeeded.
            let failed: std::collections::HashSet<String> = retval
                .array()
                .map(|h| h.values().filter_map(|v| v.string()).collect())
                .unwrap_or_default();
            for key in keys {
                if !failed.contains(key) {
                    bus.publish(InvalEvent::Delete(key));
                }
            }
        }
        ApcuDeleteArg::Iterator(class) => {
            // Placeholder — we don't yet read the iterator's
            // backing regex/array of keys, so peers receive only
            // the class name. Receiver-side currently logs only
            // (see crate::bus), so this is
            // observable but non-actionable.
            if retval.is_true() {
                bus.publish(InvalEvent::DeletePattern(class));
            }
        }
        ApcuDeleteArg::Other(_) | ApcuDeleteArg::None => {}
    }
}

/// Swap PHP's `apcu_delete` handler with our intercept in the
/// **current ZTS thread's** `CG(function_table)`. Idempotent: a
/// no-op if this thread's entry already points at our intercept.
///
/// Called from two places, mirroring the belt-and-suspenders pattern
/// `claviron_is_cancelled` uses:
///
/// 1. `ensure_sapi` — covers the bootstrap thread that wins
///    `php_module_startup` (its `CG(function_table)` is the one PHP
///    populated synchronously during startup).
/// 2. `php_thread_main`, right after `ext_php_rs_sapi_per_thread_init` —
///    covers every other pool thread, whose `CG(function_table)` is a
///    fresh per-thread copy created by the per-thread bind. Without
///    this call, `apcu_delete` in those threads still points at
///    APCu's stock handler and our observer never fires.
///
/// Skipped silently if APCu is not loaded (no `apcu_delete` entry in
/// `CG(function_table)`); the thread keeps running with no observer.
pub(crate) fn ensure_apcu_delete_intercept_for_current_thread() {
    use ext_php_rs::ffi::{_zend_function, zend_hash_str_find_ptr_lc};

    type FunctionPointerHandler =
        extern "C" fn(execute_data: *mut ExecuteData, retval: *mut Zval);
    type RawFunctionPointerHandler = unsafe extern "C" fn(
        execute_data: *mut ext_php_rs::ffi::_zend_execute_data,
        retval: *mut ext_php_rs::ffi::_zval_struct,
    );

    // SAFETY: function table mutation is performed in two contexts —
    // (1) ensure_sapi's `OnceLock` initialiser before any request
    // thread runs, (2) per-thread bootstrap before this thread starts
    // pulling jobs off its mpsc. Either way no concurrent reader or
    // writer on `internal_function.handler` for this thread.
    unsafe {
        let cg = ext_php_rs::ffi::ext_php_rs_compiler_globals();
        if cg.is_null() {
            warn!("apcu_delete intercept: compiler globals null");
            return;
        }
        let function_table_ptr = (*cg).function_table;
        if function_table_ptr.is_null() {
            warn!("apcu_delete intercept: CG(function_table) is null");
            return;
        }

        let key: &[u8] = b"apcu_delete";
        let func_ptr = zend_hash_str_find_ptr_lc(
            function_table_ptr,
            key.as_ptr() as *const c_char,
            key.len(),
        ) as *mut _zend_function;

        if func_ptr.is_null() {
            warn!(
                "apcu_delete intercept: 'apcu_delete' not in CG(function_table); APCu extension \
                 not loaded for this thread — skipping observer"
            );
            return;
        }

        let our_handler_addr =
            claviron_apcu_delete_handler as *const () as usize;

        if let Some(current) = (*func_ptr).internal_function.handler {
            // Already our intercept (idempotent re-entry, e.g.
            // boot thread that ran ensure_sapi then later took its
            // own `php_thread_main` per-thread init slot).
            if current as *const () as usize == our_handler_addr {
                return;
            }
            // First time we see APCu's real handler. Stash it for
            // the intercept to delegate to. `OnceLock::set` accepts
            // exactly one write; subsequent threads observe the same
            // pointer (in ZTS, internal function handlers resolve to
            // the same compiled C symbol per process).
            let _ = ORIGINAL_APCU_DELETE.set(current);
        } else {
            warn!(
                "apcu_delete intercept: 'apcu_delete' has null handler; skipping override"
            );
            return;
        }

        let new_handler: FunctionPointerHandler = std::mem::transmute(
            claviron_apcu_delete_handler as extern "C" fn(_, _),
        );
        (*func_ptr).internal_function.handler = Some(std::mem::transmute::<
            FunctionPointerHandler,
            RawFunctionPointerHandler,
        >(new_handler));

        #[cfg(debug_assertions)]
        tracing::trace!(
            "apcu_delete intercept: handler swapped for current thread"
        );
    }
}
