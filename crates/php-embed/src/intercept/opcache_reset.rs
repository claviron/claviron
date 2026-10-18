//! `opcache_reset` intercept — captures every PHP-level
//! `opcache_reset()` call, broadcasts the flush to peer workers via
//! the SHM bus, and delegates to opcache's stock handler so PHP
//! semantics stay intact.
//!
//! Peer replay is deliberately coalesced. `opcache_reset()` has no
//! payload and means "the whole opcache must be reset"; once that
//! intent is pending for this worker, additional reset broadcasts do
//! not add information. We keep a single pending bit instead of a
//! queue, so reset bursts apply at most once on the next PHP request
//! entry. This preserves propagation while avoiding repeated
//! whole-cache resets under active traffic.

use std::ffi::c_char;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, Function};
use tracing::{debug, info, warn};

static ORIGINAL_OPCACHE_RESET: OnceLock<
    unsafe extern "C" fn(
        execute_data: *mut ext_php_rs::ffi::_zend_execute_data,
        retval: *mut ext_php_rs::ffi::_zval_struct,
    ),
> = OnceLock::new();

// `opcache_reset()` is a level-triggered invalidation, not a
// payload-bearing event: once this worker knows "the whole opcache
// must be reset", later reset broadcasts before the next PHP request
// do not carry extra state. Coalescing deliberately loses event
// count/timing, but preserves the only semantic that matters here:
// at least one peer requested a whole-opcache reset. This keeps reset
// storms from running `opcache_reset()` N times in the request-start
// hook, where active SAPI traffic is most sensitive.
static APPLY_PENDING: AtomicBool = AtomicBool::new(false);

pub(crate) fn enqueue_apply(event: crate::ipc::OwnedInvalEvent) {
    match event {
        crate::ipc::OwnedInvalEvent::OpcacheReset => {
            if APPLY_PENDING.swap(true, Ordering::AcqRel) {
                debug!(
                    target: "claviron::opcache_intercept",
                    "coalesced duplicate opcache_reset broadcast; reset already pending"
                );
            }
        }
        other => {
            warn!(
                target: "claviron::opcache_intercept",
                event = ?other,
                "opcache_reset apply queue: unexpected variant; dispatcher bug?"
            );
        }
    }
}

pub(crate) fn drain_apply_queue() {
    if !APPLY_PENDING.swap(false, Ordering::AcqRel) {
        return;
    }
    let Some(opcache_reset) = Function::try_from_function("opcache_reset")
    else {
        warn!(
            target: "claviron::opcache_intercept",
            "apply: opcache_reset not registered; events dropped"
        );
        return;
    };

    let result = super::with_replay_guard(|| opcache_reset.try_call(vec![]));
    match result {
        Ok(retval) => {
            debug!(
                target: "claviron::opcache_intercept",
                kind = "applied",
                success = retval.is_true(),
                "broadcast opcache_reset applied"
            );
        }
        Err(e) => {
            warn!(
                target: "claviron::opcache_intercept",
                kind = "applied",
                error = %e,
                "broadcast opcache_reset call failed"
            );
        }
    }
}

/// PHP-callable: replacement for `opcache_reset()` that observes
/// the call before delegating. Broadcasts to peer workers so each
/// resets its local opcache.
extern "C" fn claviron_opcache_reset_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    match ORIGINAL_OPCACHE_RESET.get() {
        Some(original) => unsafe {
            original(ex as *mut ExecuteData, retval as *mut Zval);
        },
        None => {
            retval.set_bool(false);
            return;
        }
    }

    if retval.is_true()
        && !super::replay_in_progress()
        && let Some(bus) = crate::bus::bus()
    {
        bus.publish(crate::ipc::InvalEvent::OpcacheReset);
    }

    info!(
        target: "claviron::opcache_intercept",
        success = retval.is_true(),
        "opcache_reset intercepted"
    );
}

/// Swap PHP's `opcache_reset` handler with our intercept in the
/// **current ZTS thread's** `CG(function_table)`. Idempotent.
///
/// Skipped silently if opcache is not loaded.
pub(crate) fn ensure_opcache_reset_intercept_for_current_thread() {
    use ext_php_rs::ffi::{_zend_function, zend_hash_str_find_ptr_lc};

    type FunctionPointerHandler =
        extern "C" fn(execute_data: *mut ExecuteData, retval: *mut Zval);
    type RawFunctionPointerHandler = unsafe extern "C" fn(
        execute_data: *mut ext_php_rs::ffi::_zend_execute_data,
        retval: *mut ext_php_rs::ffi::_zval_struct,
    );

    unsafe {
        let cg = ext_php_rs::ffi::ext_php_rs_compiler_globals();
        if cg.is_null() {
            warn!("opcache_reset intercept: compiler globals null");
            return;
        }
        let function_table_ptr = (*cg).function_table;
        if function_table_ptr.is_null() {
            warn!("opcache_reset intercept: CG(function_table) is null");
            return;
        }

        let key: &[u8] = b"opcache_reset";
        let func_ptr = zend_hash_str_find_ptr_lc(
            function_table_ptr,
            key.as_ptr() as *const c_char,
            key.len(),
        ) as *mut _zend_function;

        if func_ptr.is_null() {
            warn!(
                "opcache_reset intercept: 'opcache_reset' not in CG(function_table); opcache \
                 extension not loaded for this thread — skipping observer"
            );
            return;
        }

        let our_handler_addr =
            claviron_opcache_reset_handler as *const () as usize;

        if let Some(current) = (*func_ptr).internal_function.handler {
            if current as *const () as usize == our_handler_addr {
                return;
            }
            let _ = ORIGINAL_OPCACHE_RESET.set(current);
        } else {
            warn!(
                "opcache_reset intercept: 'opcache_reset' has null handler; skipping"
            );
            return;
        }

        let new_handler: FunctionPointerHandler = std::mem::transmute(
            claviron_opcache_reset_handler as extern "C" fn(_, _),
        );
        (*func_ptr).internal_function.handler = Some(std::mem::transmute::<
            FunctionPointerHandler,
            RawFunctionPointerHandler,
        >(new_handler));

        #[cfg(debug_assertions)]
        tracing::trace!(
            "opcache_reset intercept: handler swapped for current thread"
        );
    }
}
