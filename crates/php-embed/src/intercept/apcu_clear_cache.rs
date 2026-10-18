//! `apcu_clear_cache` intercept — captures every PHP-level
//! `apcu_clear_cache()` call, broadcasts the flush to peer workers via
//! the SHM bus, and delegates to APCu's stock handler so PHP semantics
//! stay intact.

use std::ffi::c_char;
use std::sync::OnceLock;

use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, Function};
use tracing::{debug, info, warn};

/// Original `apcu_clear_cache` handler captured at module-startup
/// override time. Set exactly once by
/// [`ensure_apcu_clear_cache_intercept_for_current_thread`]; read
/// from each intercepted call to delegate semantics back to APCu.
static ORIGINAL_APCU_CLEAR_CACHE: OnceLock<
    unsafe extern "C" fn(
        execute_data: *mut ext_php_rs::ffi::_zend_execute_data,
        retval: *mut ext_php_rs::ffi::_zval_struct,
    ),
> = OnceLock::new();

/// Apply queue — events received from peer workers, waiting to be
/// applied to this worker's local APCu. Bus runnable pushes; PHP
/// threads pop on request entry.
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

/// Bus runnable enqueues received events here.
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
pub(crate) fn drain_apply_queue() {
    let drained: Vec<crate::ipc::OwnedInvalEvent> = match apply_queue().lock() {
        Ok(mut q) => q.drain(..).collect(),
        Err(_) => return,
    };
    if drained.is_empty() {
        return;
    }
    let Some(apcu_clear_cache) =
        Function::try_from_function("apcu_clear_cache")
    else {
        warn!(
            target: "claviron::apcu_intercept",
            count = drained.len(),
            "apply: apcu_clear_cache not registered; events dropped"
        );
        return;
    };

    for event in drained {
        match event {
            crate::ipc::OwnedInvalEvent::ApcuClearCache => {
                let result = super::with_replay_guard(|| {
                    apcu_clear_cache.try_call(vec![])
                });
                match result {
                    Ok(retval) => {
                        debug!(
                            target: "claviron::apcu_intercept",
                            kind = "applied",
                            success = retval.is_true(),
                            "broadcast apcu_clear_cache applied"
                        );
                    }
                    Err(e) => {
                        warn!(
                            target: "claviron::apcu_intercept",
                            kind = "applied",
                            error = %e,
                            "broadcast apcu_clear_cache call failed"
                        );
                    }
                }
            }
            other => {
                warn!(
                    target: "claviron::apcu_intercept",
                    event = ?other,
                    "apcu_clear_cache apply queue: unexpected variant; dispatcher bug?"
                );
            }
        }
    }
}

/// PHP-callable: replacement for `apcu_clear_cache()` that observes
/// the call before delegating to APCu's real handler. Broadcasts a
/// flush event to peer workers so each clears its local APCu.
extern "C" fn claviron_apcu_clear_cache_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    // Delegate to APCu's real handler so PHP semantics stay intact.
    match ORIGINAL_APCU_CLEAR_CACHE.get() {
        Some(original) => unsafe {
            original(ex as *mut ExecuteData, retval as *mut Zval);
        },
        None => {
            retval.set_bool(false);
            return;
        }
    }

    // Broadcast successful flush to peer workers, unless we're already
    // applying a peer's broadcast (replay guard).
    if retval.is_true()
        && !super::replay_in_progress()
        && let Some(bus) = crate::bus::bus()
    {
        bus.publish(crate::ipc::InvalEvent::ApcuClearCache);
    }

    info!(
        target: "claviron::apcu_intercept",
        success = retval.is_true(),
        "apcu_clear_cache intercepted"
    );
}

/// Swap PHP's `apcu_clear_cache` handler with our intercept in the
/// **current ZTS thread's** `CG(function_table)`. Idempotent.
///
/// Skipped silently if APCu is not loaded.
pub(crate) fn ensure_apcu_clear_cache_intercept_for_current_thread() {
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
            warn!("apcu_clear_cache intercept: compiler globals null");
            return;
        }
        let function_table_ptr = (*cg).function_table;
        if function_table_ptr.is_null() {
            warn!("apcu_clear_cache intercept: CG(function_table) is null");
            return;
        }

        let key: &[u8] = b"apcu_clear_cache";
        let func_ptr = zend_hash_str_find_ptr_lc(
            function_table_ptr,
            key.as_ptr() as *const c_char,
            key.len(),
        ) as *mut _zend_function;

        if func_ptr.is_null() {
            warn!(
                "apcu_clear_cache intercept: 'apcu_clear_cache' not in CG(function_table); APCu \
                 extension not loaded for this thread — skipping observer"
            );
            return;
        }

        let our_handler_addr =
            claviron_apcu_clear_cache_handler as *const () as usize;

        if let Some(current) = (*func_ptr).internal_function.handler {
            if current as *const () as usize == our_handler_addr {
                return;
            }
            let _ = ORIGINAL_APCU_CLEAR_CACHE.set(current);
        } else {
            warn!(
                "apcu_clear_cache intercept: 'apcu_clear_cache' has null handler; skipping"
            );
            return;
        }

        let new_handler: FunctionPointerHandler = std::mem::transmute(
            claviron_apcu_clear_cache_handler as extern "C" fn(_, _),
        );
        (*func_ptr).internal_function.handler = Some(std::mem::transmute::<
            FunctionPointerHandler,
            RawFunctionPointerHandler,
        >(new_handler));

        #[cfg(debug_assertions)]
        tracing::trace!(
            "apcu_clear_cache intercept: handler swapped for current thread"
        );
    }
}
