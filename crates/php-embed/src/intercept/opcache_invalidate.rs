//! `opcache_invalidate` intercept — captures every PHP-level
//! `opcache_invalidate(filename, force = false)` call, broadcasts the
//! invalidation to peer workers via the SHM bus, and delegates to
//! opcache's stock handler so PHP semantics stay intact.
//!
//! The `force` argument is preserved across the broadcast so peer
//! workers reproduce the original semantic (force=true bypasses
//! `opcache.validate_timestamps` checks).

use std::ffi::c_char;
use std::sync::OnceLock;

use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, Function};
use tracing::{debug, warn};

static ORIGINAL_OPCACHE_INVALIDATE: OnceLock<
    unsafe extern "C" fn(
        execute_data: *mut ext_php_rs::ffi::_zend_execute_data,
        retval: *mut ext_php_rs::ffi::_zval_struct,
    ),
> = OnceLock::new();

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

pub(crate) fn enqueue_apply(event: crate::ipc::OwnedInvalEvent) {
    if let Ok(mut q) = apply_queue().lock() {
        if q.contains(&event) {
            debug!(target: "claviron::opcache_intercept", "PHP cache invalidation coalesced: {event:?} (already pending)");
        } else {
            q.push_back(event);
        }
    }
}

pub(crate) fn drain_apply_queue() {
    let drained: Vec<crate::ipc::OwnedInvalEvent> = match apply_queue().lock() {
        Ok(mut q) => q.drain(..).collect(),
        Err(_) => return,
    };
    if drained.is_empty() {
        return;
    }
    let Some(opcache_invalidate) =
        Function::try_from_function("opcache_invalidate")
    else {
        warn!(
            target: "claviron::opcache_intercept",
            count = drained.len(),
            "apply: opcache_invalidate not registered; events dropped"
        );
        return;
    };

    for event in drained {
        match event {
            crate::ipc::OwnedInvalEvent::OpcacheInvalidate(filename, force) => {
                let file_str = filename.as_str();
                let result = super::with_replay_guard(|| {
                    opcache_invalidate.try_call(vec![&file_str, &force])
                });
                match result {
                    Ok(retval) => {
                        debug!(
                            target: "claviron::opcache_intercept",
                            kind = "applied",
                            file = %filename,
                            force,
                            success = retval.is_true(),
                            "broadcast opcache_invalidate applied"
                        );
                    }
                    Err(e) => {
                        warn!(
                            target: "claviron::opcache_intercept",
                            kind = "applied",
                            file = %filename,
                            force,
                            error = %e,
                            "broadcast opcache_invalidate call failed"
                        );
                    }
                }
            }
            other => {
                warn!(
                    target: "claviron::opcache_intercept",
                    event = ?other,
                    "opcache_invalidate apply queue: unexpected variant; dispatcher bug?"
                );
            }
        }
    }
}

/// Read the (filename, force) pair from `opcache_invalidate`'s call
/// frame as raw zvals, without consuming the parser state. The
/// original handler runs its own parser when we delegate, so the args
/// must remain untouched.
fn capture_opcache_invalidate_args(
    ex: &mut ExecuteData
) -> (Option<String>, bool) {
    let filename = unsafe { ex.zend_call_arg(0) }.and_then(|z| z.string());
    // `force` defaults to false in PHP if omitted. zend_call_arg(1)
    // returns None for omitted arg → default false. If present and
    // non-bool, PHP's own conversion semantics are applied by the
    // original handler; for our broadcast we treat truthy as true.
    let force =
        unsafe { ex.zend_call_arg(1) }.map(|z| z.is_true()).unwrap_or(false);
    (filename, force)
}

/// PHP-callable: replacement for `opcache_invalidate($filename, $force)`
/// that observes the call before delegating. Broadcasts to peer
/// workers so each invalidates the same script in its local opcache.
extern "C" fn claviron_opcache_invalidate_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    let (captured_file, captured_force) = capture_opcache_invalidate_args(ex);

    match ORIGINAL_OPCACHE_INVALIDATE.get() {
        Some(original) => unsafe {
            original(ex as *mut ExecuteData, retval as *mut Zval);
        },
        None => {
            retval.set_bool(false);
            return;
        }
    }

    // Broadcast successful invalidations only. Failed local invalidate
    // (file doesn't exist, opcache disabled) → don't propagate noise.
    if retval.is_true()
        && !super::replay_in_progress()
        && let Some(file) = captured_file.as_deref()
        && let Some(bus) = crate::bus::bus()
    {
        bus.publish(crate::ipc::InvalEvent::OpcacheInvalidate(
            file,
            captured_force,
        ));
    }

    debug!(
        target: "claviron::opcache_intercept",
        file = ?captured_file,
        force = captured_force,
        success = retval.is_true(),
        "opcache_invalidate intercepted"
    );
}

/// Swap PHP's `opcache_invalidate` handler with our intercept in the
/// **current ZTS thread's** `CG(function_table)`. Idempotent.
///
/// Skipped silently if opcache is not loaded.
pub(crate) fn ensure_opcache_invalidate_intercept_for_current_thread() {
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
            warn!("opcache_invalidate intercept: compiler globals null");
            return;
        }
        let function_table_ptr = (*cg).function_table;
        if function_table_ptr.is_null() {
            warn!("opcache_invalidate intercept: CG(function_table) is null");
            return;
        }

        let key: &[u8] = b"opcache_invalidate";
        let func_ptr = zend_hash_str_find_ptr_lc(
            function_table_ptr,
            key.as_ptr() as *const c_char,
            key.len(),
        ) as *mut _zend_function;

        if func_ptr.is_null() {
            warn!(
                "opcache_invalidate intercept: 'opcache_invalidate' not in CG(function_table); \
                 opcache extension not loaded for this thread — skipping observer"
            );
            return;
        }

        let our_handler_addr =
            claviron_opcache_invalidate_handler as *const () as usize;

        if let Some(current) = (*func_ptr).internal_function.handler {
            if current as *const () as usize == our_handler_addr {
                return;
            }
            let _ = ORIGINAL_OPCACHE_INVALIDATE.set(current);
        } else {
            warn!(
                "opcache_invalidate intercept: 'opcache_invalidate' has null handler; skipping"
            );
            return;
        }

        let new_handler: FunctionPointerHandler = std::mem::transmute(
            claviron_opcache_invalidate_handler as extern "C" fn(_, _),
        );
        (*func_ptr).internal_function.handler = Some(std::mem::transmute::<
            FunctionPointerHandler,
            RawFunctionPointerHandler,
        >(new_handler));

        #[cfg(debug_assertions)]
        tracing::trace!(
            "opcache_invalidate intercept: handler swapped for current thread"
        );
    }
}
