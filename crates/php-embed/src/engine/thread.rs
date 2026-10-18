//! Shared OS-thread identity, PHP/TSRM binding and teardown for both execution modes.

use std::cell::OnceCell;
use std::ffi::CString;

use super::ensure_sapi;
use crate::sapi::zend_update_current_locale;

// ---------------------------------------------------------------------
// Per-thread identity stash. Set in `php_thread_main` after per-thread
// TSRM init; read pointer-only by `register_server_variables` to
// populate `$_SERVER['CLAVIRON_THREAD_ID']`. Same zero-allocation
// pattern as the process-wide `SERVER_SCOPED_VARS`, but per-thread
// because the value differs per pool slot.
// ---------------------------------------------------------------------

thread_local! {
    pub(crate) static THREAD_ID_CSTR: OnceCell<CString> = const { OnceCell::new() };
}

/// Bind the current OS thread to the PHP runtime. The single thread
/// prologue shared by request-pool, task and application threads —
/// the §4.4.1 multi-thread registration contract lives here and only
/// here. Order is load-bearing:
///
/// 1. `ensure_sapi()` — process-wide startup, first caller wins.
/// 2. Per-thread TSRM bind + locale.
/// 3. Claviron host functions re-checked on this thread's CG.
/// 4. Replication intercepts re-applied on this thread's CG
///    (idempotent on the bootstrap thread, a real override elsewhere).
/// 5. Per-thread identity stash for `$_SERVER['CLAVIRON_THREAD_ID']`.
///
/// `owner` is the execution generation that spawned this thread. It is what lets
/// that generation's drain interrupt its own executions and nothing else; see
/// `super::interrupt`.
pub(crate) fn bind_php_thread(
    thread_id: impl std::fmt::Display,
    owner: super::interrupt::ExecutionOwner,
) -> Result<(), String> {
    ensure_sapi().map_err(|err| format!("failed to init SAPI: {err:?}"))?;
    unsafe {
        ext_php_rs::embed::ext_php_rs_sapi_per_thread_init();
        zend_update_current_locale();
    }
    crate::functions::ensure_functions_for_current_thread().map_err(|err| {
        format!("failed to register claviron PHP functions: {err}")
    })?;
    crate::intercept::ensure_cache_invalidation_intercepts_for_current_thread();
    crate::intercept::sleep::ensure_sleep_intercept_for_current_thread();
    THREAD_ID_CSTR.with(|cell| {
        let _ =
            cell.set(CString::new(thread_id.to_string()).unwrap_or_default());
    });
    // After TSRM has bound this thread: the executor-globals accessor resolves
    // to the calling thread, so the pointer the watchdog needs can only be
    // captured from here.
    super::interrupt::bind_current_thread(owner);
    Ok(())
}

/// Declared after process SAPI startup but before the fallible TSRM prologue.
/// Every request-pool and dedicated OS thread releases its own PHP globals.
pub(crate) struct PhpThreadContext;

impl Drop for PhpThreadContext {
    fn drop(&mut self) {
        // Before the context goes away, so the watchdog never holds a pointer
        // into globals that have been released.
        super::interrupt::unbind_current_thread();
        unsafe { ext_php_rs::embed::ext_php_rs_sapi_per_thread_shutdown() };
    }
}
