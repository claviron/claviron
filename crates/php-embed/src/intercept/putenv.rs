//! `putenv` intercept — routes PHP-level env mutations into the
//! per-request overlay on `ClavironContext`, never libc `environ`.
//!
//! Mirrors PHP-FPM's default `clean_env=yes` semantics: env writes
//! made by a request stay confined to that request. No cross-vhost
//! leakage, no carry-over across requests.

use std::ffi::c_char;

use ext_php_rs::args::Arg;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, SapiGlobals};
use tracing::warn;

use crate::engine::ClavironContext;
use crate::sapi::ServerContext;

/// Replacement for PHP's built-in `putenv()` — writes to the
/// per-request overlay on `ClavironContext`, NOT to libc `environ`.
///
/// Forms supported:
///   * `putenv("KEY=value")` → overlay\[KEY\] = value
///   * `putenv("KEY")`        → overlay.remove(KEY) (PHP's "unset")
///
/// Lifetime: overlay is created empty at request entry and dropped
/// when the request finishes. Mirrors PHP-FPM's default
/// `clean_env=yes` behavior — env mutations stay confined to the
/// request that made them, no carry-over across requests, no
/// cross-vhost leakage. Anti-pattern of "set once, reuse next
/// request" is intentionally not supported; persistent values belong
/// in `vhost.yaml::environment` or `env_file`.
///
/// Returns true on success, false otherwise (matches PHP's putenv).
pub(crate) extern "C" fn claviron_putenv_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    let mut setting = Arg::new("setting", DataType::String);

    if ex.parser().arg(&mut setting).parse().is_err() {
        retval.set_bool(false);
        return;
    }

    let Some(setting_str) = setting.val::<String>() else {
        retval.set_bool(false);
        return;
    };

    let sapi_globals = SapiGlobals::get();
    if sapi_globals.server_context.is_null() {
        retval.set_bool(false);
        return;
    }
    let context =
        ClavironContext::from_server_context(sapi_globals.server_context);

    let Ok(mut overlay) = context.env_overlay.lock() else {
        retval.set_bool(false);
        return;
    };

    match setting_str.split_once('=') {
        Some((key, value)) => {
            overlay.insert(key.to_string(), value.to_string());
        }
        None => {
            overlay.remove(setting_str.as_str());
        }
    }

    retval.set_bool(true);
}

/// Swap PHP's built-in `putenv` function handler in
/// `CG(function_table)` with [`claviron_putenv_handler`]. Called once
/// per worker right after `php_module_startup`, which is the call
/// that registers built-in functions into compiler globals.
///
/// We target `CG(function_table)` rather than `EG(function_table)`
/// because module-level function registrations land in the compiler
/// globals; the executor globals copy is populated lazily per
/// request, so reading EG immediately after module startup returns a
/// null pointer. ext-php-rs exposes both via `CompilerGlobals` /
/// `ExecutorGlobals` — `CG.function_table` is the canonical source.
///
/// If the lookup fails (e.g. PHP build without `putenv` registered)
/// the function logs a warning and returns; PHP's stock `putenv`
/// stays active. Even then env scrub keeps libc clean — the override
/// is the second layer that captures putenv writes into the
/// per-request overlay.
pub(crate) fn override_putenv_handler() {
    use ext_php_rs::ffi::{_zend_function, zend_hash_str_find_ptr_lc};

    type FunctionPointerHandler =
        extern "C" fn(execute_data: *mut ExecuteData, retval: *mut Zval);
    type RawFunctionPointerHandler = unsafe extern "C" fn(
        execute_data: *mut ext_php_rs::ffi::_zend_execute_data,
        retval: *mut ext_php_rs::ffi::_zval_struct,
    );

    // SAFETY: called from `ensure_sapi` under `OnceLock`, after
    // `php_module_startup` has populated `CG(function_table)`. The
    // compiler globals pointer is alive for the lifetime of the SAPI
    // module (= worker process). `zend_hash_str_find_ptr_lc` returns
    // the raw `zend_function*` stored under "putenv"; we mutate its
    // `internal_function.handler` field. PHP only reads this field
    // when invoking the function, which can only happen from a
    // request thread — but the dedicated PHP thread isn't running
    // any request at this point (we're still on the bootstrap
    // synchronous path), so there is no concurrent reader.
    //
    // CG (compiler globals) is the canonical home of internal
    // function entries; EG (executor globals) is a per-request copy.
    // ext-php-rs doesn't re-export `CompilerGlobals` from the `zend`
    // facade so we go through the raw FFI accessor.
    unsafe {
        let cg = ext_php_rs::ffi::ext_php_rs_compiler_globals();
        if cg.is_null() {
            warn!("override_putenv_handler: compiler globals null");
            return;
        }
        let function_table_ptr = (*cg).function_table;
        if function_table_ptr.is_null() {
            warn!("override_putenv_handler: CG(function_table) is null");
            return;
        }

        let key: &[u8] = b"putenv";
        let func_ptr = zend_hash_str_find_ptr_lc(
            function_table_ptr,
            key.as_ptr() as *const c_char,
            key.len(),
        ) as *mut _zend_function;

        if func_ptr.is_null() {
            warn!(
                "override_putenv_handler: 'putenv' not in CG(function_table); skipping override"
            );
            return;
        }

        let new_handler: FunctionPointerHandler =
            std::mem::transmute(claviron_putenv_handler as extern "C" fn(_, _));
        (*func_ptr).internal_function.handler = Some(std::mem::transmute::<
            FunctionPointerHandler,
            RawFunctionPointerHandler,
        >(new_handler));

        #[cfg(debug_assertions)]
        tracing::trace!(
            "override_putenv_handler: putenv handler swapped to claviron overlay"
        );
    }
}
