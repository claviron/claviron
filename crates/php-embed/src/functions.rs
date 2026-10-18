//! Claviron-owned PHP-callable functions registered into PHP's function
//! table. Different category from [`crate::intercept`] — these are NEW
//! functions claviron adds to the PHP runtime, not replacement handlers
//! for existing ones.
//!
//! Registration travels through PHP's normal startup machinery via
//! `sapi_module.additional_functions` ([`build_function_entries`]) so
//! every ZTS thread observes the same function. A per-thread re-check
//! ([`ensure_functions_for_current_thread`]) covers the case where a
//! later pool thread gets a fresh `CG(function_table)` and would
//! otherwise miss claviron's built-ins.

use std::ffi::{c_char, c_int, c_void};

use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::flags::DataType;
use ext_php_rs::zend::FunctionEntry;

use crate::extension::FunctionEntries;
use crate::sapi::ZEND_RESULT_CODE_SUCCESS;

#[cfg(feature = "cache")]
pub(crate) mod cache;

mod application;
#[cfg(feature = "auth")]
pub(crate) mod auth;
mod budget;
pub(crate) mod cancellation;
pub(crate) mod config;
mod event;
#[cfg(feature = "log-pipeline")]
pub(crate) mod log;
#[cfg(feature = "mysql")]
pub(crate) mod mysql;
pub(crate) mod orbit;
#[cfg(feature = "postgres")]
pub(crate) mod postgres;
pub(crate) mod runtime;
pub(crate) mod sapi_stats;
#[cfg(feature = "tasks")]
mod task;
#[cfg(feature = "tasks")]
mod task_output;

/// Build claviron-specific PHP functions for `sapi_module.additional_functions`.
///
/// The function entries and their argument tables are retained together: PHP's
/// startup stores their `fname` / `arg_info` pointers verbatim in the
/// function table and reads them for the lifetime of the worker. A
/// process-lifetime `FunctionEntries` owner gives them a `'static`
/// lifetime; on shutdown the OS reclaims the memory.
pub(crate) fn build_function_entries() -> Result<*const FunctionEntry, String> {
    let mut entries = FunctionEntries::default();

    entries.push(
        FunctionBuilder::new("claviron_runtime_available", runtime::available)
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|err| format!("claviron_runtime_available: {err}"))?,
    );
    entries.push(
        FunctionBuilder::new(
            "claviron_runtime_configured",
            runtime::configured,
        )
        .returns(DataType::Bool, false, false)
        .build()
        .map_err(|err| format!("claviron_runtime_configured: {err}"))?,
    );
    entries.push(
        FunctionBuilder::new("claviron_app_name", runtime::app_name)
            .returns(DataType::String, false, false)
            .build()
            .map_err(|err| format!("claviron_app_name: {err}"))?,
    );
    entries.push(
        FunctionBuilder::new("claviron_environment", runtime::environment)
            .returns(DataType::String, false, false)
            .build()
            .map_err(|err| format!("claviron_environment: {err}"))?,
    );
    entries.push(
        FunctionBuilder::new(
            "claviron_application_name",
            runtime::application_name,
        )
        .returns(DataType::String, true, false)
        .build()
        .map_err(|err| format!("claviron_application_name: {err}"))?,
    );

    let cancellation = match FunctionBuilder::new(
        "claviron_is_cancelled",
        cancellation::claviron_is_cancelled_handler,
    )
    .returns(DataType::Bool, false, false)
    .build()
    {
        Ok(e) => e,
        Err(err) => return Err(format!("claviron_is_cancelled: {err}")),
    };
    entries.push(cancellation);

    let on_http = match FunctionBuilder::new(
        "claviron_is_on_http",
        cancellation::claviron_is_on_http_handler,
    )
    .returns(DataType::Bool, false, false)
    .build()
    {
        Ok(e) => e,
        Err(err) => return Err(format!("claviron_is_on_http: {err}")),
    };
    entries.push(on_http);

    budget::append_function_entries(&mut entries)?;
    config::append_function_entries(&mut entries)?;
    application::append_function_entries(&mut entries)?;
    #[cfg(feature = "auth")]
    auth::append_function_entries(&mut entries)?;
    event::append_function_entries(&mut entries)?;
    #[cfg(feature = "log-pipeline")]
    log::append_function_entries(&mut entries)?;
    #[cfg(feature = "tasks")]
    orbit::invoke::append_function_entries(&mut entries)?;
    #[cfg(feature = "tasks")]
    task::append_function_entries(&mut entries)?;
    #[cfg(feature = "tasks")]
    task_output::append_function_entries(&mut entries)?;

    sapi_stats::append_function_entries(&mut entries)?;

    #[cfg(feature = "cache")]
    cache::append_function_entries(&mut entries)?;

    #[cfg(feature = "postgres")]
    postgres::append_function_entries(&mut entries)?;
    #[cfg(feature = "mysql")]
    mysql::append_function_entries(&mut entries)?;

    orbit::counter::append_function_entries(&mut entries)?;

    orbit::lock::append_function_entries(&mut entries)?;

    for extension in crate::extension::extensions_snapshot() {
        extension
            .append_functions(&mut entries)
            .map_err(|err| format!("{}: {err}", extension.name()))?;
    }

    Ok(entries.into_raw())
}

/// Ensure claviron's host functions exist in the current ZTS thread.
///
/// `sapi_module.additional_functions` is the canonical process-startup
/// path. This per-thread guard exists because ZTS compiler globals can
/// be thread-local: if a later PHP execution thread gets a fresh
/// function table, it must not miss claviron's built-ins.
pub(crate) fn ensure_functions_for_current_thread() -> Result<(), String> {
    use ext_php_rs::ffi::{
        HashTable, zend_function_entry, zend_hash_str_find_ptr_lc,
    };

    type ZendRegisterFunctions = unsafe extern "C" fn(
        scope: *mut c_void,
        functions: *const zend_function_entry,
        function_table: *mut HashTable,
        type_: c_int,
    ) -> c_int;

    const MODULE_PERSISTENT: c_int = 1;
    const FN_NAME: &[u8] = b"claviron_is_cancelled";

    unsafe extern "C" {
        fn zend_register_functions(
            scope: *mut c_void,
            functions: *const zend_function_entry,
            function_table: *mut HashTable,
            type_: c_int,
        ) -> c_int;
    }
    let _: ZendRegisterFunctions = zend_register_functions;

    unsafe {
        let cg = ext_php_rs::ffi::ext_php_rs_compiler_globals();
        if cg.is_null() {
            return Err("compiler globals null".to_string());
        }
        let function_table_ptr = (*cg).function_table;
        if function_table_ptr.is_null() {
            return Err("CG(function_table) is null".to_string());
        }

        let existing = zend_hash_str_find_ptr_lc(
            function_table_ptr,
            FN_NAME.as_ptr() as *const c_char,
            FN_NAME.len(),
        );
        if existing.is_null() {
            let entries = build_function_entries()?;
            let rc = zend_register_functions(
                std::ptr::null_mut(),
                entries.cast::<zend_function_entry>(),
                function_table_ptr,
                MODULE_PERSISTENT,
            );
            if rc != ZEND_RESULT_CODE_SUCCESS {
                return Err(format!("zend_register_functions returned {rc}"));
            }
        }
    }

    for extension in crate::extension::extensions_snapshot() {
        if let Err(err) = extension.ensure_for_current_thread() {
            tracing::warn!(
                extension = extension.name(),
                error = %err,
                "PHP embed extension per-thread ensure failed"
            );
            return Err(format!("{}: {err}", extension.name()));
        }
    }

    Ok(())
}
