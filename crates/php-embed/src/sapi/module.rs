//! Process-global Zend module construction, native classes and MINIT startup.

use std::ffi::c_int;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use ext_php_rs::builders::ModuleBuilder;
use ext_php_rs::zend::{ModuleEntry, StaticModuleEntry};
use tracing::error;

use super::{ZEND_RESULT_CODE_FAILURE, ZEND_RESULT_CODE_SUCCESS};
use crate::extension::PhpEmbedExtension;

static MODULE_ENTRY: StaticModuleEntry = StaticModuleEntry::new();
static MODULE_STARTUP: ext_php_rs::internal::ModuleStartupMutex =
    ext_php_rs::internal::MODULE_STARTUP_INIT;

pub(crate) fn prepare_module(
    extensions: &[Arc<dyn PhpEmbedExtension>]
) -> Result<Option<*mut ModuleEntry>, String> {
    let builder =
        ModuleBuilder::new("claviron-host", env!("CARGO_PKG_VERSION"));
    let (mut builder, has_observers) =
        crate::observer::configure_module(builder, extensions)?;
    // Runtime Event is always native. Optional capabilities add
    // their own classes to the same process-global host module.
    let has_native_classes = true;
    if !has_observers && !has_native_classes {
        return Ok(None);
    }

    #[cfg(feature = "cache")]
    {
        builder = builder.class::<crate::functions::cache::PhpCacheHandle>();
    }
    #[cfg(feature = "postgres")]
    {
        builder = builder
            .class::<crate::functions::postgres::PhpPostgresConnection>()
            .class::<crate::functions::postgres::PhpPostgresCursor>();
    }
    builder = builder.class::<crate::event::PhpEventObject>();
    #[cfg(feature = "mysql")]
    {
        builder =
            builder.class::<crate::functions::mysql::PhpMySqlConnection>();
    }
    builder = builder.class::<crate::functions::orbit::cell::PhpOrbital>();
    #[cfg(feature = "tasks")]
    {
        builder = builder
            .class::<crate::functions::orbit::invoke::PhpInvocationObject>();
    }
    for extension in extensions {
        builder = extension.register_classes(builder).map_err(|error| {
            format!(
                "PHP extension '{}' native types: {error}",
                extension.name()
            )
        })?;
    }
    builder = builder.startup_function(host_module_startup);
    let (entry, startup, allocations) = builder
        .try_into()
        .map_err(|err| format!("build PHP host module entry: {err}"))?;
    MODULE_STARTUP.lock().replace(startup);
    Ok(Some(MODULE_ENTRY.get_or_init(|| (entry, allocations))))
}

unsafe extern "C" fn host_module_startup(
    module_type: c_int,
    module_number: c_int,
) -> c_int {
    match catch_unwind(AssertUnwindSafe(|| {
        MODULE_STARTUP.lock().take().map_or(Ok(()), |startup| {
            ext_php_rs::internal::ext_php_rs_startup();
            let result = startup.startup(module_type, module_number);
            if result.is_ok() {
                crate::functions::orbit::cell::install_handlers();
            }
            result
        })
    })) {
        Ok(Ok(())) => ZEND_RESULT_CODE_SUCCESS,
        Ok(Err(err)) => {
            error!(error = %err, "PHP host module startup failed");
            ZEND_RESULT_CODE_FAILURE
        }
        Err(_) => {
            error!("PHP host module startup panicked");
            ZEND_RESULT_CODE_FAILURE
        }
    }
}
