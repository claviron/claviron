//! Extension registry for Rust code compiled into the Claviron binary.
//!
//! This is not PHP's dynamic `.so` extension loader. It is a lifecycle
//! bridge for code that is already linked into the running Claviron
//! process: app/user crates can describe PHP functions, native classes and per-thread
//! setup work, and the embed SAPI registers them at the same points as
//! Claviron's own host functions.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

mod functions;

use claviron_runtime::AppState;
pub use ext_php_rs::builders::{EnumBuilder, FunctionBuilder, ModuleBuilder};
pub use ext_php_rs::class::RegisteredClass;
pub use ext_php_rs::enum_::{Discriminant, EnumCase, RegisteredEnum};
pub use ext_php_rs::flags::DataType;
pub use ext_php_rs::types::Zval;
pub use ext_php_rs::zend::{ExecuteData, FunctionEntry};
pub use ext_php_rs::{self, php_class, php_enum, php_impl};
pub use functions::FunctionEntries;

pub use crate::observer::{
    BacktraceFrame, ErrorInfo, ErrorType, ExceptionInfo, FcallInfo,
    PhpObserver, PhpObserverInterests,
};

/// A PHP extension compiled into the Claviron binary.
///
/// `configure` runs during application registration, before the PHP
/// SAPI provider boots. Use it to copy immutable runtime identity or
/// config into the extension. Function/class presence must not depend
/// on provider boot order.
///
/// `append_functions` receives a [`FunctionEntries`] owner. Push each complete
/// `FunctionBuilder::build()` result so its argument metadata stays alive.
/// Hand-written handlers must contain panics with `ext_php_rs::zend::run_handler`.
///
/// `append_functions` feeds `sapi_module.additional_functions` during
/// PHP module startup. `ensure_for_current_thread` runs after each ZTS
/// SAPI thread binds its PHP context. `register_classes` contributes native
/// classes/enums to the same internal host module during MINIT. Add declarations
/// with `module.class::<T>()` / `module.enumeration::<T>()`; do not replace module
/// lifecycle callbacks or register classes from a request/per-thread hook.
///
/// `observer` optionally contributes a process-global observer sink. Claviron
/// snapshots all sinks before PHP module startup and fans Zend callbacks out to
/// them. Downstream code must not install a separate Zend observer directly.
pub trait PhpEmbedExtension: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    fn configure(
        &self,
        _state: &AppState,
    ) -> Result<(), String> {
        Ok(())
    }

    fn append_functions(
        &self,
        _entries: &mut FunctionEntries,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Contribute compiled-in types before PHP starts. The module retains their
    /// metadata for its process lifetime, shared by all PHP execution models.
    fn register_classes<'a>(
        &self,
        module: ModuleBuilder<'a>,
    ) -> Result<ModuleBuilder<'a>, String> {
        Ok(module)
    }

    fn ensure_for_current_thread(&self) -> Result<(), String> {
        Ok(())
    }

    fn observer(&self) -> Option<Arc<dyn PhpObserver>> {
        None
    }
}

static EXTENSIONS: OnceLock<RwLock<Vec<Arc<dyn PhpEmbedExtension>>>> =
    OnceLock::new();
static EXTENSIONS_FROZEN: AtomicBool = AtomicBool::new(false);

fn registry() -> &'static RwLock<Vec<Arc<dyn PhpEmbedExtension>>> {
    EXTENSIONS.get_or_init(|| RwLock::new(Vec::new()))
}

/// Register an app/user extension with the embed SAPI.
///
/// Call this during the application's bootstrap registration, before
/// the PHP SAPI provider boots. The registry configures the extension
/// with the current `AppState` first, then publishes it for SAPI
/// startup. Duplicate names are ignored so master and worker bootstrap
/// paths can safely call the same registration code in their own
/// processes.
pub fn register_extension(
    state: &AppState,
    extension: Arc<dyn PhpEmbedExtension>,
) -> Result<(), String> {
    if EXTENSIONS_FROZEN.load(Ordering::Acquire) {
        return Err("PHP embed extension registry already frozen".to_string());
    }
    extension.configure(state)?;

    let mut guard =
        registry().write().map_err(|_| "PHP extension registry poisoned")?;
    if EXTENSIONS_FROZEN.load(Ordering::Acquire) {
        return Err("PHP embed extension registry already frozen".to_string());
    }
    if guard.iter().any(|registered| registered.name() == extension.name()) {
        return Ok(());
    }
    guard.push(extension);
    Ok(())
}

pub(crate) fn extensions_snapshot() -> Vec<Arc<dyn PhpEmbedExtension>> {
    registry().read().map(|guard| guard.clone()).unwrap_or_default()
}

pub(crate) fn freeze_extensions()
-> Result<Vec<Arc<dyn PhpEmbedExtension>>, String> {
    let guard =
        registry().write().map_err(|_| "PHP extension registry poisoned")?;
    EXTENSIONS_FROZEN.store(true, Ordering::Release);
    Ok(guard.clone())
}

#[cfg(test)]
#[path = "../tests/extension.rs"]
mod tests;
