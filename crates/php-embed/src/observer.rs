//! Public observer API and Zend callback fan-out for compiled-in extensions.
//!
//! The SAPI module owns native module/class registration. This module attaches
//! a frozen observer snapshot to its builder and isolates panicking sinks so
//! unwinding never crosses Zend's FFI boundary.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use ext_php_rs::builders::ModuleBuilder;
pub use ext_php_rs::observer::{
    BacktraceFrame, ErrorInfo, ErrorType, ExceptionInfo, FcallInfo,
};
use ext_php_rs::observer::{ErrorObserver, ExceptionObserver, FcallObserver};
use ext_php_rs::types::Zval;
use ext_php_rs::zend::ExecuteData;
use tracing::error;

use crate::extension::PhpEmbedExtension;

/// Zend callback families requested by a downstream observer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PhpObserverInterests {
    pub fcalls: bool,
    pub errors: bool,
    pub exceptions: bool,
}

impl PhpObserverInterests {
    #[must_use]
    pub const fn all() -> Self {
        Self { fcalls: true, errors: true, exceptions: true }
    }

    #[must_use]
    pub const fn errors() -> Self {
        Self { fcalls: false, errors: true, exceptions: false }
    }

    #[must_use]
    pub const fn exceptions() -> Self {
        Self { fcalls: false, errors: false, exceptions: true }
    }

    #[must_use]
    pub const fn fcalls() -> Self {
        Self { fcalls: true, errors: false, exceptions: false }
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        !self.fcalls && !self.errors && !self.exceptions
    }
}

/// A downstream observer hosted by Claviron's single Zend host module.
///
/// Implementations are process-global and may be called concurrently by every
/// SAPI thread. Keep callbacks lightweight, avoid blocking I/O, and hold only
/// thread-safe state. Filtering methods run both when Zend discovers an event
/// and immediately before per-sink delivery, so they must be cheap and free of
/// side effects.
pub trait PhpObserver: Send + Sync + 'static {
    fn interests(&self) -> PhpObserverInterests;

    fn should_observe_fcall(
        &self,
        _info: &FcallInfo<'_>,
    ) -> bool {
        self.interests().fcalls
    }

    fn on_fcall_begin(
        &self,
        _info: &FcallInfo<'_>,
        _execute_data: &ExecuteData,
    ) {
    }

    fn on_fcall_end(
        &self,
        _info: &FcallInfo<'_>,
        _execute_data: &ExecuteData,
        _retval: Option<&Zval>,
    ) {
    }

    fn should_observe_error(
        &self,
        _error_type: ErrorType,
    ) -> bool {
        self.interests().errors
    }

    fn on_error(
        &self,
        _error: &ErrorInfo<'_>,
    ) {
    }

    fn on_exception(
        &self,
        _exception: &ExceptionInfo,
    ) {
    }
}

struct ObserverSlot {
    extension: &'static str,
    sink: Arc<dyn PhpObserver>,
    enabled: AtomicBool,
}

impl ObserverSlot {
    fn new(
        extension: &'static str,
        sink: Arc<dyn PhpObserver>,
    ) -> Self {
        Self { extension, sink, enabled: AtomicBool::new(true) }
    }

    fn invoke<T>(
        &self,
        callback: &'static str,
        invoke: impl FnOnce(&dyn PhpObserver) -> T,
    ) -> Option<T> {
        if !self.enabled.load(Ordering::Acquire) {
            return None;
        }

        match catch_unwind(AssertUnwindSafe(|| invoke(self.sink.as_ref()))) {
            Ok(value) => Some(value),
            Err(_) => {
                self.enabled.store(false, Ordering::Release);
                error!(
                    extension = self.extension,
                    callback,
                    "PHP observer panicked; disabling downstream observer"
                );
                None
            }
        }
    }
}

#[derive(Default)]
struct ObserverSnapshot {
    slots: Vec<Arc<ObserverSlot>>,
}

impl ObserverSnapshot {
    fn from_extensions(extensions: &[Arc<dyn PhpEmbedExtension>]) -> Self {
        let slots = extensions
            .iter()
            .filter_map(|extension| {
                extension.observer().and_then(|sink| {
                    (!sink.interests().is_empty()).then(|| {
                        Arc::new(ObserverSlot::new(extension.name(), sink))
                    })
                })
            })
            .collect();
        Self { slots }
    }

    fn interested(
        &self,
        select: impl Fn(PhpObserverInterests) -> bool,
    ) -> Vec<Arc<ObserverSlot>> {
        self.slots
            .iter()
            .filter(|slot| select(slot.sink.interests()))
            .cloned()
            .collect()
    }
}

static OBSERVERS: OnceLock<ObserverSnapshot> = OnceLock::new();

struct FcallDispatcher {
    slots: Vec<Arc<ObserverSlot>>,
}

impl FcallDispatcher {
    fn new() -> Self {
        Self {
            slots: OBSERVERS
                .get()
                .map(|snapshot| snapshot.interested(|interest| interest.fcalls))
                .unwrap_or_default(),
        }
    }
}

impl FcallObserver for FcallDispatcher {
    fn should_observe(
        &self,
        info: &FcallInfo<'_>,
    ) -> bool {
        self.slots.iter().any(|slot| {
            slot.invoke("should_observe_fcall", |sink| {
                sink.should_observe_fcall(info)
            })
            .unwrap_or(false)
        })
    }

    fn begin(
        &self,
        execute_data: &ExecuteData,
    ) {
        let info = unsafe { fcall_info(execute_data) };
        for slot in &self.slots {
            let _ = slot.invoke("on_fcall_begin", |sink| {
                if sink.should_observe_fcall(&info) {
                    sink.on_fcall_begin(&info, execute_data);
                }
            });
        }
    }

    fn end(
        &self,
        execute_data: &ExecuteData,
        retval: Option<&Zval>,
    ) {
        let info = unsafe { fcall_info(execute_data) };
        for slot in &self.slots {
            let _ = slot.invoke("on_fcall_end", |sink| {
                if sink.should_observe_fcall(&info) {
                    sink.on_fcall_end(&info, execute_data, retval);
                }
            });
        }
    }
}

struct ErrorDispatcher {
    slots: Vec<Arc<ObserverSlot>>,
}

impl ErrorDispatcher {
    fn new() -> Self {
        Self {
            slots: OBSERVERS
                .get()
                .map(|snapshot| snapshot.interested(|interest| interest.errors))
                .unwrap_or_default(),
        }
    }
}

impl ErrorObserver for ErrorDispatcher {
    fn should_observe(
        &self,
        error_type: ErrorType,
    ) -> bool {
        self.slots.iter().any(|slot| {
            slot.invoke("should_observe_error", |sink| {
                sink.should_observe_error(error_type)
            })
            .unwrap_or(false)
        })
    }

    fn on_error(
        &self,
        info: &ErrorInfo<'_>,
    ) {
        for slot in &self.slots {
            let _ = slot.invoke("on_error", |sink| {
                if sink.should_observe_error(info.error_type) {
                    sink.on_error(info);
                }
            });
        }
    }
}

struct ExceptionDispatcher {
    slots: Vec<Arc<ObserverSlot>>,
}

impl ExceptionDispatcher {
    fn new() -> Self {
        Self {
            slots: OBSERVERS
                .get()
                .map(|snapshot| {
                    snapshot.interested(|interest| interest.exceptions)
                })
                .unwrap_or_default(),
        }
    }
}

impl ExceptionObserver for ExceptionDispatcher {
    fn on_exception(
        &self,
        info: &ExceptionInfo,
    ) {
        for slot in &self.slots {
            let _ = slot.invoke("on_exception", |sink| sink.on_exception(info));
        }
    }
}

/// Attach the frozen observer snapshot to the SAPI-owned Zend module builder.
pub(crate) fn configure_module<'a>(
    mut builder: ModuleBuilder<'a>,
    extensions: &[Arc<dyn PhpEmbedExtension>],
) -> Result<(ModuleBuilder<'a>, bool), String> {
    let snapshot = ObserverSnapshot::from_extensions(extensions);
    let has_fcalls =
        snapshot.slots.iter().any(|slot| slot.sink.interests().fcalls);
    let has_errors =
        snapshot.slots.iter().any(|slot| slot.sink.interests().errors);
    let has_exceptions =
        snapshot.slots.iter().any(|slot| slot.sink.interests().exceptions);
    let has_observers = has_fcalls || has_errors || has_exceptions;

    if has_observers {
        OBSERVERS.set(snapshot).map_err(|_| {
            "PHP observer snapshot already prepared".to_string()
        })?;
    }

    if has_fcalls {
        builder = builder.fcall_observer(FcallDispatcher::new);
    }
    if has_errors {
        builder = builder.error_observer(ErrorDispatcher::new);
    }
    if has_exceptions {
        builder = builder.exception_observer(ExceptionDispatcher::new);
    }
    Ok((builder, has_observers))
}

/// Build the metadata view for a function currently being executed.
///
/// # Safety
///
/// `execute_data` and its Zend metadata must remain valid for the returned
/// borrow. Zend guarantees this for the duration of an observer callback.
unsafe fn fcall_info(execute_data: &ExecuteData) -> FcallInfo<'_> {
    let func = execute_data.func;
    if func.is_null() {
        return FcallInfo {
            function_name: None,
            class_name: None,
            filename: None,
            lineno: 0,
            is_internal: false,
        };
    }

    let common = unsafe { &(*func).common };
    #[allow(clippy::cast_possible_truncation)]
    let is_internal =
        common.type_ == ext_php_rs::ffi::ZEND_INTERNAL_FUNCTION as u8;
    let function_name = unsafe { zend_string_to_str(common.function_name) };
    let class_name = if common.scope.is_null() {
        None
    } else {
        unsafe { zend_string_to_str((*common.scope).name) }
    };
    let (filename, lineno) = if is_internal {
        (None, 0)
    } else {
        let op_array = unsafe { &(*func).op_array };
        (unsafe { zend_string_to_str(op_array.filename) }, op_array.line_start)
    };

    FcallInfo { function_name, class_name, filename, lineno, is_internal }
}

/// # Safety
///
/// `value` must be null or point to a live Zend string for the returned borrow.
unsafe fn zend_string_to_str<'a>(
    value: *mut ext_php_rs::ffi::zend_string
) -> Option<&'a str> {
    if value.is_null() {
        return None;
    }
    let len = unsafe { (*value).len };
    let ptr = unsafe { (*value).val.as_ptr() };
    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
    std::str::from_utf8(bytes).ok()
}

#[cfg(test)]
#[path = "../tests/observer.rs"]
mod tests;
