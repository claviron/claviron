//! claviron's SAPI primitives — direct ext-php-rs + libphp.so FFI.
//!
//! Generic free functions parameterised by `<C: ServerContext>` drive
//! the per-request lifecycle and SAPI module construction. No traits
//! with HRTB-bound `From<&Self>` impls and no orphan-rule trickery —
//! claviron is the sole consumer here, so the wiring is direct.
//!
//! The real runtime crate is and remains `ext-php-rs`. The tiny `ffi`
//! submodule below adds extern declarations for PHP C API symbols that
//! ext-php-rs doesn't re-export — `php_request_startup`,
//! `php_request_shutdown`, etc. These are required to drive ZTS PHP
//! per-request without `Embed::run`'s global write-lock that would
//! otherwise serialise concurrent execution.
//!
//! Concrete claviron callbacks (ub_write, register_server_variables,
//! claviron_getenv, etc.) live in [`callbacks`].

use std::ffi::{CStr, NulError, c_char, c_int, c_void};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::time::SystemTime;

use ext_php_rs::builders::SapiBuilder;
use ext_php_rs::embed::{Embed, EmbedError};
use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ModuleEntry, SapiGlobals, SapiModule};
use thiserror::Error;

pub(crate) mod callbacks;
mod module;
#[cfg(test)]
#[path = "../tests/sapi/tests.rs"]
mod tests;

pub use callbacks::init_server_scoped_vars;

// ---------------------------------------------------------------------
// PHP standard return codes. Mirrored locally so callers don't need to
// reach into ext-php-rs's private ffi module.
// ---------------------------------------------------------------------

pub type ZendResultCode = c_int;
pub const ZEND_RESULT_CODE_SUCCESS: ZendResultCode = 0;
pub const ZEND_RESULT_CODE_FAILURE: ZendResultCode = -1;

static ADDITIONAL_MODULE: AtomicPtr<ModuleEntry> =
    AtomicPtr::new(std::ptr::null_mut());

pub(crate) fn install_additional_module(
    module: *mut ModuleEntry
) -> Result<(), String> {
    if module.is_null() {
        return Err("PHP additional module pointer is null".to_string());
    }

    match ADDITIONAL_MODULE.compare_exchange(
        std::ptr::null_mut(),
        module,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => Ok(()),
        Err(existing) if existing == module => Ok(()),
        Err(_) => Err("PHP additional module already installed".to_string()),
    }
}

// ---------------------------------------------------------------------
// Raw FFI to libphp.so. ext-php-rs publicly exposes `Embed::run_script`,
// `SapiBuilder`, `SapiGlobals` etc. but not the per-request lifecycle
// functions — those are needed for ZTS concurrent execution because
// `Embed::run` serialises every PHP call via a global write-lock.
// Symbols resolve via the same libphp.so that ext-php-rs links against.
// ---------------------------------------------------------------------

pub mod ffi {
    use super::*;

    unsafe extern "C" {
        // SAPI-level (process-wide)
        pub fn sapi_startup(sf: *mut SapiModule);

        // Module-level (per-SAPI)
        pub fn php_module_startup(
            sapi: *mut SapiModule,
            additional_modules: *mut c_void,
        ) -> c_int;
        pub fn php_module_shutdown();

        // Request-level (per-request — the reason we vendor)
        pub fn php_request_startup() -> c_int;
        pub fn php_request_shutdown(dummy: *mut c_void);
        pub fn zend_unset_timeout();
        pub fn php_build_argv(
            query: *const c_char,
            vars: *mut Zval,
        );

        // Output buffering
        pub fn php_output_end_all();

        // Authorization header parsing
        pub fn php_handle_auth_data(auth: *const c_char) -> c_int;

        // Track-vars registration ($_SERVER / $_GET / $_POST entries)
        pub fn php_register_variable_safe(
            var: *const c_char,
            str: *const c_char,
            str_len: usize,
            track_vars_array: *mut Zval,
        );

        // Per-thread locale refresh (call after ext_php_rs_sapi_per_thread_init)
        pub fn zend_update_current_locale();

        // Compile and run a PHP snippet in the current execution.
        pub fn zend_eval_string(
            str: *const c_char,
            retval: *mut Zval,
            string_name: *const c_char,
        ) -> c_int;
    }
}

// Public re-exports of the FFI symbols engine.rs uses directly.
pub use ffi::{
    php_handle_auth_data, php_output_end_all, zend_update_current_locale,
};

// ---------------------------------------------------------------------
// $_SERVER variable name constants. Pre-built `&CStr` literals so the
// hot register_variable path doesn't allocate per request.
// ---------------------------------------------------------------------

pub static PHP_SELF: &CStr = c"PHP_SELF";
pub static SERVER_ADDR: &CStr = c"SERVER_ADDR";
pub static SERVER_NAME: &CStr = c"SERVER_NAME";
pub static SERVER_SOFTWARE: &CStr = c"SERVER_SOFTWARE";
pub static SERVER_PROTOCOL: &CStr = c"SERVER_PROTOCOL";
pub static REQUEST_METHOD: &CStr = c"REQUEST_METHOD";
pub static QUERY_STRING: &CStr = c"QUERY_STRING";
pub static DOCUMENT_ROOT: &CStr = c"DOCUMENT_ROOT";
pub static HTTPS: &CStr = c"HTTPS";
pub static REMOTE_ADDR: &CStr = c"REMOTE_ADDR";
pub static REMOTE_PORT: &CStr = c"REMOTE_PORT";
pub static SCRIPT_FILENAME: &CStr = c"SCRIPT_FILENAME";
pub static SERVER_PORT: &CStr = c"SERVER_PORT";
pub static PATH_TRANSLATED: &CStr = c"PATH_TRANSLATED";
pub static SCRIPT_NAME: &CStr = c"SCRIPT_NAME";
pub static REQUEST_URI: &CStr = c"REQUEST_URI";
pub static PATH_INFO: &CStr = c"PATH_INFO";
pub static CLAVIRON_THREAD_ID: &CStr = c"CLAVIRON_THREAD_ID";
pub static HTTP_COOKIE: &CStr = c"HTTP_COOKIE";

// ---------------------------------------------------------------------
// Errors surfaced from `run_request`.
// ---------------------------------------------------------------------

#[derive(Debug, Error, PartialEq)]
pub enum ExecutePhpError {
    #[error(transparent)]
    InitSapiGlobalsError(#[from] NulError),
    #[error("Request startup failed")]
    RequestStartupFailed,
    #[error("A bailout occurred during the execution")]
    Bailout,
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

/// Signal that the response sink has refused further bytes. We set
/// `PG(connection_status) = PHP_CONNECTION_ABORTED` directly, so a
/// PHP script polling `connection_aborted()` in its loop sees the
/// flag at its next iteration and exits naturally.
///
/// Why NOT call PHP's `php_handle_aborted_connection`: that helper
/// also fires `zend_bailout` (longjmp) when `ignore_user_abort` is
/// off. During `php_request_shutdown`, PHP's own
/// `php_output_end_all` calls our `ub_write` callback to flush
/// buffered output. If at that point the downstream sink is dead,
/// firing `zend_bailout` from inside shutdown re-enters PHP's
/// internal bailout machinery and hangs the worker thread —
/// `php_request_shutdown` never returns, the SAPI thread's busy
/// flag never resets, and the pool reports
/// `php pool has no idle thread` from then on.
///
/// Setting the flag directly is enough: `connection_aborted()` reads
/// `PG(connection_status)` and returns true. Streaming PHP scripts
/// (`while (!connection_aborted()) { … }`) exit on the next loop
/// iteration. The cost is up to one PHP `sleep(1)` cycle of
/// additional latency before PHP notices, vs. an immediate longjmp
/// that we cannot use safely.
pub fn handle_abort_connection() {
    // PHP_CONNECTION_ABORTED — defined as 1 in main/php.h. Hard-coded
    // here because the constant isn't re-exported through ext-php-rs's
    // allowed bindings.
    const PHP_CONNECTION_ABORTED: core::ffi::c_short = 1;
    unsafe {
        let pg = ext_php_rs::ffi::ext_php_rs_process_globals();
        if !pg.is_null() {
            (*pg).connection_status = PHP_CONNECTION_ABORTED;
        }
    }
}

/// Register a `$_SERVER`-style variable into the active track-vars Zval.
/// Values are passed by pointer + length so the hot path does not need
/// to allocate a temporary NUL-terminated CString for every field.
///
/// # Safety
/// `vars` must be a valid `*mut Zval` produced by PHP's variable-
/// registration callback site.
pub unsafe fn register_variable<V: AsRef<[u8]>>(
    name: &CStr,
    value: V,
    vars: *mut Zval,
) {
    let value = value.as_ref();
    unsafe {
        ffi::php_register_variable_safe(
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            vars,
        );
    }
}

/// Pre-built `&CStr` variant — reuses cached values without per-call
/// per-call `CString::new`. Use for server-scoped values (hostname,
/// runner, software) that are computed once at SAPI boot and reused
/// across every request.
///
/// # Safety
/// Same contract as [`register_variable`]: `vars` must come from a PHP
/// variable-registration callback.
pub unsafe fn register_variable_cstr(
    name: &CStr,
    value: &CStr,
    vars: *mut Zval,
) {
    let value = value.to_bytes();
    unsafe {
        ffi::php_register_variable_safe(
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            vars,
        );
    }
}

// ---------------------------------------------------------------------
// `request_info` field cleanup macros. PHP's request_info struct holds
// raw cstring pointers we created via `CString::into_raw()`; on
// deactivate they must be reclaimed via `CString::from_raw()` so they
// can drop, then nulled. The trailing `#[allow(unused_assignments)]`
// suppresses dead-store warning at call sites that don't read the slot.
// ---------------------------------------------------------------------

#[macro_export]
macro_rules! free_raw_cstring {
    ($struct:expr, $field:ident) => {
        if !$struct.$field.is_null() {
            unsafe {
                drop(std::ffi::CString::from_raw($struct.$field.cast_mut()))
            };
            #[allow(unused_assignments)]
            {
                $struct.$field = std::ptr::null();
            }
        }
    };
}

#[macro_export]
macro_rules! free_raw_cstring_mut {
    ($struct:expr, $field:ident) => {
        if !$struct.$field.is_null() {
            unsafe { drop(std::ffi::CString::from_raw($struct.$field)) };
            #[allow(unused_assignments)]
            {
                $struct.$field = std::ptr::null_mut();
            }
        }
    };
}

// ---------------------------------------------------------------------
// ServerContext — claviron's per-request state holder. Implementor
// owns the Request<B>/Response state, populates SAPI globals from it,
// supplies request body bytes via `read_post`, and finalises on abort.
// ---------------------------------------------------------------------

pub trait ServerContext: Sized {
    /// Move into a heap-allocated raw pointer, suitable for placing in
    /// `SapiGlobals.server_context`. Caller must reclaim via `from_raw`.
    #[must_use = "losing the pointer leaks memory"]
    fn into_raw(self) -> *mut Self {
        Box::into_raw(Box::new(self))
    }

    /// # Safety
    /// `ptr` must originate from `into_raw` on the same type and not
    /// have been freed.
    #[must_use = "drop the returned Box if you intend to drop the context"]
    unsafe fn from_raw(ptr: *mut c_void) -> Box<Self> {
        unsafe { Box::from_raw(ptr.cast()) }
    }

    /// Borrow without taking ownership — for SAPI callbacks that only
    /// read state (output writer, header sink, cookie reader).
    fn from_server_context<'a>(server_context: *mut c_void) -> &'a mut Self {
        let context = server_context.cast();
        unsafe { &mut *context }
    }

    /// When the request reached the server, if the embedder knows; PHP's
    /// `REQUEST_TIME_FLOAT` then says that instead of when PHP started it.
    fn request_time(&self) -> Option<SystemTime> {
        None
    }

    /// Populate `SapiGlobals.request_info` from this context. Returned
    /// `NulError` surfaces as `ExecutePhpError::InitSapiGlobalsError`.
    fn init_sapi_globals(&mut self) -> Result<(), NulError>;

    /// PHP's `read_post` SAPI hook. Implementor writes up to `to_read`
    /// body bytes into `buffer`; returns the number actually written.
    fn read_post(
        &mut self,
        buffer: *mut c_char,
        to_read: usize,
    ) -> usize;

    /// True once the response has been finalised (status + headers
    /// committed, no further output should be accepted).
    fn is_request_finished(&self) -> bool;

    /// Finalise the response. Returns `true` on success. Called at
    /// deactivate time so output writers can take a last
    /// `php_output_end_all` flush.
    fn finish_request(&mut self) -> bool;
}

// ---------------------------------------------------------------------
// Per-request driver. Free function instead of a trait method — there's
// only one orchestration shape, and parametric dispatch on `C` keeps
// the type-specific deactivate / read_post wired without HRTB tricks.
// ---------------------------------------------------------------------

/// Run a single PHP script in this context: init globals → publish self
/// as `server_context` → `php_request_startup` → execute → shutdown.
/// `bailout_handler` fires if `Embed::run_script` reports an execution
/// error or Zend bailout. Returning `true` treats that signal as a
/// request failure and surfaces `ExecutePhpError::Bailout`; returning
/// `false` treats it as normal PHP control flow such as `exit()`.
pub fn run_request<C, P, F>(
    context: C,
    script: P,
    bailout_handler: F,
) -> Result<(), ExecutePhpError>
where
    C: ServerContext,
    P: AsRef<std::path::Path> + std::fmt::Debug + std::panic::RefUnwindSafe,
    F: Fn(EmbedError) -> bool + std::panic::RefUnwindSafe,
{
    run_execution_with_startup(context, script, bailout_handler, || {})
}

/// Establish one Zend execution scope. PHP calls this lifecycle
/// php_request_startup/shutdown even for a background service: the scope lives
/// for the entire script, and does not imply an HTTP request or per-job reset.
/// The callback acknowledges the native context, not userland app readiness.
pub(crate) fn run_execution_with_startup<C, P, F>(
    context: C,
    script: P,
    bailout_handler: F,
    started: impl FnOnce(),
) -> Result<(), ExecutePhpError>
where
    C: ServerContext,
    P: AsRef<std::path::Path> + std::fmt::Debug + std::panic::RefUnwindSafe,
    F: Fn(EmbedError) -> bool + std::panic::RefUnwindSafe,
{
    let failed = run_execution_scope(context, started, || {
        match Embed::run_script(&script) {
            Ok(()) => false,
            Err(e) => bailout_handler(e),
        }
    })?;
    if failed { Err(ExecutePhpError::Bailout) } else { Ok(()) }
}

/// Caller-owned work in the same native scope as scripts/programs.
/// Zend values must be decoded and dropped before request shutdown; only
/// owned Rust data may leave this scope.
pub(crate) fn run_execution_scope<C, T>(
    mut context: C,
    started: impl FnOnce(),
    execute: impl FnOnce() -> T,
) -> Result<T, ExecutePhpError>
where
    C: ServerContext,
{
    #[cfg(feature = "mysql")]
    crate::functions::mysql::begin_request();
    #[cfg(feature = "sqlite")]
    crate::functions::sqlite::begin_request();
    // Covers early startup failure, bailout, task completion and application
    // exit. Application receive/respond own their narrower message scopes.
    #[cfg(feature = "mysql")]
    let _mysql_scope = crate::functions::mysql::ExecutionCleanup;
    #[cfg(feature = "sqlite")]
    let _sqlite_scope = crate::functions::sqlite::ExecutionCleanup;
    if let Err(err) = context.init_sapi_globals() {
        return Err(ExecutePhpError::from(err));
    }
    SapiGlobals::get_mut().server_context = context.into_raw().cast();

    if unsafe { ffi::php_request_startup() } == ZEND_RESULT_CODE_FAILURE {
        return Err(ExecutePhpError::RequestStartupFailed);
    }

    // Request startup armed PHP's own `max_execution_time` timer from php.ini.
    // The engine budgets every execution itself (`engine::interrupt`), and on
    // a threaded host that timer is not merely redundant: its signal goes to
    // whichever thread the kernel picks, so a daemon dies at the ini value or
    // PHP logs "zend_timeout_handler() called in a thread not managed by PHP".
    // The ini value stays readable in `EG(timeout_seconds)` for the budget.
    unsafe { ffi::zend_unset_timeout() };

    // Per-request intercept hooks: drain peer-broadcast invalidations
    // queued by the bus runnable while no request was in flight. Runs
    // INSIDE the request's PHP context (after `php_request_startup`,
    // before script execution) so apply functions like `apcu_delete`
    // have a valid executor and the script sees a freshly-invalidated
    // APCu.
    crate::intercept::pre_execute_work_hooks();
    started();

    let catch = ext_php_rs::zend::try_catch_first(
        std::panic::AssertUnwindSafe(execute),
    );

    unsafe { ffi::php_request_shutdown(std::ptr::null_mut()) };
    let mut request_info = SapiGlobals::get().request_info;
    crate::free_raw_cstring_mut!(request_info, path_translated);

    catch.map_err(|_| ExecutePhpError::Bailout)
}

// ---------------------------------------------------------------------
// SAPI module construction. One function does the whole "build a
// SapiModule with claviron's defaults, plus your customisation" job —
// no traits, no HRTB, no orphan-rule trickery.
// ---------------------------------------------------------------------

/// Build an `ext_php_rs::SapiModule` ready for `sapi_startup`. The
/// caller supplies:
///   - `name` / `pretty_name`: identifies this SAPI to PHP scripts
///     (`php_sapi_name()` returns `name`).
///   - `log_message`: claviron's syslog-priority → tracing bridge.
///   - `customise`: closure that adds caller-specific callbacks
///     (ub_write, send_header, register_server_variables, etc.) on top
///     of the base SapiBuilder.
///
/// Default lifecycle callbacks are wired here for the given
/// `ServerContext` type: startup/shutdown call `php_module_startup` /
/// `php_module_shutdown`; deactivate cleans request_info cstrings and
/// finalises the per-request `C`; read_post forwards to `C::read_post`
/// after honouring positive `content_length` / `read_post_bytes`
/// bounds. Unknown-length streamed requests are delegated directly to
/// the context and terminate when it returns EOF.
pub fn build_sapi_module<C: ServerContext>(
    name: &str,
    pretty_name: &str,
    log_message: extern "C" fn(*const c_char, c_int),
    customise: impl FnOnce(SapiBuilder) -> SapiBuilder,
) -> ext_php_rs::error::Result<SapiModule> {
    let mut module = customise(SapiBuilder::new(name, pretty_name)).build()?;

    if module.startup.is_none() {
        module.startup = Some(default_startup);
    }
    if module.shutdown.is_none() {
        module.shutdown = Some(default_shutdown);
    }
    if module.deactivate.is_none() {
        module.deactivate = Some(default_deactivate::<C>);
    }
    if module.read_post.is_none() {
        module.read_post = Some(default_read_post::<C>);
    }
    if module.log_message.is_none() {
        module.log_message = Some(log_message);
    }
    if module.get_request_time.is_none() {
        module.get_request_time = Some(default_get_request_time::<C>);
    }

    Ok(module)
}

/// Process-wide `sapi_startup` + the module's own `startup` (which
/// invokes `php_module_startup`). Idempotent at the SAPI layer; callers
/// must guard with `OnceLock` to avoid double-init.
pub fn run_sapi_startup(module: *mut SapiModule) -> ZendResultCode {
    unsafe {
        let ini_entries = (*module).ini_entries;
        ffi::sapi_startup(module);
        (*module).ini_entries = ini_entries;

        if let Some(startup) = (*module).startup {
            return startup(module);
        }
    }
    ZEND_RESULT_CODE_FAILURE
}

// ---------------------------------------------------------------------
// Default callback implementations. Generic over `C: ServerContext` so
// each consumer (today: `engine.rs::ClavironContext`) gets its own
// monomorphised function pointer — no dynamic dispatch.
// ---------------------------------------------------------------------

unsafe extern "C" fn default_startup(sapi: *mut SapiModule) -> ZendResultCode {
    let additional_module = ADDITIONAL_MODULE.load(Ordering::Acquire);
    unsafe { ffi::php_module_startup(sapi, additional_module.cast::<c_void>()) }
}

extern "C" fn default_shutdown(_sapi: *mut SapiModule) -> ZendResultCode {
    unsafe { ffi::php_module_shutdown() };
    ZEND_RESULT_CODE_SUCCESS
}

extern "C" fn default_deactivate<C: ServerContext>() -> ZendResultCode {
    let sapi_globals = SapiGlobals::get();
    if !sapi_globals.sapi_started {
        return ZEND_RESULT_CODE_SUCCESS;
    }
    if sapi_globals.server_context.is_null() {
        return ZEND_RESULT_CODE_SUCCESS;
    }

    let mut request_info = sapi_globals.request_info;
    crate::free_raw_cstring!(request_info, request_method);
    crate::free_raw_cstring_mut!(request_info, query_string);
    crate::free_raw_cstring_mut!(request_info, request_uri);
    crate::free_raw_cstring!(request_info, content_type);
    crate::free_raw_cstring_mut!(request_info, cookie_data);

    let mut context = unsafe { C::from_raw(sapi_globals.server_context) };
    drop(sapi_globals);
    if !context.is_request_finished() && !context.finish_request() {
        handle_abort_connection();
    }
    SapiGlobals::get_mut().server_context = std::ptr::null_mut();

    ZEND_RESULT_CODE_SUCCESS
}

extern "C" fn default_read_post<C: ServerContext>(
    buffer: *mut c_char,
    length: usize,
) -> usize {
    let sapi_globals = SapiGlobals::get();
    let content_length = sapi_globals.request_info().content_length();
    if content_length <= 0 {
        return C::from_server_context(sapi_globals.server_context)
            .read_post(buffer, length);
    }
    if sapi_globals.read_post_bytes >= content_length {
        return 0;
    }
    let to_read =
        length.min((content_length - sapi_globals.read_post_bytes) as usize);
    C::from_server_context(sapi_globals.server_context)
        .read_post(buffer, to_read)
}

unsafe extern "C" fn default_get_request_time<C: ServerContext>(
    time: *mut f64
) -> ZendResultCode {
    let server_context = SapiGlobals::get().server_context;
    let received = (!server_context.is_null())
        .then(|| C::from_server_context(server_context).request_time())
        .flatten();
    let timestamp = received
        .unwrap_or_else(SystemTime::now)
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time is before Unix epoch");
    unsafe { time.write(timestamp.as_secs_f64()) };
    ZEND_RESULT_CODE_SUCCESS
}
