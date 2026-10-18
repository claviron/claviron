//! claviron's concrete SAPI callbacks — `ub_write`, `flush`,
//! `send_header(s)`, `read_cookies`, `log_message`, `claviron_getenv`,
//! `register_server_variables` — plus the [`ClavironSapi`] module
//! wrapper that ties them onto a `SapiBuilder`.
//!
//! Generic harness in [`super`]; concrete implementations here. Each
//! callback reads/writes through `ClavironContext` (in
//! [`crate::engine`]) which carries the per-request state (runtime,
//! parts, body, output sink, env overlay).

use std::cell::Cell;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::sync::OnceLock;

use claviron_http::ext::{HostName, TlsInfo};
use claviron_runtime::{AppState, RunMode};
use ext_php_rs::types::Zval;
use ext_php_rs::zend::{SapiGlobals, SapiHeader, SapiHeaders, SapiModule};
use http::Version;
use http::header::{CONTENT_TYPE, HeaderName};
use http::status::StatusCode;
use regex::Regex;
use tracing::{debug, error, info, warn};

use super::{
    CLAVIRON_THREAD_ID, DOCUMENT_ROOT, HTTP_COOKIE, HTTPS, PATH_INFO,
    PATH_TRANSLATED, PHP_SELF, QUERY_STRING, REMOTE_ADDR, REMOTE_PORT,
    REQUEST_METHOD, REQUEST_URI, SCRIPT_FILENAME, SCRIPT_NAME, SERVER_ADDR,
    SERVER_NAME, SERVER_PORT, SERVER_PROTOCOL, SERVER_SOFTWARE, ServerContext,
    build_sapi_module, install_additional_module, register_variable,
    register_variable_cstr,
};
use crate::engine::ClavironContext;
use crate::error::SapiGatewayError;

// ---------------------------------------------------------------------
// Per-request error flag — flipped in `log_message` whenever PHP emits
// a syslog priority ≤ 3 (error/critical/alert/emergency). Read by
// `engine::run_php_on_thread` to decide whether a clean `exit()` /
// `die()` should still be classified as a request failure.
// ---------------------------------------------------------------------

thread_local! {
    pub(crate) static PHP_REQUEST_LOGGED_ERROR: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn reset_php_request_error_flag() {
    PHP_REQUEST_LOGGED_ERROR.with(|flag| flag.set(false));
}

pub(crate) fn php_request_logged_error() -> bool {
    PHP_REQUEST_LOGGED_ERROR.with(Cell::get)
}

const PHP_LOG_MAX_SIZE: usize = 16 * 1024;

pub(super) fn sanitize_log_message(message: &str) -> String {
    static HTML_DOCUMENT: OnceLock<Regex> = OnceLock::new();
    let regex = HTML_DOCUMENT.get_or_init(|| {
        Regex::new(r"(?is)<html(?:\s[^>]*)?>.*?</html\s*>")
            .expect("the PHP log HTML filter must compile")
    });
    let message = regex.replace_all(message, "");
    let message = message.trim();
    if message.len() <= PHP_LOG_MAX_SIZE {
        return message.to_owned();
    }

    let mut end = PHP_LOG_MAX_SIZE;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{} [truncated]", &message[..end])
}

// ---------------------------------------------------------------------
// Server-scoped, boot-time cached CStrings. Populated once from
// `AppState` at `PhpSapiService::boot` and read pointer-by-pointer in
// every `register_server_variables` call thereafter — no per-request
// `CString::new` for these values. claviron-php-fcgi can't cache these
// (FCGI runs in FPM, foreign process, no `AppState` access — server
// identity travels through PhpRuntime), but embed sits inside claviron
// so the canonical state IS available at boot.
// ---------------------------------------------------------------------

struct ServerScopedCstrs {
    hostname: CString,
    runner: CString,
    worker_id: CString,
    worker_count: CString,
    run_mode: RunMode,
    process_id: CString,
    software: CString,
}

static SERVER_SCOPED_VARS: OnceLock<ServerScopedCstrs> = OnceLock::new();

pub fn init_server_scoped_vars(state: &AppState) {
    let _ = SERVER_SCOPED_VARS.set(ServerScopedCstrs {
        hostname: CString::new(state.hostname().to_string())
            .unwrap_or_default(),
        runner: CString::new(run_identifier(state.run_mode()))
            .unwrap_or_default(),
        worker_id: CString::new(state.worker_id().unwrap_or(0).to_string())
            .unwrap_or_default(),
        worker_count: CString::new(state.worker_count().to_string())
            .unwrap_or_default(),
        run_mode: state.run_mode(),
        process_id: CString::new(std::process::id().to_string())
            .unwrap_or_default(),
        software: CString::new(state.name().to_string()).unwrap_or_default(),
    });
}

/// One identity vocabulary for HTTP and background PHP. This is execution
/// metadata, not a TaskRuntime and not a process-environment export.
fn visit_execution_identity(
    context: &ClavironContext,
    mut visit: impl FnMut(&CStr, &str),
) {
    if let Some(scoped) = SERVER_SCOPED_VARS.get() {
        visit(c"CLAVIRON_HOSTNAME", &scoped.hostname.to_string_lossy());
        visit(c"CLAVIRON_RUNNER", &scoped.runner.to_string_lossy());
        visit(c"CLAVIRON_WORKER_ID", &scoped.worker_id.to_string_lossy());
        visit(c"CLAVIRON_WORKER_COUNT", &scoped.worker_count.to_string_lossy());
        visit(c"CLAVIRON_RUN_MODE", scoped.run_mode.as_env_value());
        visit(c"CLAVIRON_PROCESS_ID", &scoped.process_id.to_string_lossy());
        if matches!(scoped.run_mode, RunMode::Worker(_)) {
            visit(
                c"CLAVIRON_WORKER_NUMBER",
                &scoped.worker_id.to_string_lossy(),
            );
        }
    } else if let crate::engine::PhpExecution::Request(request) =
        &context.execution
    {
        // Direct-engine callers can supply runtime identity without service boot.
        let runtime = &request.runtime;
        visit(c"CLAVIRON_HOSTNAME", &runtime.server_hostname);
        visit(c"CLAVIRON_RUNNER", &run_identifier(runtime.server_run_mode));
        visit(c"CLAVIRON_RUN_MODE", runtime.server_run_mode.as_env_value());
        visit(c"CLAVIRON_PROCESS_ID", &std::process::id().to_string());
        let worker = match runtime.server_run_mode {
            RunMode::Worker(id) => id.raw(),
            _ => 0,
        };
        visit(c"CLAVIRON_WORKER_ID", &worker.to_string());
        visit(
            c"CLAVIRON_WORKER_COUNT",
            &runtime.server_worker_count.to_string(),
        );
        if matches!(runtime.server_run_mode, RunMode::Worker(_)) {
            visit(c"CLAVIRON_WORKER_NUMBER", &worker.to_string());
        }
    }

    // Route-local runner selection is independent of process identity boot.
    // Persistent programs have no PhpRuntime; do not invent a runner for them.
    if let crate::engine::PhpExecution::Request(request) = &context.execution {
        visit(c"CLAVIRON_PHP_RUNNER", request.runtime.php_runner.as_str());
    }

    crate::engine::thread::THREAD_ID_CSTR.with(|cell| {
        if let Some(value) = cell.get() {
            visit(CLAVIRON_THREAD_ID, &value.to_string_lossy());
        }
    });
    if let Some(program) = context.program() {
        visit(c"CLAVIRON_PHP_PROGRAM", &program.name);
        visit(c"CLAVIRON_PHP_INSTANCE", &program.instance.to_string());
        // Which model owns this program, through the channel its name and
        // instance already travel — rather than a separate function saying the
        // same thing in a different vocabulary. `php_sapi_name()` cannot: it is
        // one string per process and reads "claviron" for all three models.
        visit(c"CLAVIRON_PHP_MODEL", program.model.as_str());
    }
}

fn register_execution_identity(
    context: &ClavironContext,
    vars: *mut Zval,
) {
    visit_execution_identity(context, |key, value| unsafe {
        register_variable(key, value, vars);
    });
}

// ---------------------------------------------------------------------
// ClavironSapi — owns the libphp.so SapiModule pointer for our process.
// Constructed once from `ensure_sapi`; stashed in a process-global
// `OnceLock<Result<Arc<ClavironSapi>, String>>` so every PHP thread
// sees the same module.
// ---------------------------------------------------------------------

pub(crate) struct ClavironSapi(pub(crate) Box<SapiModule>);

// SAFETY: constructed and started once before request threads; the process-global
// SAPI owner retains this allocation and PHP uses its own copy of the module.
unsafe impl Send for ClavironSapi {}
unsafe impl Sync for ClavironSapi {}

impl ClavironSapi {
    pub(crate) fn new() -> Result<Self, SapiGatewayError> {
        let extensions =
            crate::extension::freeze_extensions().map_err(|err| {
                SapiGatewayError::Internal(format!(
                    "freeze PHP extension registry: {err}"
                ))
            })?;
        if let Some(module) = super::module::prepare_module(&extensions)
            .map_err(|err| {
                SapiGatewayError::Internal(format!(
                    "build PHP host module: {err}"
                ))
            })?
        {
            install_additional_module(module).map_err(|err| {
                SapiGatewayError::Internal(format!(
                    "install PHP host module: {err}"
                ))
            })?;
        }

        let mut sapi_module = build_sapi_module::<ClavironContext>(
            "claviron",
            "Claviron PHP SAPI",
            log_message,
            |builder| {
                builder
                    // The engine hands PHP a request that is already read, so
                    // there is no input phase to bound. With php.ini's usual
                    // `max_input_time = 60`, `php_execute_script` re-arms
                    // PHP's own execution timer after `execute_php` cancelled
                    // it — on a threaded host that timer is what kills a
                    // daemon at the ini value or logs the "thread not managed
                    // by PHP" complaint. `-1` is PHP's own "no input phase".
                    //
                    // Errors belong in the log, never in the response. PHP
                    // writes them through `ub_write`, which is the body the
                    // client receives, so `display_errors` would append raw
                    // engine text, paths included, to whatever the
                    // application already wrote. Force `log_errors` on and
                    // clear PHP's file target so every engine record reaches
                    // this SAPI's `log_message` bridge instead of bypassing
                    // tracing through a process-global `error_log` path. And
                    // `html_errors` defaults to on for every SAPI but CLI,
                    // which turns it off for itself; a log line wants no tags.
                    .ini_entries(
                        "max_input_time=-1\ndisplay_errors=0\ndisplay_startup_errors=0\nhtml_errors=0\nlog_errors=1\nerror_log=\n"
                    )
                    .ub_write_function(ub_write)
                    .flush_function(flush)
                    .send_headers_function(send_headers)
                    .send_header_function(send_header)
                    .read_cookies_function(read_cookies)
                    .register_server_variables_function(register_server_variables)
                    .getenv_function(claviron_getenv)
            }
        )
        .map_err(|err| SapiGatewayError::Internal(format!("build sapi module: {err}")))?;

        sapi_module.additional_functions =
            crate::functions::build_function_entries().map_err(|err| {
                SapiGatewayError::Internal(format!(
                    "build sapi functions: {err}"
                ))
            })?;

        Ok(Self(Box::new(sapi_module)))
    }
}

// ---------------------------------------------------------------------
// SAPI callbacks
// ---------------------------------------------------------------------

extern "C" fn log_message(
    message: *const c_char,
    syslog_type_int: c_int,
) {
    if message.is_null() {
        return;
    }

    let message = sanitize_log_message(
        &unsafe { CStr::from_ptr(message) }.to_string_lossy(),
    );
    if syslog_type_int <= 3 {
        PHP_REQUEST_LOGGED_ERROR.with(|flag| flag.set(true));
    }

    let globals = SapiGlobals::get();
    if !globals.server_context.is_null() {
        let context =
            ClavironContext::from_server_context(globals.server_context);
        if let Some((application, instance)) = context.application_log_scope() {
            match syslog_type_int {
                0..=3 => {
                    error!(runtime = "php", %application, instance, "{message}");
                }
                4 => {
                    warn!(runtime = "php", %application, instance, "{message}")
                }
                5 | 6 => {
                    info!(runtime = "php", %application, instance, "{message}")
                }
                7 => {
                    debug!(runtime = "php", %application, instance, "{message}")
                }
                _ => {}
            }
            return;
        }
        let mut line = message.as_bytes().to_vec();
        if !line.ends_with(b"\n") {
            line.push(b'\n');
        }
        if context.write_program_output(true, &line).is_some() {
            let _ = context.flush_program_output(true);
            return;
        }
    }

    match syslog_type_int {
        0..=3 => {
            error!("{message}");
        }
        4 => warn!("{message}"),
        5 | 6 => info!("{message}"),
        7 => debug!("{message}"),
        _ => {}
    }
}

extern "C" fn ub_write(
    str: *const c_char,
    str_length: usize,
) -> usize {
    if str.is_null() || str_length == 0 {
        return 0;
    }

    let sapi_globals = SapiGlobals::get();
    if sapi_globals.server_context.is_null() {
        return str_length;
    }

    let context =
        ClavironContext::from_server_context(sapi_globals.server_context);
    if context.is_request_finished() {
        return 0;
    }

    let data =
        unsafe { std::slice::from_raw_parts(str.cast::<u8>(), str_length) };
    match context.append_output(data) {
        true => str_length,
        false => {
            super::handle_abort_connection();
            0
        }
    }
}

extern "C" fn flush(server_context: *mut c_void) {
    if server_context.is_null() {
        return;
    }

    let context = ClavironContext::from_server_context(server_context);
    if !context.php_flush() {
        super::handle_abort_connection();
    }
}

extern "C" fn send_headers(headers: *mut SapiHeaders) -> c_int {
    if headers.is_null() {
        return 3;
    }

    let sapi_globals = SapiGlobals::get();
    if sapi_globals.server_context.is_null() {
        return 2;
    }

    let context =
        ClavironContext::from_server_context(sapi_globals.server_context);
    let code = unsafe { (*headers).http_response_code };
    if let Ok(code) = u16::try_from(code)
        && let Ok(status) = StatusCode::from_u16(code)
        && let Some(output) = context.response_output()
        && let Ok(mut output) = output.lock()
    {
        output.status = status;
    }

    2
}

extern "C" fn send_header(
    header: *mut SapiHeader,
    server_context: *mut c_void,
) {
    if header.is_null() || server_context.is_null() {
        return;
    }

    let Some(sapi_header) = (unsafe { header.as_ref() }) else {
        return;
    };
    let context = ClavironContext::from_server_context(server_context);

    let Some(name) = sapi_header.name() else {
        return;
    };
    if name.eq_ignore_ascii_case("Status") {
        if let Some(value) = sapi_header.value()
            && let Some(code) = value.split_whitespace().next()
            && let Ok(code) = code.parse::<u16>()
            && let Ok(status) = StatusCode::from_u16(code)
            && let Some(output) = context.response_output()
            && let Ok(mut output) = output.lock()
        {
            output.status = status;
        }
        return;
    }

    if let Some(value) = sapi_header.value() {
        context.append_header(name, value);
    }
}

extern "C" fn read_cookies() -> *mut c_char {
    let sapi_globals = SapiGlobals::get();
    if sapi_globals.server_context.is_null() {
        return std::ptr::null_mut();
    }

    ClavironContext::from_server_context(sapi_globals.server_context)
        .cookie_header()
        .and_then(|cookie| CString::new(cookie).ok())
        .map(CString::into_raw)
        .unwrap_or_else(std::ptr::null_mut)
}

// ---------------------------------------------------------------------
// claviron_getenv — vhost-isolated env reads. See module docs in
// engine.rs (now this file) for the full access matrix.
// ---------------------------------------------------------------------

/// Per-vhost env passthrough whitelist consulted by [`claviron_getenv`].
///
/// Same allow-list as `claviron_runtime::env_scrub::RUNTIME_ESSENTIALS`
/// (the keys that survive boot scrub). Listed inline so the SAPI hook
/// stays in this crate; the lists must be kept in sync — both define
/// "OS / runtime essentials, not application secrets".
const PHP_ENV_PASSTHROUGH: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "TMP",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    "LC_COLLATE",
    "LC_TIME",
    "LC_NUMERIC",
    "LC_MONETARY",
    "TZ",
    "PHPRC",
    "PHP_INI_SCAN_DIR",
    "PGCLIENTENCODING",
];

/// # Vhost env access matrix (boundary doc)
///
/// claviron exposes env values to PHP through TWO channels and
/// intentionally not a third. Application-supplied environment values
/// are immutable and shared across requests on that vhost; per-request
/// overlay (`putenv` writes) is born empty and dropped at request end
/// — matches PHP-FPM's `clean_env=yes` default.
///
/// | PHP API                | Path                                       | Lifetime   | Vhost-scoped value visible? |
/// |------------------------|--------------------------------------------|------------|------------------------------|
/// | `getenv($key)`         | SAPI hook → `claviron_getenv` (this)       | request    | ✅ overlay → vhost.environment → whitelist |
/// | `$_SERVER['KEY']`      | `register_server_variables` callback       | request    | ✅ vhost.environment loop |
/// | `getenv()` (no-arg)    | bypasses SAPI hook, iterates libc          | process    | ❌ libc post-scrub holds only `RUNTIME_ESSENTIALS` |
/// | `$_ENV[…]`             | populated from libc `environ`              | request    | ❌ same reason |
/// | `putenv("KEY=val")`    | function-table override → request overlay  | request    | overlay write (libc untouched, dropped at request end) |
/// | C extensions `getenv`  | direct libc, bypasses our hook             | process    | ❌ scrubbed |
///
/// Two channels were a deliberate trade-off: keeping libc clean
/// (concern #2 from the env-isolation design) means the no-arg
/// `getenv()` and `$_ENV` see only the OS / runtime essentials. Both
/// are rarely used to look up app config in modern frameworks (Laravel
/// `env()` checks `$_SERVER` first, phpdotenv reads per-key), so the
/// loss is theoretical.
///
/// SAPI `getenv` callback — execution-local environment reads.
///
/// Resolution order:
///   1. Per-execution overlay (`ClavironContext::env_overlay`) — last writer
///      wins. Populated by the intercepted `putenv()` handler.
///   2. Immutable typed environment supplied by the owning HTTP runtime or
///      persistent program.
///   3. Shared Claviron runtime/thread identity and program identity.
///   4. `PHP_ENV_PASSTHROUGH` whitelist → live libc `getenv`. Lets
///      `PATH`/`TZ`/locale/`PHPRC` reach PHP normally; libc was
///      scrubbed at boot, so only essentials remain there.
///   5. NULL → PHP's `sapi_getenv` falls back to libc `getenv()`.
///
/// # Memory
/// PHP's `sapi_getenv` (in `main/SAPI.c`) `estrndup`s our return
/// value then `free()`s the original with libc free. We must hand
/// back a libc-allocated buffer — `libc::malloc` + manual NUL.
extern "C" fn claviron_getenv(
    name: *const c_char,
    name_len: usize,
) -> *mut c_char {
    if name.is_null() {
        return std::ptr::null_mut();
    }
    // `c_char` is `i8` on aarch64-apple-darwin and `u8` on every Linux
    // target — `from_utf8` only takes `&[u8]`, so reinterpret the slice
    // through a raw cast that's a no-op on both platforms.
    let name_bytes =
        unsafe { std::slice::from_raw_parts(name.cast::<u8>(), name_len) };
    let Ok(name_str) = std::str::from_utf8(name_bytes) else {
        return std::ptr::null_mut();
    };

    let sapi_globals = SapiGlobals::get();
    if !sapi_globals.server_context.is_null() {
        let context =
            ClavironContext::from_server_context(sapi_globals.server_context);

        // 1. Per-request overlay (putenv writes go here, dropped at request end).
        if let Ok(overlay) = context.env_overlay.lock()
            && let Some(value) = overlay.get(name_str)
        {
            return alloc_libc_cstring(value);
        }

        // 2. Immutable environment supplied by this execution's owner.
        if let Some(value) = context.environment_value(name_str) {
            return alloc_libc_cstring(value);
        }

        let mut identity = std::ptr::null_mut();
        visit_execution_identity(context, |key, value| {
            if key.to_bytes() == name_bytes {
                identity = alloc_libc_cstring(value);
            }
        });
        if !identity.is_null() {
            return identity;
        }
    }

    // 3. Whitelist passthrough → libc.
    if PHP_ENV_PASSTHROUGH.contains(&name_str)
        && let Ok(value) = std::env::var(name_str)
    {
        return alloc_libc_cstring(&value);
    }

    // 4. NULL → libc fallback (scrubbed for app keys).
    std::ptr::null_mut()
}

/// Allocate a NUL-terminated C string with libc `malloc` so PHP's
/// `sapi_getenv` can `free()` it after copying.
fn alloc_libc_cstring(s: &str) -> *mut c_char {
    let len = s.len();
    let ptr = unsafe { libc::malloc(len + 1) } as *mut c_char;
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        if len > 0 {
            std::ptr::copy_nonoverlapping(
                s.as_ptr() as *const c_char,
                ptr,
                len,
            );
        }
        *ptr.add(len) = 0;
    }
    ptr
}

// ---------------------------------------------------------------------
// register_server_variables — populates $_SERVER. Reads through
// ClavironContext (engine.rs) and the boot-time SERVER_SCOPED_VARS
// cache for hostname/runner/software (zero-alloc fast path).
// ---------------------------------------------------------------------

extern "C" fn register_server_variables(vars: *mut Zval) {
    if vars.is_null() {
        return;
    }

    let sapi_globals = SapiGlobals::get();
    if sapi_globals.server_context.is_null() {
        return;
    }

    let context =
        ClavironContext::from_server_context(sapi_globals.server_context);
    let crate::engine::PhpExecution::Request(request) = &context.execution
    else {
        register_program_variables(context, vars);
        return;
    };
    let runtime = &request.runtime;
    let proto = match request.parts.version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/1.1",
    };
    let php_self = format!(
        "{}{}",
        request.sapi_req.script_name,
        request.sapi_req.path_info.as_deref().unwrap_or_default()
    );

    // SNI-resolved hostname injected by the vhost middleware
    // (`VhostCtx::hostname` → `HostName` extension). Same source the
    // FCGI gateway uses (claviron-php-fcgi::proxy), so $_SERVER on
    // both runners agrees. Available on every HTTP version: TLS SNI
    // is the source of truth before HTTP/1's `Host:` header or
    // HTTP/2/3's `:authority` are even reachable.
    let request_host =
        request.parts.extensions.get::<HostName>().map(|h| h.0.to_cow_str());

    let scoped = SERVER_SCOPED_VARS.get();

    unsafe {
        // PHP / SAPI identity
        register_variable(PHP_SELF, php_self, vars);
        if let Some(scoped) = scoped {
            register_variable_cstr(SERVER_SOFTWARE, &scoped.software, vars);
        } else {
            register_variable(
                SERVER_SOFTWARE,
                runtime.server_software.clone(),
                vars,
            );
        }
        register_variable(SERVER_PROTOCOL, proto, vars);

        // Document / script paths
        register_variable(
            DOCUMENT_ROOT,
            request.sapi_req.document_root.to_string_lossy().as_bytes(),
            vars,
        );
        register_variable(
            SCRIPT_FILENAME,
            request.sapi_req.script_file.to_string_lossy().as_bytes(),
            vars,
        );
        register_variable(
            PATH_TRANSLATED,
            request.sapi_req.script_file.to_string_lossy().as_bytes(),
            vars,
        );
        register_variable(
            SCRIPT_NAME,
            request.sapi_req.script_name.as_str(),
            vars,
        );
        register_variable(
            c"DOCUMENT_URI",
            request.sapi_req.document_uri.as_str(),
            vars,
        );

        // Request line
        register_variable(
            REQUEST_URI,
            request.sapi_req.request_uri.as_str(),
            vars,
        );
        register_variable(REQUEST_METHOD, request.parts.method.as_str(), vars);
        register_variable(
            QUERY_STRING,
            request.sapi_req.query_string.as_str(),
            vars,
        );

        // SERVER_NAME / HTTP_HOST family (request host from `Host` header)
        if let Some(request_host) = request_host.as_ref() {
            register_variable(SERVER_NAME, request_host.as_ref(), vars);
            register_variable(c"HTTP_HOST", request_host.as_ref(), vars);
            register_variable(
                c"HTTP_X_FORWARDED_HOST",
                request_host.as_ref(),
                vars,
            );
        }
        if let Some(scheme) =
            request.conn_ctx.as_ref().map(|conn| conn.scheme()).or_else(|| {
                claviron_http::ext::request_scheme(&request.parts.extensions)
            })
        {
            register_variable(c"REQUEST_SCHEME", scheme.as_str(), vars);
            register_variable(c"HTTP_X_FORWARDED_PROTO", scheme.as_str(), vars);
            register_variable(
                HTTPS,
                if scheme == http::uri::Scheme::HTTPS { "on" } else { "off" },
                vars,
            );
        }

        // Network identity (from per-conn ConnCtx; empty when running outside
        // a listener context, e.g. unit tests with no extension)
        if let Some(conn) = request.conn_ctx.as_ref() {
            register_variable(SERVER_ADDR, conn.local_ip.to_string(), vars);
            register_variable(SERVER_PORT, conn.local_port.to_string(), vars);
            let remote_ip = conn.real_ip.unwrap_or_else(|| conn.remote.ip());
            register_variable(REMOTE_ADDR, remote_ip.to_string(), vars);
            register_variable(
                REMOTE_PORT,
                conn.remote.port().to_string(),
                vars,
            );
            register_variable(c"HTTP_VIA", conn.remote.ip().to_string(), vars);
            if let Some(cc) = conn.country_code.as_deref() {
                register_variable(c"CLIENT_COUNTRY", cc, vars);
            }
            register_tls_vars(conn.tls(), vars);
        }

        // Body shape (PHP also reads these from request_info, but $_SERVER
        // exposure mirrors fcgi-style CGI vars)
        if let Some(ct) = request
            .parts
            .headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
        {
            register_variable(c"CONTENT_TYPE", ct, vars);
        }
        register_variable(
            c"CONTENT_LENGTH",
            request
                .request_body
                .content_length(&request.parts.headers)
                .to_string(),
            vars,
        );

        register_execution_identity(context, vars);
    }

    if let Some(path_info) = request.sapi_req.path_info.as_deref() {
        unsafe {
            register_variable(PATH_INFO, path_info, vars);
        }
    }

    // Typed environment supplied by the route's application owner.
    for (key, value) in context.environment() {
        if let Ok(name) = CString::new(key.as_str()) {
            unsafe {
                register_variable(name.as_c_str(), value.as_str(), vars);
            }
        }
    }

    // Forward incoming HTTP headers as HTTP_* CGI vars. Mirrors the
    // FCGI runner's `build_params` loop — same DENY_HEADERS hop-by-hop
    // and transport-owned scheme filter, so embed and fcgi expose
    // identical $_SERVER surfaces to PHP regardless of runner.
    //
    // `Cookie` is in DENY_HEADERS and therefore skipped here. HTTP/2
    // and HTTP/3 split it into multiple header occurrences for
    // HPACK/QPACK efficiency; a per-iteration register would overwrite
    // each subsequent `register_variable("HTTP_COOKIE", v, …)` and
    // drop every cookie past the first. HTTP_COOKIE is registered
    // ONCE below from the joined value built by `cookie_header()`.
    //
    // `Content-Type` / `Content-Length` are also in DENY_HEADERS — PHP
    // already sees them through `CONTENT_TYPE`/`CONTENT_LENGTH` set
    // earlier (CGI 1.1), and forwarding them under `HTTP_*` would
    // duplicate.
    for (name, value) in request.parts.headers.iter() {
        if claviron_php::headers::is_request_denied(name.as_str()) {
            continue;
        }
        let Ok(value) = value.to_str() else {
            continue;
        };
        let Some(cgi_name) = cgi_header_name(name) else {
            continue;
        };
        unsafe {
            register_variable(cgi_name.as_c_str(), value, vars);
        }
    }

    // HTTP_COOKIE — single register from the joined value. PHP also
    // populates `$_COOKIE` via the `read_cookies` SAPI callback (which
    // uses the same joined string), but apps that read
    // `$_SERVER['HTTP_COOKIE']` directly need this here too.
    if let Some(joined) = context.cookie_header() {
        unsafe {
            register_variable(HTTP_COOKIE, joined.as_str(), vars);
        }
    }
}

/// Persistent scripts have program identity and environment, without invented
/// HTTP method, TLS, host, or connection metadata.
fn register_program_variables(
    context: &ClavironContext,
    vars: *mut Zval,
) {
    let Some(program) = context.program() else {
        return;
    };
    let script = &program.script;
    unsafe {
        register_variable(
            SCRIPT_FILENAME,
            script.as_os_str().as_encoded_bytes(),
            vars,
        );
        register_variable(
            SCRIPT_NAME,
            script.as_os_str().as_encoded_bytes(),
            vars,
        );
        register_variable(
            PHP_SELF,
            script.as_os_str().as_encoded_bytes(),
            vars,
        );
        register_execution_identity(context, vars);
        if let Some(scoped) = SERVER_SCOPED_VARS.get() {
            register_variable_cstr(SERVER_SOFTWARE, &scoped.software, vars);
        }
        for (key, value) in context.environment() {
            if let Ok(key) = CString::new(key.as_str()) {
                register_variable(&key, value, vars);
            }
        }
        // Program arguments are native script values, not environment keys.
        super::ffi::php_build_argv(std::ptr::null(), vars);
    }
}

fn register_tls_vars(
    tls: Option<&TlsInfo>,
    vars: *mut Zval,
) {
    let Some(tls) = tls else {
        return;
    };

    if let Some(protocol) = tls.ssl_protocol() {
        unsafe { register_variable(c"SSL_PROTOCOL", protocol, vars) };
    }
    if let Some(cipher) = tls.ssl_cipher() {
        unsafe { register_variable(c"SSL_CIPHER", cipher, vars) };
    }
    unsafe {
        register_variable(c"SSL_CLIENT_VERIFY", tls.ssl_client_verify(), vars)
    };
    if let Some(cert) = tls.client_cert.as_ref() {
        unsafe {
            register_variable(
                c"SSL_CLIENT_S_DN",
                cert.subject_dn.as_str(),
                vars,
            );
            register_variable(
                c"SSL_CLIENT_I_DN",
                cert.issuer_dn.as_str(),
                vars,
            );
        }
    }
}

/// Stable per-process identity label for `CLAVIRON_RUNNER`. Mirrors
/// `claviron-php-fcgi`'s helper so embed-side and FCGI-side $_SERVER
/// vars use the same vocabulary.
fn run_identifier(run_mode: RunMode) -> String {
    match run_mode {
        RunMode::Standalone => "standalone".to_string(),
        RunMode::Master => "master".to_string(),
        RunMode::Worker(wid) => format!("worker_{}", wid.raw()),
    }
}

fn cgi_header_name(name: &HeaderName) -> Option<CString> {
    let raw = name.as_str().as_bytes();
    let mut cgi_name = Vec::with_capacity(b"HTTP_".len() + raw.len());
    cgi_name.extend_from_slice(b"HTTP_");
    for byte in raw {
        cgi_name.push(match byte {
            b'a'..=b'z' => byte - 32,
            b'-' => b'_',
            _ => *byte,
        });
    }
    CString::new(cgi_name).ok()
}
