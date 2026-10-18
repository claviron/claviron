//! `claviron_is_cancelled(): bool` exposes the cancellation token owned by
//! the current PHP execution.

use std::sync::atomic::Ordering;

use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, SapiGlobals};

use crate::engine::{ClavironContext, SAPI_SHUTTING_DOWN};
use crate::sapi::ServerContext;

/// PHP-callable cancellation check for the current execution.
///
/// Persistent applications and background programs observe their own
/// controller token. Request executions observe the request-pool drain token.
/// This intentionally says nothing about why the execution was cancelled: a
/// task stop, restart, service drain, or server shutdown are all valid
/// cancellation sources.
pub(crate) extern "C" fn claviron_is_cancelled_handler(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let globals = SapiGlobals::get();
        let cancelled = if globals.server_context.is_null() {
            SAPI_SHUTTING_DOWN.load(Ordering::Acquire)
        } else {
            ClavironContext::from_server_context(globals.server_context)
                .is_cancelled()
        };
        retval.set_bool(cancelled);
    }));
}

/// PHP-callable "am I serving an HTTP request right now?".
///
/// True for a script execution, where the SAPI owns the request, and for an
/// application between `claviron_application_receive()` and
/// `claviron_application_respond()`, where the request arrived as a PHP value
/// instead. The two share no machinery — one answers through `echo` and
/// `header()`, the other through `claviron_application_respond()` — but both
/// are HTTP, and a script that only wants to know *that* should not have to
/// infer it from `PHP_SAPI`, which is `"claviron"` for every model in the
/// process and cannot distinguish them.
///
/// False for a task always, and for an application running its own loop
/// between messages.
pub(crate) extern "C" fn claviron_is_on_http_handler(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let globals = SapiGlobals::get();
        let on_http = !globals.server_context.is_null()
            && ClavironContext::from_server_context(globals.server_context)
                .is_on_http();
        retval.set_bool(on_http);
    }));
}
