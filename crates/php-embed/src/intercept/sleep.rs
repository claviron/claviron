//! `sleep` / `usleep` intercepts — make a sleeping program reachable.
//!
//! A PHP program that is sleeping is the one state Claviron could not reach.
//! `php.yaml` says so in its own words: a script blocked outside the VM cannot
//! be interrupted, so a drain waits out `graceful_timeout` and then ends the
//! execution through PHP's timeout path. That is why stopping a console task
//! used to cost seconds instead of milliseconds.
//!
//! It is also the state such a program spends nearly all of its time in. Every
//! daemon-timed task loop in the ecosystem is `work(); sleep(n);` — `queue:work` between
//! empty polls, `schedule:run` inside `repeatEvents()`, and every
//! hand-written `while (true)`. Intercepting the sleep turns the least
//! reachable state into the most reachable one.
//!
//! ## What a cancelled sleep does
//!
//! It returns early, and nothing else. The program continues from the next
//! statement, which is where its own stop condition lives:
//! `while (!claviron_is_cancelled())` is evaluated a full sleep sooner, and the
//! program leaves on its own terms — running its `finally` blocks, committing
//! or rolling back its own transactions, closing what it opened. Nothing is
//! thrown into it and nothing is taken from it.
//!
//! ## The loop that does not listen
//!
//! A sleep is usually what paces a `while` loop, so returning early from every
//! one of them would turn a program that ignores cancellation into a spin: the
//! same loop, at whatever rate the machine allows, until `graceful_timeout`
//! interrupts it. `queue:work` would poll its queue flat out; `repeatEvents()`
//! would burn a core until the minute ended.
//!
//! So the early return happens **once**. It is a signal, and a signal only
//! needs delivering once: a loop that checks `claviron_is_cancelled()` sees it
//! on the very next statement and leaves, while a loop that does not is left
//! sleeping exactly as it does today, deadline and all. The cooperative program
//! gets the whole benefit, the uncooperative one is no worse off than before,
//! and neither can spin.
//!
//! ## Off by default
//!
//! Opt in with `lifecycle.interruptible_sleep`. The behaviour of a language
//! builtin is not ours to change on an operator's behalf: a library that leans
//! on `sleep()` actually sleeping -- a backoff, a rate limiter, a polling
//! protocol -- gets one shortened wait during cancellation, and nothing in the
//! calling code says so. The one-shot rule bounds that to a single occurrence
//! rather than removing it.
//!
//! It is also not what makes a well-behaved task stop promptly: a worker that
//! reaches its own stop check within a second of being cancelled was already
//! fast. What this buys is proportional to how long the program sleeps, which
//! is why a `sleep(60)` idle loop is the case it exists for.
//!
//! ## Scope
//!
//! Programs only. A request has no cancellation token — it has
//! `PG(connection_status)`, a different mechanism with a different meaning —
//! so a request's `sleep()` is delegated untouched to PHP's own handler.

use std::cell::Cell;
use std::ffi::c_char;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ext_php_rs::args::Arg;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, SapiGlobals};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::engine::{ClavironContext, PhpExecution};
use crate::sapi::ServerContext;

type Handler = unsafe extern "C" fn(
    execute_data: *mut ext_php_rs::ffi::_zend_execute_data,
    retval: *mut ext_php_rs::ffi::_zval_struct,
);

/// Whether an operator asked for this. Published once at service boot, read by
/// every thread as it binds.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Record the operator's choice. Called from `PhpSapiService` boot, beside
/// `engine::interrupt::configure`, and for the same reason: a builder that is
/// never registered must not decide process behaviour.
pub(crate) fn configure(enabled: bool) {
    ENABLED.store(enabled, Ordering::SeqCst);
}

static ORIGINAL_SLEEP: OnceLock<Handler> = OnceLock::new();
static ORIGINAL_USLEEP: OnceLock<Handler> = OnceLock::new();

/// How often a waiting program looks up to see whether it was cancelled.
///
/// Small enough that a stop is indistinguishable from immediate, large enough
/// that a one-second sleep costs forty wakeups rather than a spin. A sleep
/// shorter than this is not sliced at all.
const POLL_SLICE: Duration = Duration::from_millis(25);

/// This execution's cancellation token, if it has one.
///
/// `None` for a request, and for anything reached before the server context is
/// installed — both delegate to PHP unchanged.
fn cancellation() -> Option<CancellationToken> {
    let sapi_globals = SapiGlobals::get();
    if sapi_globals.server_context.is_null() {
        return None;
    }
    let context =
        ClavironContext::from_server_context(sapi_globals.server_context);
    match &context.execution {
        PhpExecution::Program(program) => Some(program.cancellation.clone()),
        PhpExecution::Request(_) => None,
    }
}

thread_local! {
    /// Whether this thread has already been told, by an early return, that its
    /// program was cancelled. Cleared whenever a sleep completes normally, so a
    /// thread reused by a later program starts fresh.
    static SIGNALLED: Cell<bool> = const { Cell::new(false) };
}

/// Wait out `total`, or until cancelled. Returns what was left unwaited.
///
/// One loop, whatever the state: cancellation is read on every slice, so the
/// answer is never more than [`POLL_SLICE`] stale. What the one-shot rule
/// changes is only whether a cancelled wait *returns* — not whether it looks.
fn wait(
    total: Duration,
    token: &CancellationToken,
) -> Duration {
    // A wait that begins uncancelled begins a fresh episode. This is also what
    // clears the flag for a thread the pool hands to a later program.
    if !token.is_cancelled() {
        SIGNALLED.with(|signalled| signalled.set(false));
    }

    let deadline = Instant::now() + total;
    loop {
        // Cut this wait short exactly once. A program that checks
        // `claviron_is_cancelled()` needs one; a program that does not would
        // spin on every subsequent one.
        if token.is_cancelled() && !SIGNALLED.with(Cell::get) {
            SIGNALLED.with(|signalled| signalled.set(true));
            return deadline.saturating_duration_since(Instant::now());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Duration::ZERO;
        }
        std::thread::sleep(remaining.min(POLL_SLICE));
    }
}

/// Replacement for PHP's `sleep()`.
///
/// Returns 0 when the full duration elapsed and the remaining whole seconds
/// when it did not — which is `sleep()`'s existing contract for a sleep cut
/// short by a signal, and the reason no caller needs to learn anything new.
pub(crate) extern "C" fn claviron_sleep_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    let mut seconds = Arg::new("seconds", DataType::Long);
    if ex.parser().arg(&mut seconds).parse().is_err() {
        delegate(&ORIGINAL_SLEEP, ex, retval);
        return;
    }
    let Some(seconds) = seconds.val::<i64>() else {
        delegate(&ORIGINAL_SLEEP, ex, retval);
        return;
    };
    let Ok(seconds) = u64::try_from(seconds) else {
        // Negative: PHP's own handler owns the ValueError.
        delegate(&ORIGINAL_SLEEP, ex, retval);
        return;
    };

    let Some(token) = cancellation() else {
        delegate(&ORIGINAL_SLEEP, ex, retval);
        return;
    };

    let left = wait(Duration::from_secs(seconds), &token);
    if left.is_zero() {
        retval.set_long(0);
        return;
    }
    debug!(
        target: "claviron::sleep_intercept",
        requested_secs = seconds,
        remaining_ms = left.as_millis() as u64,
        "sleep cut short by cancellation"
    );
    // Round up: a sleep interrupted with anything left owes at least a second,
    // and returning 0 would claim it completed.
    retval.set_long(i64::try_from(left.as_secs().max(1)).unwrap_or(i64::MAX));
}

/// Replacement for PHP's `usleep()`. Returns null either way, as PHP's does.
pub(crate) extern "C" fn claviron_usleep_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    let mut micros = Arg::new("microseconds", DataType::Long);
    if ex.parser().arg(&mut micros).parse().is_err() {
        delegate(&ORIGINAL_USLEEP, ex, retval);
        return;
    }
    let Some(micros) = micros.val::<i64>() else {
        delegate(&ORIGINAL_USLEEP, ex, retval);
        return;
    };
    let Ok(micros) = u64::try_from(micros) else {
        delegate(&ORIGINAL_USLEEP, ex, retval);
        return;
    };

    let Some(token) = cancellation() else {
        delegate(&ORIGINAL_USLEEP, ex, retval);
        return;
    };

    let left = wait(Duration::from_micros(micros), &token);
    if !left.is_zero() {
        debug!(
            target: "claviron::sleep_intercept",
            requested_us = micros,
            remaining_ms = left.as_millis() as u64,
            "usleep cut short by cancellation"
        );
    }
    retval.set_null();
}

fn delegate(
    original: &OnceLock<Handler>,
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    match original.get().copied() {
        Some(original) => unsafe {
            original(ex as *mut ExecuteData, retval as *mut Zval);
        },
        None => retval.set_null(),
    }
}

/// Swap PHP's `sleep` and `usleep` handlers in the **current ZTS thread's**
/// `CG(function_table)`. Idempotent.
pub(crate) fn ensure_sleep_intercept_for_current_thread() {
    if !ENABLED.load(Ordering::SeqCst) {
        return;
    }
    install(b"sleep", &ORIGINAL_SLEEP, claviron_sleep_handler);
    install(b"usleep", &ORIGINAL_USLEEP, claviron_usleep_handler);
}

fn install(
    name: &[u8],
    original: &OnceLock<Handler>,
    replacement: extern "C" fn(&mut ExecuteData, &mut Zval),
) {
    use ext_php_rs::ffi::{_zend_function, zend_hash_str_find_ptr_lc};

    let label = String::from_utf8_lossy(name).into_owned();
    unsafe {
        let cg = ext_php_rs::ffi::ext_php_rs_compiler_globals();
        if cg.is_null() {
            warn!(function = %label, "sleep intercept: compiler globals null");
            return;
        }
        let function_table_ptr = (*cg).function_table;
        if function_table_ptr.is_null() {
            warn!(function = %label, "sleep intercept: CG(function_table) is null");
            return;
        }

        let func_ptr = zend_hash_str_find_ptr_lc(
            function_table_ptr,
            name.as_ptr() as *const c_char,
            name.len(),
        ) as *mut _zend_function;
        if func_ptr.is_null() {
            warn!(function = %label, "sleep intercept: function not in CG(function_table)");
            return;
        }

        let replacement_addr = replacement as *const () as usize;
        if let Some(current) = (*func_ptr).internal_function.handler {
            if current as *const () as usize == replacement_addr {
                return;
            }
            let _ = original.set(current);
        } else {
            warn!(function = %label, "sleep intercept: null handler; skipping");
            return;
        }

        (*func_ptr).internal_function.handler = Some(std::mem::transmute::<
            extern "C" fn(&mut ExecuteData, &mut Zval),
            Handler,
        >(replacement));
    }
}
