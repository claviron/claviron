//! `PhpSapiEngine` — request orchestrator that bridges the async
//! ingress (axum / upstream caller) with the synchronous PHP thread
//! pool ([`pool::SapiThreadPool`]).
//!
//! Per-request shape:
//!
//!   1. `execute` decides Buffered vs Streaming exec mode from
//!      `PhpRuntime`; buffered jobs collect the request body, streaming
//!      jobs bridge it into PHP's blocking `read_post` hook.
//!   2. `pool.dispatch(job)` round-robins onto a SAPI thread with
//!      bounded queueing (slot_freed wakeup, queue_timeout fallback).
//!   3. The pool thread's [`pool::worker::php_thread_main`] loop receives
//!      the job and calls [`run_php_on_thread`], which runs the script
//!      under the per-request [`ClavironContext`].
//!   4. Buffered: caller awaits `reply_rx`, gets a complete
//!      `RouteResponse`. Streaming: caller awaits the headers oneshot
//!      and returns immediately while the PHP thread keeps pushing
//!      body bytes through an mpsc the response was built around.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use claviron_http::ext::ConnCtx;
use claviron_http::policy::BodyNeed;
use claviron_http::{HttpExchange, RouteBody, RouteResponse};
use claviron_php::PhpRuntime;
use ext_php_rs::embed::EmbedError;
use ext_php_rs::zend::ExecutorGlobals;
use http::request::Parts;
use http::{HeaderMap, Request, Response, StatusCode};
#[cfg(test)]
use http_body_util::BodyExt;
pub use pool::{SapiThreadPoolSnapshot, SapiThreadSlotSnapshot};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::error::SapiGatewayError;
use crate::sapi::callbacks::{
    ClavironSapi, php_request_logged_error, reset_php_request_error_flag,
};
use crate::sapi::{
    ExecutePhpError, ZEND_RESULT_CODE_SUCCESS, run_request, run_sapi_startup,
};

pub(crate) mod application;
mod body;
pub(crate) mod context;
pub(crate) mod interrupt;
pub(crate) mod pool;
pub(crate) mod startup;
use body::request::{RequestBodySource, collect_body};
use body::response::{
    BodySink, CapturedResponse, STREAM_BODY_CAPACITY, StreamingSink,
    body_from_receiver,
};
pub(crate) use context::{
    ClavironContext, PhpExecution, PhpProgramContext, PhpWorkUnit, ProgramModel,
};

pub(crate) mod program;
#[cfg(feature = "tasks")]
pub(crate) mod task;
pub(crate) mod thread;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SapiPoolSettings {
    pub(crate) max_requests: Option<u64>,
    pub(crate) threads_min: u8,
    pub(crate) threads_max: u8,
    pub(crate) threads_max_idle: u8,
    pub(crate) thread_idle_timeout: Duration,
    pub(crate) thread_max_life: Option<Duration>,
}

// ---------------------------------------------------------------------
// Process-wide statics — flags and module slot the rest of the crate
// reads through `crate::engine::*`.
// ---------------------------------------------------------------------

/// Request-pool shutdown flag. Read by HTTP executions of
/// `claviron_is_cancelled()` and by
/// [`pool::SapiThreadPool::dispatch`] to short-circuit new work.
/// Flipped in [`pool::SapiThreadPool::drain`] when the runtime
/// cancellation token fires, before we wait for in-flight requests
/// to finish.
///
/// One HTTP request pool exists per process. Task executions use their
/// own cancellation token through the current SAPI server context, so
/// stopping or restarting this pool cannot stop a task.
pub(crate) static SAPI_SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

/// The wake that goes with [`SAPI_SHUTTING_DOWN`]: whatever a request waits
/// on — a body piece, room in the reply channel — parks on this instead of
/// polling the flag, and the drain cancels it. One token per pool
/// generation, since a drained pool may be resumed and a cancelled token
/// cannot be un-cancelled.
static SAPI_SHUTDOWN: std::sync::RwLock<
    Option<tokio_util::sync::CancellationToken>,
> = std::sync::RwLock::new(None);

/// This generation's shutdown wake, taken when a request starts.
pub(crate) fn sapi_shutdown_signal() -> tokio_util::sync::CancellationToken {
    SAPI_SHUTDOWN
        .write()
        .expect("sapi shutdown lock poisoned")
        .get_or_insert_with(tokio_util::sync::CancellationToken::new)
        .clone()
}

/// Wake every request of this generation: the pool is draining.
pub(crate) fn cancel_sapi_shutdown_signal() {
    if let Some(token) =
        SAPI_SHUTDOWN.read().expect("sapi shutdown lock poisoned").as_ref()
    {
        token.cancel();
    }
}

/// A fresh wake for a resumed pool.
pub(crate) fn renew_sapi_shutdown_signal() {
    *SAPI_SHUTDOWN.write().expect("sapi shutdown lock poisoned") =
        Some(tokio_util::sync::CancellationToken::new());
}

/// Process-wide SAPI module slot. Set the first time any pool thread
/// reaches [`ensure_sapi`]; subsequent threads observe the same `Arc`.
static SAPI: OnceLock<Result<Arc<ClavironSapi>, String>> = OnceLock::new();

// ---------------------------------------------------------------------
// Public request type — produced by `proxy.rs::try_php_proxy`,
// consumed by `PhpSapiEngine::execute`.
// ---------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct PhpSapiRequest {
    pub script_name: String,
    pub script_file: std::path::PathBuf,
    pub document_root: std::path::PathBuf,
    pub document_uri: String,
    pub request_uri: String,
    pub query_string: String,
    pub path_info: Option<String>,
}

/// Job posted to a dedicated PHP thread by [`PhpSapiEngine::execute`].
/// Owned values only — the worker thread runs PHP synchronously to
/// completion before reading the next job, so there is no concurrent
/// access to any of these fields. The `reply` oneshot is how the caller
/// receives the captured response (or error).
pub(crate) struct PhpJob {
    pub(crate) runtime: Arc<PhpRuntime>,
    pub(crate) sapi_req: PhpSapiRequest,
    pub(crate) parts: Parts,
    pub(crate) conn_ctx: Option<Arc<ConnCtx>>,
    pub(crate) request_body: RequestBodySource,
    /// Where PHP body bytes go for this specific job. Buffered jobs
    /// hold a `BodySink::Buffered` (the actual bytes accumulate in the
    /// returned `CapturedResponse`); streaming jobs hold a
    /// `BodySink::Streaming` carrying the headers oneshot + body mpsc
    /// the caller already wired into a `RouteBody`.
    pub(crate) body_sink: BodySink,
    /// Outcome reply. Buffered: full `CapturedResponse` after PHP
    /// exit. Streaming: `CapturedResponse` is still produced (status +
    /// trailing headers) but the body bytes have already been pushed
    /// through `body_sink` — caller has long since returned to the
    /// HTTP transport. Either way `Err` here means the script blew up
    /// before the response was meaningful.
    pub(crate) reply:
        oneshot::Sender<Result<CapturedResponse, SapiGatewayError>>,
}

// ---------------------------------------------------------------------
// PhpSapiEngine — the public façade.
// ---------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct PhpSapiEngine {
    /// Set once by [`PhpSapiEngine::start_pool`], called from
    /// `PhpSapiService::run` when the runnable starts.
    /// After that, every `execute` call dispatches through the pool;
    /// each pool thread reads its own mpsc, runs PHP synchronously,
    /// and replies via the per-job oneshot. The pool's per-thread
    /// mpsc is the serialisation primitive — no global mutex around
    /// PHP execution.
    pool: Arc<OnceLock<Arc<pool::SapiThreadPool>>>,
}

impl PhpSapiEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start one service generation over the process-owned PHP thread pool.
    /// The first call creates the pool; later calls re-open it only after the
    /// preceding generation has drained every accepted request.
    ///
    /// Each pool thread:
    ///   1. Initialises the SAPI module (one-time, lazy via `ensure_sapi`,
    ///      shared across all threads through `SAPI: OnceLock<…>`).
    ///   2. Calls `ext_php_rs_sapi_per_thread_init` once for this thread —
    ///      the ZTS TSRM slot is then bound to *this* thread for the
    ///      lifetime of the worker process.
    ///   3. Loops on `blocking_recv` for jobs; each job runs PHP to
    ///      completion before the next is dequeued on that thread.
    pub(crate) fn start_pool(
        &self,
        settings: SapiPoolSettings,
        cancellation: CancellationToken,
    ) -> Result<Vec<JoinHandle<()>>, String> {
        let SapiPoolSettings {
            max_requests,
            threads_min,
            threads_max,
            threads_max_idle,
            thread_idle_timeout,
            thread_max_life,
        } = settings;
        let pool = if let Some(pool) = self.pool.get() {
            pool.resume().map_err(|in_flight| {
                format!(
                    "PHP SAPI pool still has {in_flight} accepted request(s) from its previous generation"
                )
            })?;
            Arc::clone(pool)
        } else {
            let pool = Arc::new(pool::SapiThreadPool::spawn(
                max_requests,
                threads_min as usize,
                threads_max as usize,
                threads_max_idle as usize,
                thread_idle_timeout,
                thread_max_life,
            ));
            self.pool.set(Arc::clone(&pool)).map_err(|_| {
                "PHP SAPI pool was initialized concurrently".to_string()
            })?;
            pool
        };

        let mut maintenance = Vec::with_capacity(2);
        // Background per-thread rotation (only if `max_requests`
        // is `Some`). Self-disabled inside the manager when the value
        // is `None` / 0, so this is a cheap no-op for master/worker
        // setups that rely on supervisor rotation instead.
        if let Some(handle) =
            Arc::clone(&pool).spawn_rotation_manager(cancellation.child_token())
        {
            maintenance.push(handle);
        }
        // Background idle culler — only spawns when elastic mode is
        // engaged (`threads_max > threads_min`); otherwise zero-cost
        // for fixed pools.
        if let Some(handle) = Arc::clone(&pool).spawn_idle_culler(cancellation)
        {
            maintenance.push(handle);
        }
        Ok(maintenance)
    }

    /// Drain the pool: stop accepting new jobs and wait for in-flight
    /// PHP execution to finish. No-op if the pool was never started.
    /// Returns whether all request threads finished and were joined. A finite
    /// grace may interrupt remaining PHP executions; `None` waits indefinitely.
    pub async fn drain(
        &self,
        grace: Option<Duration>,
    ) -> bool {
        if let Some(pool) = self.pool.get() {
            pool.drain(grace).await
        } else {
            true
        }
    }

    /// Validate a new request-pool generation before it starts.
    /// This check does not reopen HTTP admission or change cancellation flags.
    pub(crate) fn ensure_drained(&self) -> Result<(), String> {
        if let Some(pool) = self.pool.get() {
            pool.ensure_drained().map_err(|pending| {
                format!("PHP SAPI pool still has {pending} pending dispatch/reservation(s) from its previous generation")
            })?;
        }
        Ok(())
    }

    pub fn pool_snapshot(&self) -> Option<SapiThreadPoolSnapshot> {
        self.pool.get().map(|pool| pool.snapshot())
    }

    pub async fn execute(
        &self,
        runtime: Arc<PhpRuntime>,
        sapi_req: PhpSapiRequest,
        req: Request<RouteBody>,
        exchange_tx: Option<mpsc::Sender<HttpExchange>>,
    ) -> Result<RouteResponse, SapiGatewayError> {
        // INVARIANT `EXECUTION_IO: OPCACHE_OWNS_FRESHNESS` — canonical text in
        // `crates/claviron-php/ARCHITECTURE.md`. Deliberate: no pre-flight
        // `fs::metadata` on the script_file. PHP returns its own 404 ("No
        // input file specified") if the script is genuinely missing. Do not
        // add a stat here; read the invariant first if you think one is owed.
        let (parts, body) = req.into_parts();
        if parts.extensions.get::<claviron_http::policy::HttpPolicy>().is_none()
        {
            return Err(SapiGatewayError::Internal(
                "HttpPolicy is required for PHP script execution".into(),
            ));
        }
        let policy = runtime.policy(&parts);
        if claviron_http::body::declared_length_within(
            &parts.headers,
            policy.requests.max_body_size,
        )
        .is_err()
        {
            let mut response = Response::new(claviron_http::empty_body());
            *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
            return Ok(response);
        }
        let conn_ctx = parts.extensions.get::<Arc<ConnCtx>>().cloned();

        let pool = self.pool.get().ok_or_else(|| {
            SapiGatewayError::Internal(
                "php thread pool not started — PhpSapiService::run not spawned yet?".to_string()
            )
        })?;

        // The reply is always a stream: the response goes back as soon as
        // the headers commit, and body bytes flow through an mpsc the PHP
        // thread keeps pushing into until the script finishes (or the
        // client disconnects). A stream can be made whole, a whole never a
        // stream, and measured on bison it costs nothing (`docs/REVIEW.md`
        // §3e). Holding a small reply to send it whole — and with it the
        // fatal-error rewrite to 500, which needs the reply still in hand —
        // is step 5b's sink.
        // `exchange_tx` (audit middleware) is dropped — body bytes flow
        // straight to the HTTP client and are never re-buffered. Same
        // trade-off as the FCGI path.
        let _ = exchange_tx;
        // Nobody, no pump: PHP still asks for its input when the request
        // ends, and a pump that has not run yet answers that with a 50 ms
        // sleep on the PHP thread. A body the policy takes whole is
        // collected first; any other is handed to PHP as it arrives.
        let request_body = if http_body::Body::is_end_stream(&body) {
            RequestBodySource::buffered(Bytes::new())
        } else {
            let declared = http_body::Body::size_hint(&body).exact();
            let whole = policy
                .requests
                .delivery(BodyNeed::Stream, declared, false)
                .takes_whole(declared);
            if whole {
                let bytes =
                    match collect_body(body, policy.requests.max_body_size)
                        .await
                    {
                        Ok(bytes) => bytes,
                        Err(error)
                            if matches!(
                            error
                                .downcast_ref::<claviron_http::body::BodyLimit>(
                                ),
                            Some(
                                claviron_http::body::BodyLimit::TooLarge { .. }
                            )
                        ) =>
                        {
                            let mut response =
                                Response::new(claviron_http::empty_body());
                            *response.status_mut() =
                                StatusCode::PAYLOAD_TOO_LARGE;
                            return Ok(response);
                        }
                        Err(error) => {
                            return Err(SapiGatewayError::BadRequest(format!(
                                "request body read error: {error}"
                            )));
                        }
                    };
                RequestBodySource::buffered(bytes)
            } else {
                RequestBodySource::streaming(
                    body,
                    policy.requests.max_body_size,
                )
            }
        };

        let (headers_tx, headers_rx) =
            oneshot::channel::<(StatusCode, HeaderMap)>();
        let (body_tx, body_rx) = mpsc::channel::<Bytes>(STREAM_BODY_CAPACITY);
        let sink = StreamingSink::new(
            headers_tx,
            body_tx,
            policy.responses.write_timeout,
            policy.responses,
        );

        // Reply oneshot still wired so a PHP failure that
        // happens *before* headers commit — e.g. parse error
        // on first include — surfaces as a gateway error
        // rather than a hung response. Successful streaming
        // ignores this reply (we've already returned).
        let (reply_tx, reply_rx) = oneshot::channel();
        let job = PhpJob {
            runtime,
            sapi_req,
            parts,
            conn_ctx,
            request_body,
            body_sink: BodySink::Streaming(sink),
            reply: reply_tx,
        };
        // Every request waits up to the queue timeout for a thread:
        // admission follows the route's `requests.queue_timeout`, not how the
        // reply is sent. A burst shorter than it smooths out
        // instead of turning into 503s.
        pool.dispatch(job, policy.requests.queue_timeout).await?;

        // A streamed reply commits its head while PHP still runs; a held
        // one commits nothing and comes back whole with the job's reply —
        // its status already through the fatal-error check. The head's
        // sender dropping unsent is the held reply's sign, not a failure:
        // the reply then says what happened.
        tokio::pin!(headers_rx);
        tokio::pin!(reply_rx);
        let head = tokio::select! {
            biased;
            head = &mut headers_rx => head.ok(),
            reply = &mut reply_rx => return whole_reply(reply),
        };
        let Some((status, headers)) = head else {
            return whole_reply(reply_rx.await);
        };
        let mut response = Response::new(body_from_receiver(body_rx));
        *response.status_mut() = status;
        let dst = response.headers_mut();
        for (name, value) in headers.iter() {
            dst.append(name.clone(), value.clone());
        }
        Ok(response)
    }
}

/// A reply that was held to its end, as the PHP thread hands it back.
fn whole_reply(
    reply: Result<
        Result<CapturedResponse, SapiGatewayError>,
        oneshot::error::RecvError,
    >
) -> Result<RouteResponse, SapiGatewayError> {
    match reply {
        Ok(Ok(captured)) => Ok(captured.into_response()),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(SapiGatewayError::BadGateway(
            "php thread reply dropped".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------
// Per-job execution — runs on the dedicated PHP thread (set up by
// `pool::worker::php_thread_main`). SAPI startup + TSRM init for this thread
// are already done by the thread bootstrap; this function only handles
// the per-request lifecycle.
// ---------------------------------------------------------------------

/// `body_sink` decides where PHP body bytes go: into `CapturedResponse`
/// (Buffered, returned to the caller at PHP exit) or out through an
/// mpsc channel the caller already wrapped into a streaming
/// `RouteBody` (Streaming). Headers + status always land in `output`
/// regardless of mode.
pub(crate) fn run_php_on_thread(
    runtime: Arc<PhpRuntime>,
    sapi_req: PhpSapiRequest,
    parts: Parts,
    conn_ctx: Option<Arc<ConnCtx>>,
    request_body: RequestBodySource,
    body_sink: BodySink,
) -> Result<CapturedResponse, SapiGatewayError> {
    #[cfg(feature = "metrics")]
    let _request_guard = crate::metrics::EmbedRequestGuard::enter();
    #[cfg(feature = "metrics")]
    {
        if let Some(len) = request_body.buffered_len() {
            crate::metrics::record_request_bytes(len as u64);
        }
    }

    let output = Arc::new(Mutex::new(CapturedResponse::default()));
    let context = ClavironContext::new(
        runtime,
        sapi_req.clone(),
        parts,
        conn_ctx,
        request_body,
        Arc::clone(&output),
        body_sink,
    );

    let script = sapi_req.script_file.clone();
    reset_php_request_error_flag();

    // One script execution is one request here, so the budget belongs around
    // it — and php.ini can describe it, which is why this takes PHP's own
    // `max_execution_time` rather than a key of ours shadowing it. The
    // application and task models run a program outside the request pool
    // request and are bounded at their own boundaries instead.
    let _budget = interrupt::Budget::from_php();

    let clean_exit_candidate = AtomicBool::new(false);
    let exec_result = run_request(context, script, |err| {
        let logged_php_error = php_request_logged_error();
        let has_exception = ExecutorGlobals::has_exception();
        let fail_request = !matches!(err, EmbedError::ExecuteScriptError)
            || logged_php_error
            || has_exception;
        if fail_request {
            error!(
                ?err,
                logged_php_error, has_exception, "php sapi run_script failed"
            );
        } else {
            // ext-php-rs returns `ExecuteScriptError` for any non-Ok
            // `run_script` — clean `exit()` / `die()` AND real fatals
            // share the same enum discriminant. The `fail_request`
            // classification above catches logged/exception failures;
            // body markers are checked after request shutdown because
            // display_errors can write a fatal page without calling the
            // SAPI log callback.
            clean_exit_candidate.store(true, Ordering::Relaxed);
        }
        fail_request
    });
    if let Err(ExecutePhpError::Bailout) = &exec_result {
        #[cfg(feature = "metrics")]
        crate::metrics::record_bailout();
    }
    let logged_php_error = php_request_logged_error();

    let mut captured = output
        .lock()
        .map_err(|_| {
            SapiGatewayError::Internal(
                "php sapi output lock poisoned".to_string(),
            )
        })?
        .clone();

    match exec_result {
        Ok(()) if logged_php_error => {
            // Some PHP fatals emit their error page and still let
            // request shutdown complete normally from ext-php-rs'
            // point of view. The SAPI log callback remains the
            // canonical signal that the script failed.
            captured.mark_script_failure();
        }
        Ok(())
            if clean_exit_candidate.load(Ordering::Relaxed)
                && captured.contains_php_fatal_output() =>
        {
            error!(
                "php sapi script emitted fatal output after ExecuteScriptError"
            );
            captured.mark_script_failure();
        }
        Ok(()) if clean_exit_candidate.load(Ordering::Relaxed) => {
            #[cfg(debug_assertions)]
            tracing::debug!("php sapi script ended via PHP exit");
        }
        Ok(()) => {}
        Err(ExecutePhpError::Bailout) => {
            // PHP may already have emitted a useful fatal/warning body
            // through ub_write. Preserve that response, but never let
            // PHP's default 200 status leak for a failed script.
            captured.mark_script_failure();
        }
        Err(err) => return Err(err.into()),
    }

    #[cfg(feature = "metrics")]
    crate::metrics::record_response_bytes(captured.body.len() as u64);
    Ok(captured)
}

// ---------------------------------------------------------------------
// One-time SAPI module bootstrap. First pool thread to enter wins the
// `OnceLock`; subsequent threads observe the same `Arc<ClavironSapi>`.
// ---------------------------------------------------------------------

pub(crate) fn ensure_sapi() -> Result<Arc<ClavironSapi>, SapiGatewayError> {
    let result = SAPI.get_or_init(|| {
        startup::start()?;
        unsafe {
            ext_php_rs::embed::ext_php_rs_sapi_startup();
        }

        let mut sapi = ClavironSapi::new().map_err(|err| err.to_string())?;
        if run_sapi_startup(&raw mut *sapi.0) != ZEND_RESULT_CODE_SUCCESS {
            return Err("php module startup failed".to_string());
        }

        // PHP module startup populated `CG(function_table)`. Swap the
        // built-in `putenv` handler with our vhost-scoped overlay
        // writer so cross-vhost env leakage cannot happen even when
        // PHP code calls putenv().
        crate::intercept::putenv::override_putenv_handler();

        // Replication intercepts: observe cache-invalidation calls
        // and broadcast to peer workers so each invalidates its local
        // copy. Per-thread setup runs once on the bootstrap thread
        // here; every other pool thread re-runs each call from
        // `php_thread_main` after its per-thread TSRM bind.
        crate::intercept::ensure_cache_invalidation_intercepts_for_current_thread();

        Ok(Arc::new(sapi))
    });

    result
        .as_ref()
        .map(Arc::clone)
        .map_err(|err| SapiGatewayError::Internal(err.clone()))
}

// ---------------------------------------------------------------------
// Errors + body collection helper.
// ---------------------------------------------------------------------

impl From<ExecutePhpError> for SapiGatewayError {
    fn from(value: ExecutePhpError) -> Self {
        match value {
            ExecutePhpError::InitSapiGlobalsError(err) => {
                SapiGatewayError::BadRequest(err.to_string())
            }
            ExecutePhpError::RequestStartupFailed => {
                SapiGatewayError::Internal(
                    "php request startup failed".to_string(),
                )
            }
            ExecutePhpError::Bailout => {
                SapiGatewayError::Internal("php bailout".to_string())
            }
        }
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/engine.rs"]
mod tests;
