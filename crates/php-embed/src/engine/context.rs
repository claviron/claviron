//! One execution's PHP context, shared by request and persistent-program paths.

use std::collections::HashMap;
use std::ffi::{CString, NulError, c_char, c_int};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use claviron_http::ext::ConnCtx;
use claviron_php::PhpRuntime;
use ext_php_rs::zend::SapiGlobals;
use http::header::CONTENT_TYPE;
use http::request::Parts;
use http::{HeaderName, HeaderValue, StatusCode, Version};
use tokio_util::sync::CancellationToken;

use super::body::request::RequestBodySource;
use super::body::response::{BodySink, CapturedResponse};
use super::{PhpSapiRequest, SAPI_SHUTTING_DOWN};
use crate::sapi::{ServerContext, php_handle_auth_data, php_output_end_all};

// ---------------------------------------------------------------------
// ClavironContext — one PHP execution scope. Implements `ServerContext` so
// the generic SAPI harness can drive PHP through it. Fields are
// `pub(crate)` because the concrete SAPI callbacks in
// `crate::sapi::callbacks` and the intercepts in `crate::intercept`
// read them directly off the raw `server_context` pointer.
// ---------------------------------------------------------------------

pub(crate) struct PhpRequestContext {
    pub(crate) runtime: Arc<PhpRuntime>,
    pub(crate) sapi_req: PhpSapiRequest,
    pub(crate) parts: Parts,
    pub(crate) conn_ctx: Option<Arc<ConnCtx>>,
    pub(crate) request_body: RequestBodySource,
    pub(crate) output: Arc<Mutex<CapturedResponse>>,
    pub(crate) body_sink: BodySink,
    environment: Option<RequestEnvironment>,
}

/// One immutable environment pinned before dispatch to a reusable ZTS thread.
#[derive(Clone)]
pub(crate) struct RequestEnvironment(
    pub claviron_runtime::congif::dotenw::Snapshot,
);

impl RequestEnvironment {
    fn values<'a>(
        snapshot: &'a Option<Self>,
        runtime: &'a PhpRuntime,
    ) -> &'a HashMap<String, String> {
        snapshot.as_ref().map_or(runtime.environment.as_ref(), |environment| {
            environment.0.as_ref()
        })
    }
}

impl PhpRequestContext {
    fn environment(&self) -> &HashMap<String, String> {
        RequestEnvironment::values(&self.environment, &self.runtime)
    }
}

/// One bounded piece of work inside a program: what a budget wraps, and what
/// decides whether the program is on HTTP right now.
///
/// Note what a `Request` here is *not*. The SAPI holds no request for a
/// program — this one arrived as a PHP value from
/// `claviron_application_receive`, and its response goes back through
/// `claviron_application_respond`, never through `ub_write` and `header()`.
/// Same word, different mechanism, and both are genuinely HTTP.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PhpWorkUnit {
    Request,
    Event,
}

/// Which service owns a program.
///
/// This is the **only** place the task/application distinction is named.
/// Anything asking what a program is *doing* reads
/// [`PhpProgramContext::current`] instead — the two used to be spelled four
/// different ways (a `&str` thread label, a `bool` on `run`, and two flags on
/// the application inbox) and each said it in its own vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProgramModel {
    /// A caller-owned terminal command, not a TaskService occurrence.
    Console,
    /// One returned PHP value, without an HTTP request or persistent program.
    Evaluation,
    /// Only constructed by the `php:` task executor, which is the `tasks`
    /// feature. A build without it serves applications and requests and runs
    /// no programs of its own.
    #[cfg_attr(not(feature = "tasks"), allow(dead_code))]
    Task,
    Application,
}

impl ProgramModel {
    /// Thread-name prefix, and what PHP reads as `CLAVIRON_PHP_MODEL`.
    ///
    /// The word has moved twice: `dedicated`, then `daemon`, now `task` —
    /// the first two stayed inside Rust, and this one does not. A program of
    /// this model is a `php:` task, scheduled and placed by `TaskService`, and
    /// `daemon` is now only one of the four timings such a task may have. The
    /// distinction the field actually draws is [`Self::receives_work`]: work
    /// arrives at an application, and a task *is* the work.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Console => "console",
            Self::Evaluation => "evaluation",
            Self::Task => "task",
            Self::Application => "application",
        }
    }

    /// Can Rust see a unit of work begin inside this program?
    ///
    /// A task's loop is invisible from here — there is no call that means "one
    /// iteration is starting" — so the whole program is one execution. An
    /// application's units arrive through `receive`, so they are visible and
    /// are marked one at a time.
    pub(crate) fn receives_work(self) -> bool {
        matches!(self, Self::Application)
    }

    /// Whether ordinary PHP output belongs on the process console.
    ///
    /// A task is console-shaped. An application is a request server whose
    /// only response path is `claviron_application_respond`; sending an
    /// uncaught framework error page to stdout would corrupt the server log.
    pub(crate) fn uses_console_output(self) -> bool {
        matches!(self, Self::Task | Self::Console)
    }
}

pub(crate) struct PhpProgramContext {
    pub(crate) model: ProgramModel,
    pub(crate) name: String,
    pub(crate) instance: u8,
    pub(crate) script: std::path::PathBuf,
    pub(crate) environment: std::collections::BTreeMap<String, String>,
    pub(crate) cancellation: CancellationToken,
    /// What `receive` last handed PHP, or `None` while the program is running
    /// its own body.
    ///
    /// A task never receives, so this stays `None` for its whole life — that
    /// *is* the difference between the two models, and nothing else has to
    /// restate it.
    pub(crate) current: Option<PhpWorkUnit>,
    #[cfg(feature = "tasks")]
    pub(crate) output: Option<claviron_tasks::TaskOutput>,
}

// Keep the request payload inline: boxing it adds an allocation to every HTTP
// request. Program contexts are allocated once per persistent script.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PhpExecution {
    /// Script model: the SAPI owns an HTTP request and the request *is* the
    /// execution. Output is the response body and `header()` sets its headers.
    Request(PhpRequestContext),
    /// Task and application: a program execution with its own PHP context.
    Program(PhpProgramContext),
}

pub(crate) struct ClavironContext {
    pub(crate) execution: PhpExecution,
    /// Topics and bus cursors belong to this execution, not its reusable ZTS
    /// thread. Persistent programs retain them for their lifetime; one-shot
    /// requests drop them during request shutdown.
    pub(crate) event_subscription:
        Mutex<Option<crate::event::EventSubscription>>,
    /// Invocation operations this execution consumes; same ownership as the
    /// event subscription.
    #[cfg(feature = "tasks")]
    pub(crate) invocation_subscription:
        Mutex<Option<crate::functions::orbit::invoke::InvocationSubscriptions>>,
    /// Intercepted putenv writes belong to this execution: one HTTP request
    /// or the complete lifetime of one persistent program. They never reach
    /// libc or another execution's environment.
    pub(crate) env_overlay: Mutex<HashMap<String, String>>,
    pub(crate) request_finished: bool,
}

impl ClavironContext {
    pub(super) fn new(
        runtime: Arc<PhpRuntime>,
        sapi_req: PhpSapiRequest,
        parts: Parts,
        conn_ctx: Option<Arc<ConnCtx>>,
        request_body: RequestBodySource,
        output: Arc<Mutex<CapturedResponse>>,
        body_sink: BodySink,
    ) -> Self {
        let environment = parts.extensions.get::<RequestEnvironment>().cloned();
        Self {
            execution: PhpExecution::Request(PhpRequestContext {
                runtime,
                sapi_req,
                parts,
                conn_ctx,
                request_body,
                output,
                body_sink,
                environment,
            }),
            event_subscription: Mutex::new(None),
            #[cfg(feature = "tasks")]
            invocation_subscription: Mutex::new(None),
            env_overlay: Mutex::new(HashMap::new()),
            request_finished: false,
        }
    }

    pub(crate) fn environment(
        &self
    ) -> impl Iterator<Item = (&String, &String)> {
        let (request, program) = match &self.execution {
            PhpExecution::Request(request) => {
                (Some(request.environment().iter()), None)
            }
            PhpExecution::Program(program) => {
                (None, Some(program.environment.iter()))
            }
        };
        request.into_iter().flatten().chain(program.into_iter().flatten())
    }

    pub(crate) fn environment_value(
        &self,
        name: &str,
    ) -> Option<&String> {
        match &self.execution {
            PhpExecution::Request(request) => request.environment().get(name),
            PhpExecution::Program(program) => program.environment.get(name),
        }
    }

    /// The program this execution is, if it is one.
    pub(crate) fn program(&self) -> Option<&PhpProgramContext> {
        match &self.execution {
            PhpExecution::Request(_) => None,
            PhpExecution::Program(program) => Some(program),
        }
    }

    pub(crate) fn program_mut(&mut self) -> Option<&mut PhpProgramContext> {
        match &mut self.execution {
            PhpExecution::Request(_) => None,
            PhpExecution::Program(program) => Some(program),
        }
    }

    /// Is PHP serving an HTTP request right now?
    ///
    /// True in two shapes that share nothing mechanically: a script execution,
    /// where the SAPI owns the request, and an application between `receive`
    /// and `respond`, where the request arrived as a PHP value. Both are HTTP,
    /// and code that cares about *that* rather than about which machinery
    /// delivered it asks here.
    ///
    /// False for a task always, and for an application running its own loop
    /// between messages — which is the state the old two-variant enum could
    /// not tell apart from serving one.
    pub(crate) fn is_on_http(&self) -> bool {
        match &self.execution {
            PhpExecution::Request(_) => true,
            PhpExecution::Program(program) => {
                matches!(program.current, Some(PhpWorkUnit::Request))
            }
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        match &self.execution {
            PhpExecution::Request(_) => {
                SAPI_SHUTTING_DOWN.load(Ordering::Acquire)
            }
            PhpExecution::Program(program) => {
                program.cancellation.is_cancelled()
            }
        }
    }

    pub(crate) fn append_output(
        &mut self,
        data: &[u8],
    ) -> bool {
        if self.request_finished {
            return false;
        }

        let request = match &self.execution {
            PhpExecution::Request(request) => request,
            PhpExecution::Program(program) => {
                if program.model.uses_console_output() {
                    return self
                        .write_program_output(false, data)
                        .unwrap_or(true);
                }
                return true;
            }
        };
        match &request.body_sink {
            BodySink::Streaming(sink) => {
                // Held or streamed, as the reply's head and the policy say
                // (`StreamingSink::write`). A streamed write is a bounded
                // send: returns false on receiver-closed OR
                // when the route's `responses.write_timeout` expires (the
                // transport went silent but didn't drop the body
                // receiver — classic browser-refresh leak). The
                // caller (`ub_write` extern) maps `false` to
                // `php_handle_aborted_connection`, so PHP's
                // `connection_aborted()` flips to true and the
                // streaming `while` loop exits at the next
                // iteration. Without the deadline, PHP sits forever
                // in `blocking_send` and the SAPI thread leaks.
                sink.write(&request.output, data)
            }
        }
    }

    /// PHP's `flush()`: a held reply becomes a stream so what the script
    /// wrote reaches the client now.
    pub(crate) fn php_flush(&mut self) -> bool {
        if !self.flush_output() {
            return false;
        }
        match &self.execution {
            PhpExecution::Request(request) => match &request.body_sink {
                BodySink::Streaming(sink) => sink.flush(&request.output),
            },
            PhpExecution::Program(_) => true,
        }
    }

    pub(crate) fn append_header(
        &mut self,
        name: &str,
        value: &str,
    ) {
        let Some(output) = self.response_output() else {
            return;
        };
        let Ok(header_name) = HeaderName::try_from(name.to_ascii_lowercase())
        else {
            return;
        };
        let Ok(header_value) = HeaderValue::from_str(value) else {
            return;
        };

        if let Ok(mut output) = output.lock() {
            output.headers.append(header_name, header_value);
        }
    }

    pub(crate) fn set_status_from_sapi(&mut self) {
        let code = SapiGlobals::get().sapi_headers().http_response_code;
        let Ok(code) = u16::try_from(code) else {
            return;
        };
        let Ok(status) = StatusCode::from_u16(code) else {
            return;
        };

        if let Some(output) = self.response_output()
            && let Ok(mut output) = output.lock()
        {
            output.status = status;
        }
    }

    pub(crate) fn flush_output(&mut self) -> bool {
        match &self.execution {
            PhpExecution::Program(program)
                if program.model.uses_console_output() =>
            {
                self.flush_program_output(false).unwrap_or(true)
            }
            PhpExecution::Program(_) => true,
            PhpExecution::Request(_) => {
                self.set_status_from_sapi();
                true
            }
        }
    }

    pub(crate) fn response_output(
        &self
    ) -> Option<&Arc<Mutex<CapturedResponse>>> {
        match &self.execution {
            PhpExecution::Request(request) => Some(&request.output),
            PhpExecution::Program(_) => None,
        }
    }

    /// Structured identity for diagnostics emitted by a persistent application.
    ///
    /// Worker identity belongs to the process-wide logging layer. Keeping only
    /// the application-local fields here avoids copying that global scope into
    /// every event while still making one PHP instance unambiguous in journald.
    pub(crate) fn application_log_scope(&self) -> Option<(&str, u8)> {
        match &self.execution {
            PhpExecution::Program(program)
                if program.model == ProgramModel::Application =>
            {
                Some((&program.name, program.instance))
            }
            PhpExecution::Program(_) | PhpExecution::Request(_) => None,
        }
    }

    /// Write one embedded task console stream without changing process-global
    /// file descriptors. `None` means this execution is not a task.
    pub(crate) fn write_program_output(
        &self,
        error: bool,
        bytes: &[u8],
    ) -> Option<bool> {
        let PhpExecution::Program(program) = &self.execution else {
            return None;
        };
        if !program.model.uses_console_output() {
            return None;
        }

        #[cfg(feature = "tasks")]
        if let Some(output) = &program.output {
            return Some(if error {
                output.write_stderr(bytes)
            } else {
                output.write_stdout(bytes)
            });
        }

        use std::io::Write;
        Some(if error {
            std::io::stderr().lock().write_all(bytes).is_ok()
        } else {
            std::io::stdout().lock().write_all(bytes).is_ok()
        })
    }

    pub(crate) fn flush_program_output(
        &self,
        error: bool,
    ) -> Option<bool> {
        let PhpExecution::Program(program) = &self.execution else {
            return None;
        };
        if !program.model.uses_console_output() {
            return None;
        }

        #[cfg(feature = "tasks")]
        if let Some(output) = &program.output {
            return Some(if error {
                output.flush_stderr()
            } else {
                output.flush_stdout()
            });
        }

        use std::io::Write;
        Some(if error {
            std::io::stderr().lock().flush().is_ok()
        } else {
            std::io::stdout().lock().flush().is_ok()
        })
    }

    // HTTP/2/3 split-Cookie folding — delegates to the shared HTTP
    // helper so embed and fcgi don't drift.
    pub(crate) fn cookie_header(&self) -> Option<String> {
        match &self.execution {
            PhpExecution::Request(request) => {
                claviron_http::resolvers::join_cookie_header(
                    &request.parts.headers,
                )
            }
            PhpExecution::Program(_) => None,
        }
    }
}

impl ServerContext for ClavironContext {
    fn request_time(&self) -> Option<std::time::SystemTime> {
        match &self.execution {
            PhpExecution::Request(request) => request
                .parts
                .extensions
                .get::<claviron_http::ext::ReceivedAt>()
                .map(|at| at.0),
            PhpExecution::Program(_) => None,
        }
    }

    fn init_sapi_globals(&mut self) -> Result<(), NulError> {
        let mut sapi_globals = SapiGlobals::get_mut();

        if let PhpExecution::Program(program) = &self.execution {
            let script = &program.script;
            sapi_globals.sapi_headers.http_response_code =
                StatusCode::OK.as_u16() as c_int;
            sapi_globals.request_info.request_method = std::ptr::null();
            sapi_globals.request_info.query_string = std::ptr::null_mut();
            sapi_globals.request_info.path_translated =
                CString::new(script.as_os_str().as_encoded_bytes())?.into_raw();
            sapi_globals.request_info.request_uri = std::ptr::null_mut();
            sapi_globals.request_info.content_length = 0;
            sapi_globals.request_info.content_type = std::ptr::null();
            sapi_globals.request_info.proto_num = 0;
            return Ok(());
        }
        let PhpExecution::Request(request) = &self.execution else {
            unreachable!()
        };

        sapi_globals.sapi_headers.http_response_code =
            StatusCode::OK.as_u16() as c_int;
        sapi_globals.request_info.request_method =
            CString::new(request.parts.method.as_str())?
                .into_raw()
                .cast_const();
        sapi_globals.request_info.query_string =
            if request.sapi_req.query_string.is_empty() {
                std::ptr::null_mut()
            } else {
                CString::new(request.sapi_req.query_string.as_str())?.into_raw()
            };
        sapi_globals.request_info.path_translated = CString::new(
            request.sapi_req.script_file.to_string_lossy().as_bytes(),
        )?
        .into_raw();
        sapi_globals.request_info.request_uri =
            CString::new(request.sapi_req.request_uri.as_str())?.into_raw();
        sapi_globals.request_info.content_length =
            request.request_body.content_length(&request.parts.headers);
        sapi_globals.request_info.content_type = request
            .parts
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(CString::new)
            .transpose()?
            .map(|value| value.into_raw().cast_const())
            .unwrap_or_else(std::ptr::null);
        sapi_globals.request_info.proto_num = match request.parts.version {
            Version::HTTP_09 => 900,
            Version::HTTP_10 => 1000,
            Version::HTTP_11 => 1100,
            Version::HTTP_2 => 2000,
            Version::HTTP_3 => 3000,
            _ => 1100,
        };

        if let Some(auth) = request.parts.headers.get("Authorization")
            && let Ok(auth) = CString::new(auth.as_bytes())
        {
            unsafe {
                php_handle_auth_data(auth.as_ptr());
            }
        }

        Ok(())
    }

    fn read_post(
        &mut self,
        buffer: *mut c_char,
        to_read: usize,
    ) -> usize {
        match &mut self.execution {
            PhpExecution::Request(request) => {
                request.request_body.read_post(buffer, to_read)
            }
            PhpExecution::Program { .. } => 0,
        }
    }

    fn is_request_finished(&self) -> bool {
        self.request_finished
    }

    fn finish_request(&mut self) -> bool {
        if self.request_finished {
            return false;
        }

        // Mark finished BEFORE calling `php_output_end_all` — that
        // function flushes any buffered output by calling our `ub_write`
        // SAPI callback, which checks `is_request_finished()` and
        // short-circuits to a 0-byte return when true. Without this
        // ordering, an `ub_write` during shutdown sees the (already-
        // dropped) body_rx, returns 0, and triggers
        // `handle_abort_connection` / `zend_bailout` from inside
        // `php_request_shutdown` — re-entering PHP's own cleanup
        // bailout context and stalling the worker thread (the PHP
        // exit log fires but `run_request` never returns, leaving
        // `busy=true` forever). Set the flag first → ub_write skips
        // the abort path → shutdown completes cleanly.
        self.request_finished = true;
        unsafe {
            php_output_end_all();
        }
        let flushed = self.flush_output();
        // Streaming path: if PHP returned without ever writing a body
        // byte, the headers oneshot still needs to fire so the caller
        // can build a Response (empty body, current status). Dropping
        // body_tx here also signals end-of-stream to the receiver — a
        // body-less response naturally falls out.
        if let PhpExecution::Request(request) = &self.execution
            && let BodySink::Streaming(sink) = &request.body_sink
        {
            sink.finish(&request.output);
        }
        flushed
    }
}

#[cfg(test)]
#[path = "../../tests/engine/context.rs"]
mod tests;
