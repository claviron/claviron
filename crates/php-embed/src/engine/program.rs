//! Common execution primitive for persistent PHP programs. Daemon placement
//! and application request dispatch own separate lifecycles above this layer.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CString, c_char};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ext_php_rs::embed::EmbedError;
use ext_php_rs::ffi::_zend_compiler_globals as CompilerGlobals;
use ext_php_rs::zend::{ExecutorGlobals, SapiGlobals};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{ClavironContext, PhpExecution};
use crate::sapi::callbacks::{
    php_request_logged_error, reset_php_request_error_flag,
};
use crate::sapi::run_execution_scope;

/// Give this program its own working directory.
///
/// A thread can have one. PHP's `chdir()` does not touch the process: this
/// build defines `VIRTUAL_DIR` (ZTS does, `zend_virtual_cwd.h:52`), so
/// `VCWD_CHDIR` is `virtual_chdir` and the result lands in `CWDG(cwd)`, which
/// ZTS reaches through TSRM — thread-local storage. `getcwd()` reads the same
/// place. So two programs in one process can sit in two directories, and
/// neither moves the server.
///
/// Called through PHP rather than through libc for exactly that reason: libc
/// `chdir` is process-wide and would move every thread at once.
fn enter_working_directory(cwd: &Path) -> Result<(), String> {
    // Single quotes, so only a backslash and a quote need escaping.
    let escaped =
        cwd.to_string_lossy().replace('\\', "\\\\").replace('\'', "\\'");
    let snippet = format!("chdir('{escaped}');");
    let (Ok(code), Ok(name)) =
        (CString::new(snippet), CString::new("claviron: working directory"))
    else {
        return Err(format!(
            "working directory is not representable: {}",
            cwd.display()
        ));
    };

    // Null retval and a bailout guard, for the reasons given on
    // `define_standard_streams`.
    let _ = ext_php_rs::zend::try_catch_first(|| {
        // SAFETY: called from the PHP thread inside request scope, after RINIT.
        unsafe {
            crate::sapi::ffi::zend_eval_string(
                code.as_ptr(),
                std::ptr::null_mut(),
                name.as_ptr(),
            );
        }
    });
    Ok(())
}

/// Give a task program the `STDIN` / `STDOUT` / `STDERR` constants.
///
/// PHP defines these in the **CLI SAPI only**; an embedded SAPI gets none of
/// them. A task run here is console-shaped by nature, and console code
/// reaches for them without checking — Laravel's error renderer (Collision)
/// dies on `Undefined constant "NunoMaduro\Collision\STDOUT"` while trying to
/// print a different error, so the report about the first failure is lost to a
/// second one.
///
/// Defined through PHP's own `fopen`, as `php_cli.c` does, rather than by
/// building stream resources from Rust: the value of these constants is a
/// stream, and PHP is the thing that knows how to make one. Guarded by
/// `defined()` so anything that already registered them wins.
///
/// Tasks and caller-owned console commands get them. A script-model request
/// is an HTTP request, and an application is a request server with an explicit
/// response function; neither has a terminal or should grow one.
fn define_standard_streams(model: super::ProgramModel) {
    #[cfg(feature = "tasks")]
    const SNIPPET: &str = r#"
        if (!class_exists('__ClavironTaskOutputStream', false)) {
            final class __ClavironTaskOutputStream {
                public mixed $context = null;
                private string $stream = 'stdout';
                public function stream_open($path, $mode, $options, &$opened_path): bool {
                    $this->stream = parse_url($path, PHP_URL_HOST) ?: 'stdout';
                    return $this->stream === 'stdout' || $this->stream === 'stderr';
                }
                public function stream_write($data): int {
                    return claviron_task_output_write($this->stream, $data);
                }
                public function stream_flush(): bool {
                    return claviron_task_output_flush($this->stream);
                }
                public function stream_stat(): array { return []; }
                public function stream_set_option($option, $arg1, $arg2): bool { return false; }
            }
        }
        if (!in_array('claviron-task-output', stream_get_wrappers(), true)) {
            stream_wrapper_register('claviron-task-output', __ClavironTaskOutputStream::class);
        }
        if (!defined('STDIN')) define('STDIN', fopen('php://stdin', 'r'));
        if (!defined('STDOUT')) define('STDOUT', fopen('claviron-task-output://stdout', 'w'));
        if (!defined('STDERR')) define('STDERR', fopen('claviron-task-output://stderr', 'w'));
    "#;
    #[cfg(not(feature = "tasks"))]
    const SNIPPET: &str = "\
        if (!defined('STDIN')) define('STDIN', fopen('php://stdin', 'r'));\
        if (!defined('STDOUT')) define('STDOUT', fopen('php://stdout', 'w'));\
        if (!defined('STDERR')) define('STDERR', fopen('php://stderr', 'w'));";

    // A standalone console owns the terminal, even in a build with TaskService.
    // Task output wrappers remain exclusive to managed task occurrences.
    let snippet = if model == super::ProgramModel::Console {
        "if (!defined('STDIN')) define('STDIN', fopen('php://stdin', 'r'));\
         if (!defined('STDOUT')) define('STDOUT', fopen('php://stdout', 'w'));\
         if (!defined('STDERR')) define('STDERR', fopen('php://stderr', 'w'));"
    } else {
        SNIPPET
    };
    let (Ok(code), Ok(name)) =
        (CString::new(snippet), CString::new("claviron: standard streams"))
    else {
        return;
    };

    // A null retval matters: given one, `zend_eval_stringl` wraps the source in
    // `return <code>;`, which turns these statements into a parse error. We
    // want the statements run, not their value.
    //
    // Wrapped, because a bailout here would longjmp past the caller's
    // `started.send` and leave the supervisor waiting forever on a program that
    // had in fact begun — which is exactly how the first version of this failed.
    let _ = ext_php_rs::zend::try_catch_first(|| {
        // SAFETY: called from the PHP thread inside request scope, after RINIT,
        // so the executor and the stream layer are both up.
        unsafe {
            crate::sapi::ffi::zend_eval_string(
                code.as_ptr(),
                std::ptr::null_mut(),
                name.as_ptr(),
            );
        }
    });
}

pub(crate) struct PhpProgram {
    pub(super) name: String,
    pub(super) script: PathBuf,
    pub(super) args: Vec<String>,
    /// Where this program's thread sits while it runs. Thread-local — see
    /// [`enter_working_directory`].
    pub(super) cwd: Option<PathBuf>,
    pub(super) environment: BTreeMap<String, String>,
    /// Per-execution deadline. `None` is unbounded, which every daemon task is;
    /// a launched program gets the value `php.yaml launch.execution_timeout`
    /// resolved to.
    pub(super) budget: Option<std::time::Duration>,
    #[cfg(feature = "tasks")]
    pub(super) output: Option<claviron_tasks::TaskOutput>,
}

impl PhpProgram {
    pub(crate) fn new(
        name: String,
        script: PathBuf,
        args: Vec<String>,
        cwd: Option<PathBuf>,
        environment: BTreeMap<String, String>,
        budget: Option<std::time::Duration>,
    ) -> Self {
        Self {
            name,
            script,
            args,
            cwd,
            environment,
            budget,
            #[cfg(feature = "tasks")]
            output: None,
        }
    }

    #[cfg(feature = "tasks")]
    pub(super) fn with_output(
        mut self,
        output: claviron_tasks::TaskOutput,
    ) -> Self {
        self.output = Some(output);
        self
    }
}

/// Own the native argv backing storage through PHP execution and shutdown.
/// The pointer table never changes after installation; all state is ZTS-local.
struct ProgramArguments {
    _values: Vec<CString>,
    _pointers: Vec<*mut c_char>,
}

impl ProgramArguments {
    fn install(
        script: &Path,
        args: &[String],
    ) -> Result<Self, String> {
        let argc =
            i32::try_from(args.len().saturating_add(1)).map_err(|_| {
                "too many persistent PHP script arguments".to_string()
            })?;
        let values = std::iter::once(script.as_os_str().as_encoded_bytes())
            .chain(args.iter().map(|arg| arg.as_bytes()))
            .map(CString::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| {
                "persistent PHP script arguments contain a NUL byte".to_string()
            })?;
        let mut pointers = values
            .iter()
            .map(|value| value.as_ptr().cast_mut())
            .collect::<Vec<_>>();
        pointers.push(std::ptr::null_mut());
        {
            let mut globals = SapiGlobals::get_mut();
            globals.request_info.argc = argc;
            globals.request_info.argv = pointers.as_mut_ptr();
        }
        Ok(Self { _values: values, _pointers: pointers })
    }
}

impl Drop for ProgramArguments {
    fn drop(&mut self) {
        let mut globals = SapiGlobals::get_mut();
        globals.request_info.argc = 0;
        globals.request_info.argv = std::ptr::null_mut();
    }
}

pub(super) fn run(
    program: PhpProgram,
    model: super::ProgramModel,
    instance: u8,
    cancellation: CancellationToken,
    started: oneshot::Sender<Result<(), String>>,
    owner: super::interrupt::ExecutionOwner,
) -> Result<(), String> {
    run_program(program, model, instance, cancellation, started, owner)
        .map(|_| ())
}

pub(crate) fn run_console(
    program: PhpProgram,
    cancellation: CancellationToken,
) -> Result<i32, String> {
    let (started, _) = oneshot::channel();
    run_program(
        program,
        super::ProgramModel::Console,
        0,
        cancellation,
        started,
        super::interrupt::ExecutionOwner::next(),
    )
}

fn run_program(
    program: PhpProgram,
    model: super::ProgramModel,
    instance: u8,
    cancellation: CancellationToken,
    started: oneshot::Sender<Result<(), String>>,
    owner: super::interrupt::ExecutionOwner,
) -> Result<i32, String> {
    if let Err(err) = super::ensure_sapi() {
        let err = format!("failed to init SAPI: {err}");
        let _ = started.send(Err(err.clone()));
        return Err(err);
    }
    let _thread_context = super::thread::PhpThreadContext;
    if let Err(err) = super::thread::bind_php_thread(
        format!("{}:{}:{instance}", model.as_str(), program.name),
        owner,
    ) {
        let _ = started.send(Err(err.clone()));
        return Err(err);
    }
    let _arguments =
        match ProgramArguments::install(&program.script, &program.args) {
            Ok(arguments) => arguments,
            Err(error) => {
                let _ = started.send(Err(error.clone()));
                return Err(error);
            }
        };
    let mut started = Some(started);
    let script = program.script;
    let cwd = program.cwd;
    let context = ClavironContext {
        execution: PhpExecution::Program(super::PhpProgramContext {
            model,
            name: program.name,
            instance,
            script: script.clone(),
            environment: program.environment,
            cancellation: cancellation.clone(),
            current: None,
            #[cfg(feature = "tasks")]
            output: program.output,
        }),
        event_subscription: None,
        #[cfg(feature = "tasks")]
        invocation_subscription: Mutex::new(None),
        env_overlay: Mutex::new(HashMap::new()),
        request_finished: false,
    };
    reset_php_request_error_flag();
    // A program with no visible units is one long execution: active from the
    // moment it starts until it returns. One that receives them marks each in
    // turn (`functions::application`), because marking the whole loop active
    // would make an idle worker look busy. `receives_work` is that question
    // asked once, instead of the `bool` this used to take alongside a `&str`
    // that said the same thing again.
    let _active =
        (!model.receives_work()).then(super::interrupt::Active::enter);
    // Armed here and not by the caller: the deadline lives in this thread's
    // interrupt registration, which `_thread_context` made a few lines up, so
    // an earlier arm would have found no thread to arm. Held in this frame,
    // outside the `try_catch` below -- a bailout is a longjmp and skips any
    // guard living inside it. This is the `--timeout` a CLI worker had from
    // `pcntl_alarm`, given by the engine that owns the thread.
    let _budget = program.budget.map(super::interrupt::Budget::arm);
    let mut ini_budget = None;
    let result = run_execution_scope(
        context,
        || {
            if model == super::ProgramModel::Console {
                ini_budget = super::interrupt::Budget::from_php();
                // PHP's SAPI_OPTION_NO_CHDIR (main/SAPI.h) is an ABI flag,
                // not policy: preserve the caller's declared console cwd
                // instead of php_execute_script switching to the runner folder.
                const SAPI_OPTION_NO_CHDIR: i32 = 1;
                SapiGlobals::get_mut().options |= SAPI_OPTION_NO_CHDIR;
            }
            // Before anything compiles: a console script reads relative paths
            // from the moment it starts, including its own autoloader.
            if let Some(cwd) = cwd.as_deref() {
                let _ = enter_working_directory(cwd);
            }
            if model.uses_console_output() {
                define_standard_streams(model);
            }
            // RINIT resets compiler globals. Persistent console-style scripts
            // need shebang handling and native argv before compilation.
            CompilerGlobals::get_mut().skip_shebang = true;
            unsafe {
                crate::sapi::ffi::php_build_argv(
                    std::ptr::null(),
                    std::ptr::null_mut(),
                );
            }
            if let Some(started) = started.take() {
                let _ = started.send(Ok(()));
            }
        },
        || {
            let executed = ext_php_rs::embed::Embed::run_script(&script);
            let status = ExecutorGlobals::get().exit_status;
            // Capture before RSHUTDOWN releases Zend state. A deliberate
            // nonzero console exit is a command outcome, not an engine error.
            let failed = executed.is_err_and(|err| {
                !matches!(err, EmbedError::ExecuteScriptError)
                    || php_request_logged_error()
                    || ExecutorGlobals::has_exception()
                    || (model != super::ProgramModel::Console && status != 0)
            });
            if failed {
                Err("PHP program execution failed".to_owned())
            } else {
                Ok(status)
            }
        },
    )
    .map_err(|err| err.to_string())
    .and_then(std::convert::identity);
    let result = if php_request_logged_error() && result.is_ok() {
        Err("PHP reported a script error".to_string())
    } else {
        result
    };
    if let Some(started) = started {
        let _ = started.send(result.clone().map(|_| ()));
    }
    result
}
