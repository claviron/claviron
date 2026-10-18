//! Evaluate a PHP document without AppState, an HTTP request or a service pool.
//! Shares the process SAPI with hosted PHP; never starts a second embed engine.

use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ext_php_rs::convert::IntoZval;
use ext_php_rs::types::{ZendStr, Zval};
use ext_php_rs::zend::{ExecutorGlobals, try_catch};
use tokio_util::sync::CancellationToken;

use crate::engine::context::{
    ClavironContext, PhpExecution, PhpProgramContext, ProgramModel,
};
use crate::engine::interrupt::{Active, Budget, ExecutionOwner};
use crate::engine::thread::{PhpThreadContext, bind_php_thread};
use crate::sapi::run_execution_scope;

mod value;
pub use crate::engine::startup::PhpIni;
pub use value::{PhpArrayKey, PhpValue};

/// Optional PHP bootstrap returning a callable `(argument, environment): void`.
/// Its source is trusted application code, just like the evaluated document.
#[derive(Clone, Debug)]
pub struct PhpBootstrap {
    pub path: PathBuf,
    pub argument: String,
}

/// Independent one-shot evaluator. Each call has its own OS/ZTS thread and
/// Zend request heap; only owned Rust data leaves it. It does not provide
/// Laravel application boot, an HTTP context, or runtime services.
#[derive(Clone, Debug)]
pub struct PhpEvaluator {
    bootstrap: Option<PhpBootstrap>,
    max_depth: usize,
    cancellation: CancellationToken,
}

impl PhpEvaluator {
    /// `max_depth` bounds returned array nesting; no hidden execution deadline.
    /// Execution uses the already declared host/INI budget, if one exists.
    pub fn new(max_depth: usize) -> Self {
        Self {
            bootstrap: None,
            max_depth,
            cancellation: CancellationToken::new(),
        }
    }

    /// Declare process INI locations during cold composition, before evaluation.
    /// If a PhpEmbedRuntime is registered, its declaration must agree.
    ///
    /// # Safety
    /// PHP selects its scan directory from the process environment. The caller
    /// must ensure no other thread/foreign code accesses that environment while
    /// these settings are declared. Prefer declaring PHPRC/PHP_INI_SCAN_DIR at
    /// process launch when this single-threaded composition cannot be assured.
    pub unsafe fn with_ini(
        self,
        ini: PhpIni,
    ) -> Result<Self, String> {
        crate::engine::startup::declare(ini)?;
        Ok(self)
    }

    pub fn with_bootstrap(
        mut self,
        bootstrap: PhpBootstrap,
    ) -> Self {
        self.bootstrap = Some(bootstrap);
        self
    }

    pub fn with_cancellation(
        mut self,
        cancellation: CancellationToken,
    ) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Execute the supplied source snapshot with its original filename for
    /// `__FILE__`, `__DIR__` and diagnostics. Never rereads that file or relies
    /// on OPcache's earlier compiled version. This call blocks until PHP ends.
    pub fn evaluate_source(
        &self,
        path: &Path,
        source: &[u8],
        environment: &BTreeMap<String, String>,
    ) -> Result<PhpValue, String> {
        if self.cancellation.is_cancelled() {
            return Err("PHP evaluation cancelled".into());
        }
        std::thread::scope(|scope| {
            let thread = std::thread::Builder::new()
                .name("php-evaluate".into())
                .spawn_scoped(scope, || {
                    self.evaluate_on_thread(path, source, environment)
                })
                .map_err(|error| error.to_string())?;
            thread
                .join()
                .map_err(|_| "PHP evaluation thread panicked".to_owned())?
        })
    }

    fn evaluate_on_thread(
        &self,
        path: &Path,
        source: &[u8],
        environment: &BTreeMap<String, String>,
    ) -> Result<PhpValue, String> {
        let path =
            std::path::absolute(path).map_err(|error| error.to_string())?;
        crate::engine::ensure_sapi().map_err(|error| error.to_string())?;
        let _thread = PhpThreadContext;
        bind_php_thread("evaluate", ExecutionOwner::next())?;
        let context = ClavironContext {
            execution: PhpExecution::Program(PhpProgramContext {
                model: ProgramModel::Evaluation,
                name: path.display().to_string(),
                instance: 0,
                script: path.clone(),
                environment: environment.clone(),
                cancellation: self.cancellation.clone(),
                current: None,
                #[cfg(feature = "tasks")]
                output: None,
            }),
            event_subscription: Mutex::new(None),
            #[cfg(feature = "tasks")]
            invocation_subscription: Mutex::new(None),
            env_overlay: Mutex::new(HashMap::new()),
            request_finished: false,
        };
        let _active = Active::enter();
        // Armed after RINIT reads php.ini and held outside Zend's longjmp frame.
        let mut budget = None;
        run_execution_scope(
            context,
            || {
                budget = Budget::from_php();
            },
            || {
                if let Some(bootstrap) = &self.bootstrap {
                    prepare(bootstrap, environment)?;
                }
                if self.cancellation.is_cancelled() {
                    return Err("PHP evaluation cancelled".into());
                }
                let value = execute_source(&path, source)?;
                value::decode(&value, self.max_depth)
            },
        )
        .map_err(|error| format!("PHP evaluation failed: {error:?}"))?
    }
}

/// Compile the exact document bytes and capture its top-level return. Each FFI
/// boundary has its own bailout frame; native values stay in the outer frame
/// so a PHP exit/fatal cannot skip their Rust destructors.
fn execute_source(
    path: &Path,
    bytes: &[u8],
) -> Result<Zval, String> {
    let filename = CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|error| error.to_string())?;
    let mut source = ZendStr::new(bytes, false);
    let mut value = Zval::new();
    let compile = unsafe { ext_php_rs::ffi::zend_compile_string }
        .ok_or("Zend compiler is unavailable")?;
    let op_array = try_catch(std::panic::AssertUnwindSafe(|| {
        // SAFETY: current thread owns an active PHP request and these buffers.
        unsafe { compile(std::ptr::from_mut(&mut *source).cast(), filename.as_ptr(), ext_php_rs::ffi::_zend_compile_position_ZEND_COMPILE_POSITION_AT_SHEBANG) }
    })).map_err(|error| error.to_string())?;
    if op_array.is_null() {
        return Err(exception_message("PHP document did not compile"));
    }
    let executed = try_catch(std::panic::AssertUnwindSafe(|| {
        // SAFETY: op_array was compiled above on the current Zend heap.
        unsafe { ext_php_rs::ffi::zend_execute(op_array, &raw mut value) };
    }));
    // SAFETY: compile_string returns an owned, non-cached op_array.
    unsafe {
        ext_php_rs::ffi::destroy_op_array(op_array);
        ext_php_rs::ffi::_efree(op_array.cast());
    }
    executed.map_err(|error| error.to_string())?;
    if ExecutorGlobals::has_exception() {
        return Err(exception_message("PHP document threw an exception"));
    }
    Ok(value)
}

fn exception_message(fallback: &str) -> String {
    if let Some(mut exception) = ExecutorGlobals::take_exception() {
        let mut temporary = Zval::new();
        let object = std::ptr::from_mut(&mut *exception)
            .cast::<ext_php_rs::ffi::zend_object>();
        // Throwable's message is protected. Reading with the declaring class
        // scope avoids the public-property accessor and executes no user method.
        let message = try_catch(std::panic::AssertUnwindSafe(|| {
            // SAFETY: this live Throwable and rv belong to the current request.
            unsafe {
                ext_php_rs::ffi::zend_read_property(
                    (*object).ce,
                    object,
                    c"message".as_ptr(),
                    7,
                    true,
                    &raw mut temporary,
                )
                .cast::<Zval>()
                .as_ref()
                .and_then(|value| value.str())
                .map(str::to_owned)
            }
        }))
        .ok()
        .flatten()
        .unwrap_or_else(|| fallback.to_owned());
        format!("{fallback}: {message}")
    } else {
        fallback.to_owned()
    }
}

fn prepare(
    bootstrap: &PhpBootstrap,
    environment: &BTreeMap<String, String>,
) -> Result<(), String> {
    let source = std::fs::read(&bootstrap.path).map_err(|error| {
        format!("PHP bootstrap {}: {error}", bootstrap.path.display())
    })?;
    let path = std::path::absolute(&bootstrap.path)
        .map_err(|error| error.to_string())?;
    let mut callable = execute_source(&path, &source)?;
    if !callable.is_callable() {
        return Err("PHP bootstrap must return a callable".into());
    }
    let mut parameters = [
        bootstrap
            .argument
            .clone()
            .into_zval(false)
            .map_err(|error| error.to_string())?,
        environment
            .clone()
            .into_zval(false)
            .map_err(|error| error.to_string())?,
    ];
    let mut result = Zval::new();
    let status = try_catch(std::panic::AssertUnwindSafe(|| {
        // SAFETY: callable, parameter buffer and retval belong to this request.
        unsafe {
            ext_php_rs::ffi::_call_user_function_impl(
                std::ptr::null_mut(),
                &raw mut callable,
                &raw mut result,
                parameters.len() as u32,
                parameters.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        }
    }))
    .map_err(|error| error.to_string())?;
    if ExecutorGlobals::has_exception() {
        return Err(exception_message("PHP bootstrap threw an exception"));
    }
    if status < 0 {
        return Err("PHP bootstrap callable failed".into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/evaluate.rs"]
mod tests;
