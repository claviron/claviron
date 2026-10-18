//! What the engine needs to run one named PHP program.
//!
//! This used to carry the whole supervision policy too — instances, dispatch,
//! restart, attempts, three backoff values and a lease TTL. None of it ever
//! reached PHP: `engine::task::run_task_program` builds
//! `PhpProgram::new(name, script, args, environment)` and passes nothing else
//! through. The rest was a second implementation of what `TaskService` already
//! holds for its other executors, and it moved there with the `php` task kind.

use std::collections::BTreeMap;
use std::path::PathBuf;

/// One PHP task occurrence: what to run, with what arguments, in what
/// environment. TaskService owns placement, concurrency, retry and drain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TaskProgram {
    pub(crate) name: String,
    pub(crate) script_path: PathBuf,
    pub(crate) args: Vec<String>,
    /// Where this program's PHP thread sits while it runs, and what a relative
    /// `script_path` is resolved against.
    ///
    /// A thread can have one: under ZTS, PHP's `chdir()` writes the
    /// thread-local virtual CWD rather than the process's, so two programs can
    /// sit in two directories and neither moves the server.
    pub(crate) cwd: Option<PathBuf>,
    pub(crate) environment: BTreeMap<String, String>,
    #[cfg(feature = "tasks")]
    pub(crate) log: claviron_tasks::LogConfig,
    /// A per-execution budget for this program, armed on its thread. `None`
    /// leaves the program unbounded, as every daemon task is.
    pub(crate) execution_timeout: Option<std::time::Duration>,
}

impl TaskProgram {
    /// Construct one instance of a named program. Relative paths use the
    /// application's working directory, never a configuration file directory.
    pub fn new(
        name: impl Into<String>,
        script_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            name: name.into(),
            script_path: script_path.into(),
            args: Vec::new(),
            cwd: None,
            environment: BTreeMap::new(),
            #[cfg(feature = "tasks")]
            log: claviron_tasks::LogConfig::default(),
            execution_timeout: None,
        }
    }

    /// Append literal script arguments, without shell parsing or expansion.
    /// PHP argv[0] is the resolved script path; these values follow it.
    pub fn args(
        mut self,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Run in this directory, and resolve a relative script against it.
    pub fn cwd(
        mut self,
        cwd: impl Into<PathBuf>,
    ) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Bound the whole program, armed on its thread for as long as it runs.
    /// `None`, the default, leaves it unbounded: a daemon runs until it is
    /// stopped, and the runtime's `lifecycle.execution_timeout` is not a
    /// fallback here — that one bounds an application's request or event.
    pub fn execution_timeout(
        mut self,
        budget: Option<std::time::Duration>,
    ) -> Self {
        self.execution_timeout = budget;
        self
    }

    /// Add one program-local environment value.
    pub fn environment(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.environment.insert(key.into(), value.into());
        self
    }

    #[cfg(feature = "tasks")]
    pub fn log(
        mut self,
        log: claviron_tasks::LogConfig,
    ) -> Self {
        self.log = log;
        self
    }
}
