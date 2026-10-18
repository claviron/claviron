//! Caller-owned terminal PHP execution, independent of HTTP and TaskService.

use std::collections::BTreeMap;
use std::path::PathBuf;

use tokio_util::sync::CancellationToken;

use crate::engine::program::{PhpProgram, run_console};

/// One console command. Arguments are literal, streams inherit the terminal,
/// and the PHP exit status is returned after request shutdown and thread join.
/// Host-backed functions still require their capabilities to be bound by the
/// caller before execution. This API boots no services and joins no fleet.
#[derive(Clone, Debug)]
pub struct PhpConsole {
    pub script: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub cancellation: CancellationToken,
}

impl PhpConsole {
    /// Blocks until command completion. Run off Tokio's executor threads.
    /// PHP's declared INI execution budget applies; no timeout is invented.
    pub fn run(self) -> Result<i32, String> {
        if self.cancellation.is_cancelled() {
            return Err("PHP console execution cancelled".into());
        }
        let cwd = std::fs::canonicalize(&self.cwd)
            .map_err(|error| format!("PHP console cwd: {error}"))?;
        let script = if self.script.is_absolute() {
            self.script
        } else {
            cwd.join(self.script)
        };
        let script = std::fs::canonicalize(&script).map_err(|error| {
            format!("PHP console script {}: {error}", script.display())
        })?;
        let program = PhpProgram::new(
            script.display().to_string(),
            script,
            self.args,
            Some(cwd),
            self.environment,
            None,
        );
        std::thread::Builder::new()
            .name("php-console".into())
            .spawn(move || run_console(program, self.cancellation))
            .map_err(|error| error.to_string())?
            .join()
            .map_err(|_| "PHP console thread panicked".to_owned())?
    }
}
