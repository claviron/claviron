use std::io;
use std::os::unix::process::CommandExt;
use std::process::Command;

use crate::{Error, Result, WorkerBootstrap};

/// Prepares the command for a supervisor-owned worker process.
///
/// Bind one implementation before starting the runtime. Without a binding,
/// the supervisor uses [`CurrentExecutable`]. This is a code-level choice,
/// independent of Edge and supervisor configuration.
///
/// ```ignore
/// use claviron::supervisor::WorkerLauncher;
///
/// state.registry_ref().insert(std::sync::Arc::new(MyLauncher))?;
/// state.registry_ref().bind_dyn::<dyn WorkerLauncher, MyLauncher>(|launcher| launcher)?;
/// ```
///
/// The supervisor applies the bootstrap and inherited descriptors after this
/// call, then spawns and owns the child. The executable must participate in
/// the worker bootstrap, readiness, heartbeat and drain protocols; selecting
/// another executable does not itself provide a foreign-language lifecycle ABI.
pub trait WorkerLauncher: Send + Sync {
    /// Choose the executable, arguments, environment and working directory.
    ///
    /// This synchronous hook runs while the supervisor holds the worker table
    /// lock. Only prepare a command: do not spawn a process or wait for worker
    /// readiness here. An error fails this launch without falling back to the
    /// current executable.
    fn prepare(
        &self,
        worker: &WorkerBootstrap,
    ) -> io::Result<Command>;
}

/// Default launcher: re-execute the current binary without forwarding CLI args.
#[derive(Clone, Copy, Debug, Default)]
pub struct CurrentExecutable;

impl WorkerLauncher for CurrentExecutable {
    fn prepare(
        &self,
        worker: &WorkerBootstrap,
    ) -> io::Result<Command> {
        let executable = std::env::current_exe()?;
        let name = executable
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("claviron");
        let mut command = Command::new(&executable);
        command.arg0(format!("{name} [worker #{}]", worker.worker_id()));
        Ok(command)
    }
}

pub(super) fn prepare_command(
    launcher: Option<&dyn WorkerLauncher>,
    bootstrap: &WorkerBootstrap,
) -> Result<Command> {
    let mut command = launcher
        .unwrap_or(&CurrentExecutable)
        .prepare(bootstrap)
        .map_err(|error| {
            Error::worker(
                bootstrap.worker_id(),
                format!("failed to prepare worker command: {error}"),
            )
        })?;
    // The process owner supplies the authoritative envelope, even if the
    // custom command clears the environment or sets this reserved key.
    bootstrap.apply_to(&mut command)?;
    Ok(command)
}

#[cfg(test)]
#[path = "../../tests/process/launcher.rs"]
mod tests;
