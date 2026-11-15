use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use claviron_runtime::{AppState, WorkerCount, WorkerId};
use tracing::debug;

use crate::control::{WorkerControl, WorkerProcess};
use crate::lifecycle::{OwnedProcess, WorkerIntent, WorkerPhase};
use crate::{Error, Result, WorkerBootstrap};

/// Re-execute the current binary with the private worker bootstrap.
/// Caller MUST hold the fleet table write lock from before this call until
/// installing the returned row: a fast child's acknowledgement must not race
/// ahead of its authoritative process identity.
pub(crate) fn spawn_worker(
    control: &WorkerControl,
    id: WorkerId,
    incarnation: u64,
    worker_count: u8,
    state: &AppState,
) -> Result<WorkerProcess> {
    // SHUTDOWN_HAS_PRECEDENCE: nothing is spawned after the latch, whatever
    // operation reached this point.
    if control.is_shutting_down() {
        return Err(Error::service(format!(
            "worker #{id} is not spawned: the fleet is shutting down"
        )));
    }
    let config_dir = state.config_dir();

    let bootstrap = WorkerBootstrap::new(
        id,
        claviron_runtime::FleetNodeId::new(u16::from(id.raw())),
        WorkerCount::new(worker_count),
        state.fleet_capacity(),
        incarnation,
        control.master()?,
        state.name(),
        state.fleet_name(),
        config_dir,
        // Workers re-execute without CLI arguments. Carry the options
        // the operator chose in the bootstrap.
        state
            .registry_ref()
            .resolve::<claviron_runtime::LaunchBlob>()
            .map(|blob| (*blob).clone())
            .unwrap_or_default(),
    )?
    .with_environment(state.environment())?;
    let resources = claviron_runtime::process::ProcessResources::register(
        state,
    )
    .map_err(|error| {
        Error::service(format!("prepare worker resources: {error}"))
    })?;
    let descriptors = resources.for_child().map_err(|error| {
        Error::service(format!("prepare worker descriptors: {error}"))
    })?;
    let bootstrap = bootstrap.with_descriptors(descriptors.manifest());
    let executable = std::env::current_exe().map_err(|error| {
        Error::worker(id, format!("failed to prepare worker command: {error}"))
    })?;
    let name = worker_name(&executable);
    let mut cmd = Command::new(worker_program(&executable));
    cmd.arg0(format!("{name} [worker #{id}]"));
    bootstrap.apply_to(&mut cmd)?;
    descriptors.attach(&mut cmd);

    let child = cmd.spawn().map_err(|e| {
        Error::service(format!(
            "Worker #{id} failed to spawn {}: {e}",
            executable.display()
        ))
    })?;

    // Every path that creates a worker process comes through here; the
    // process gets its one exit observer as it is adopted.
    let process = OwnedProcess::from_child(child, Arc::clone(&control.exited))?
        .recover_orbit_on_exit(state, u16::from(id.raw()), incarnation);
    debug!("⚙️  Worker #{id} started with PID {}", process.pid());

    Ok(WorkerProcess::new(
        id,
        Some(process),
        WorkerPhase::Starting,
        WorkerIntent::EnsureRunning,
    )
    .with_incarnation(incarnation))
}

/// Linux marks the executable link of a process whose file was replaced or
/// removed with this suffix.
const DELETED_SUFFIX: &str = " (deleted)";

/// The program a worker executes.
///
/// On Linux this is `/proc/self/exe`, the image the master is running. It
/// stays valid after the file at the master's path was replaced or removed,
/// as an upgrade does while the fleet runs. `current_exe()` reads the same
/// link as text and returns `<path> (deleted)` then, which no file has, so
/// spawning it fails with ENOENT; spawning the replacement file instead
/// would start a worker from another build than its master.
#[cfg(target_os = "linux")]
fn worker_program(_executable: &Path) -> &Path {
    Path::new("/proc/self/exe")
}

#[cfg(not(target_os = "linux"))]
fn worker_program(executable: &Path) -> &Path {
    executable
}

/// The executable's file name for the worker's `argv[0]`, without the
/// suffix Linux adds once the file is gone.
fn worker_name(executable: &Path) -> &str {
    let name = executable
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("claviron");
    name.strip_suffix(DELETED_SUFFIX).unwrap_or(name)
}
