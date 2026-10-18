use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::Arc;

use claviron_runtime::{AppState, WorkerCount, WorkerId};
use tracing::debug;

use crate::control::{WorkerControl, WorkerProcess};
use crate::lifecycle::{OwnedProcess, WorkerIntent, WorkerPhase};
use crate::{Error, Result, WorkerBootstrap};

/// Spawn a single worker process using the private environment bootstrap
/// protocol. The embedding application's CLI is deliberately not involved:
/// downstream binaries may define any flags or subcommands they need.
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
    let current_exe = std::env::current_exe().map_err(|e| {
        Error::service(format!("failed to get current exe path: {}", e))
    })?;
    let config_dir = state.config_dir();

    let mut cmd = Command::new(&current_exe);
    let executable_name = current_exe
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("claviron");
    cmd.arg0(format!("{executable_name} [worker #{id}]"));
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
        // What the launch decided, carried verbatim. A worker is a re-exec
        // and the command line is deliberately not passed along, so without
        // this a worker would come up on defaults the operator never chose.
        state
            .registry_ref()
            .resolve::<claviron_runtime::LaunchBlob>()
            .map(|blob| (*blob).clone())
            .unwrap_or_default(),
    )?
    .with_environment(state.environment())?;
    bootstrap.apply_to(&mut cmd)?;

    let child = cmd.spawn().map_err(|e| {
        Error::service(format!("Worker #{id} failed to spawn {}", e))
    })?;

    // Every path that creates a worker process comes through here; the
    // process gets its one exit observer as it is adopted.
    let process = OwnedProcess::from_child(child, Arc::clone(&control.exited))?
        .close_links_on_exit(state, incarnation);
    debug!("⚙️  Worker #{id} started with PID {}", process.pid());

    Ok(WorkerProcess::new(
        id,
        Some(process),
        WorkerPhase::Starting,
        WorkerIntent::EnsureRunning,
    )
    .with_incarnation(incarnation))
}
