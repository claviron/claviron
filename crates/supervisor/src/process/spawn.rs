use std::sync::Arc;

use claviron_runtime::{AppState, WorkerCount, WorkerId};
use tracing::debug;

use crate::control::{WorkerControl, WorkerProcess};
use crate::lifecycle::{OwnedProcess, WorkerIntent, WorkerPhase};
use crate::{Error, Result, WorkerBootstrap, WorkerLauncher};

/// Spawn a worker through the registered launcher and private bootstrap.
/// The default launcher does not forward the embedding application's CLI.
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
        // What the launch decided, carried verbatim. The default launcher
        // re-executes without CLI arguments, so workers need this to retain
        // the options the operator chose.
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
    let launcher = state.registry_ref().resolve_dyn::<dyn WorkerLauncher>();
    let mut cmd =
        super::launcher::prepare_command(launcher.as_deref(), &bootstrap)?;
    descriptors.attach(&mut cmd);

    let child = cmd.spawn().map_err(|e| {
        Error::service(format!("Worker #{id} failed to spawn {}", e))
    })?;

    // Every path that creates a worker process comes through here; the
    // process gets its one exit observer as it is adopted.
    let process = OwnedProcess::from_child(child, Arc::clone(&control.exited))?
        .recover_orbit_on_exit(state, incarnation);
    debug!("⚙️  Worker #{id} started with PID {}", process.pid());

    Ok(WorkerProcess::new(
        id,
        Some(process),
        WorkerPhase::Starting,
        WorkerIntent::EnsureRunning,
    )
    .with_incarnation(incarnation))
}
