use claviron_runtime::{AppState, ReopenLogs, RunMode};
use claviron_sys::signal::{ServerSignals, Signal as OperatorSignal};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::{Error, WorkerManager, shutdown};

/// Main-process shutdown listener task for Master/Standalone mode.
///
/// Why this is separate from the reload listener:
/// - `reload_all()` may restart workers and can take time.
/// - Keeping shutdown (SIGINT/SIGTERM) in its own task ensures shutdown signals are handled
///   immediately even while a reload is in progress.
///
/// Listens to SIGINT (Ctrl-C) and SIGTERM.
async fn listen_shutdown_for_main_process(state: AppState) {
    let mut terminate_signal = signal(SignalKind::terminate())
        .expect("Failed to create terminate signal handler");

    tokio::select! {
        _ = state.on_shutdown() => {
            debug!("🔸 shutdown listener stopped (state shutdown)");
            return;
        }
        _ = tokio::signal::ctrl_c() => {
            debug!("🚫 Ctrl-C received, initiating shutdown");
        }
        _ = terminate_signal.recv() => {
            debug!("🚩 Terminate signal received, initiating shutdown");
        }
    }
    if state.is_master() {
        shutdown::initiate_shutdown(&state).await;
    } else {
        state.initiate_shutdown();
    }
}

/// Main-process reload listener task for Master/Standalone mode.
///
/// Listens to the reload signal (SIGHUP by default) and triggers `reload_all()`.
///
/// Note on workers:
/// - The master's `WorkerConfigurationReload` hook coordinates worker reload
///   during `reload_all()` and tracks the outcome.
async fn listen_reload_for_main_process(
    state: AppState,
    mut hup_signal: Signal,
) -> claviron_runtime::registry::Result<()> {
    loop {
        tokio::select! {
            _ = state.on_shutdown() => {
                debug!("🔸 reload listener shutdown (master)");
                break;
            }
            res = hup_signal.recv() => {
                if res.is_none() {
                    warn!("⚠️ reload signal stream closed; exiting reload listener");
                    break;
                }
                warn!("🔄 Starting 'reload' (signal received)");
                let res = state.registry_ref().reload_all(&state).await;
                match &res {
                    Ok(outcome) if outcome.is_complete() => {
                        info!(
                            reloaded_providers = outcome.reloaded_count(),
                            "reload completed successfully"
                        );
                    }
                    Ok(outcome) => {
                        for failure in outcome.failures() {
                            error!(
                                component = failure.component(),
                                error = %failure.error(),
                                "provider reload failed"
                            );
                        }
                        warn!(
                            reloaded_providers = outcome.reloaded_count(),
                            failed_providers = outcome.failed_count(),
                            "reload completed with provider failures"
                        );
                    }
                    Err(e) => {
                        error!("reload failed: {e}");
                    }
                }
                state.events().emit(crate::ServerReloaded::new(state.reload_count() as u64));
            }
        }
    }
    Ok(())
}

/// The restart signal (SIGUSR2 by default) requests the same rolling restart
/// as the worker-management CLI.
/// The manager only submits a command; execution belongs to WorkerOperationRunner.
async fn listen_restart_for_master(
    state: AppState,
    mut requests: Signal,
) {
    loop {
        tokio::select! {
            biased;
            _ = state.on_shutdown() => break,
            received = requests.recv() => {
                if received.is_none() {
                    warn!("restart signal stream closed; exiting worker restart listener");
                    break;
                }
                match WorkerManager::new(&state).and_then(|manager| manager.restart_workers()) {
                    Ok(command) => info!(%command, "rolling worker restart requested (signal)"),
                    Err(error) => error!(%error, "rolling worker restart request failed (signal)")
                }
            }
        }
    }
    debug!("worker restart signal listener stopped (master)");
}

/// The reopen signal (SIGUSR1 by default) asks every log writer to reopen its
/// files. Emitted here, where the operator's signal lands; the fleet event
/// bridge carries it to the workers.
async fn listen_reopen_for_main_process(
    state: AppState,
    mut requests: Signal,
) {
    loop {
        tokio::select! {
            biased;
            _ = state.on_shutdown() => break,
            received = requests.recv() => {
                if received.is_none() {
                    warn!("reopen signal stream closed; exiting log reopen listener");
                    break;
                }
                info!("log files reopen requested (signal)");
                state.events().emit(ReopenLogs);
            }
        }
    }
}

/// A worker keeps every operator signal from ending it: `systemctl kill`
/// signals the whole unit, and a signal with no listener terminates. The
/// master acts on them; a worker's share arrives as a fleet event.
async fn ignore_operator_signals(
    state: AppState,
    mut signals: Vec<Signal>,
) {
    loop {
        let received = futures_util::future::select_all(
            signals.iter_mut().map(|signal| Box::pin(signal.recv())),
        );
        tokio::select! {
            biased;
            _ = state.on_shutdown() => break,
            (received, _, _) = received => {
                if received.is_none() {
                    break;
                }
                debug!("operator signal left to the master");
            }
        }
    }
}

fn install(
    chosen: OperatorSignal,
    role: &str,
) -> crate::Result<Signal> {
    signal(SignalKind::from_raw(chosen.raw())).map_err(|error| {
        Error::bootstrap(format!(
            "cannot install {role} listener on {chosen}: {error}"
        ))
    })
}

/// Join handles for process signal listeners.
///
/// - `reload` listens to SIGHUP in the main process; workers have no reload signal task.
/// - `restart` listens to SIGUSR1 only in master, not standalone or workers.
/// - `shutdown` listens to SIGINT/SIGTERM and initiates shutdown.
pub struct SignalHandles {
    reload: Option<JoinHandle<()>>,
    restart: Option<JoinHandle<()>>,
    reopen: Option<JoinHandle<()>>,
    shutdown: JoinHandle<()>,
}

impl SignalHandles {
    pub async fn wait_shutdown(self) {
        let _ = self.shutdown.await;
        for task in [self.restart, self.reopen].into_iter().flatten() {
            task.abort();
            let _ = task.await;
        }
    }

    pub fn abort_reload(&self) {
        if let Some(reload) = &self.reload {
            reload.abort();
        }
    }
}

/// Spawn main-process signal listeners and return their join handles.
///
/// Reload listener errors are logged and do not crash the process.
pub fn spawn_main_signal_handlers(
    state: AppState,
    signals: ServerSignals,
) -> crate::Result<SignalHandles> {
    signals.validate().map_err(Error::bootstrap)?;
    // Install synchronously, before returning or spawning other tasks: a user
    // signal's default disposition is termination. Fleet providers need not
    // be booted yet; resolve the manager only when a request arrives. A
    // standalone process has no workers to restart and keeps the restart
    // signal's default disposition.
    let restart_signal = if state.is_master() {
        Some(install(signals.restart, "worker restart")?)
    } else {
        None
    };
    let reopen_signal = install(signals.reopen_logs, "log reopen")?;
    let reload_signal = install(signals.reload, "reload")?;
    let restart = restart_signal.map(|requests| {
        tokio::spawn(listen_restart_for_master(state.clone(), requests))
    });
    let reopen = Some(tokio::spawn(listen_reopen_for_main_process(
        state.clone(),
        reopen_signal,
    )));
    let reload_state = state.clone();
    let reload = tokio::spawn(async move {
        if let Err(e) =
            listen_reload_for_main_process(reload_state, reload_signal).await
        {
            // Reload listener errors should not be treated as fatal service failures.
            warn!("reload signal handler stopped: {}", e);
        }
    });

    let shutdown_state = state;
    let shutdown = tokio::spawn(async move {
        listen_shutdown_for_main_process(shutdown_state).await;
    });

    Ok(SignalHandles { reload: Some(reload), restart, reopen, shutdown })
}

/// Worker shutdown listener task.
///
/// Listens to SIGINT (ignored) and SIGTERM (triggers graceful shutdown coordination with the master).
async fn listen_shutdown_for_worker(state: AppState) {
    let mut terminate_signal = signal(SignalKind::terminate())
        .expect("Failed to create terminate signal handler");
    let mut interrupt_signal = signal(SignalKind::interrupt())
        .expect("Failed to create interrupt signal handler");

    loop {
        tokio::select! {
            _ = state.on_shutdown() => {
                debug!("🔸 worker shutdown listener stopped (state shutdown)");
                break;
            }
            _ = interrupt_signal.recv() => {
                // Workers IGNORE SIGINT (Ctrl-C) - master coordinate shutdown.
                debug!("⚠️  received SIGINT - ignoring (master will coordinate shutdown)");
                continue;
            }
            _ = terminate_signal.recv() => {
                // SIGTERM from master = graceful shutdown.
                debug!("🚩 received SIGTERM from master, initiating shutdown");

                // This requests teardown; bootstrap publishes completion only
                // after runnable drain and provider finalization.
                state.initiate_shutdown();
                break;
            }
        }
    }
}

/// Spawn worker-process signal listeners and return their join handles.
///
/// - `shutdown` listens to SIGTERM and initiates worker shutdown.
///
/// Worker reload requests are handled by the supervisor's Orbit event
/// callback on the local dispatcher; workers do not own a SIGHUP listener.
pub fn spawn_worker_signal_handlers(
    state: AppState
) -> crate::Result<SignalHandles> {
    let RunMode::Worker(..) = state.run_mode() else {
        return Err(Error::service(
            "worker signal handlers require worker mode",
        ));
    };

    let operator = OperatorSignal::OPERATOR
        .into_iter()
        .map(|chosen| install(chosen, "operator signal"))
        .collect::<crate::Result<Vec<_>>>()?;
    let ignored =
        Some(tokio::spawn(ignore_operator_signals(state.clone(), operator)));

    let shutdown_state = state;
    let shutdown = tokio::spawn(async move {
        listen_shutdown_for_worker(shutdown_state).await;
    });

    Ok(SignalHandles { reload: None, restart: None, reopen: ignored, shutdown })
}

#[cfg(test)]
#[path = "../tests/signals.rs"]
mod tests;
