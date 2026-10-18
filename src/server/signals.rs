use claviron_runtime::AppState;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

pub(super) struct SignalHandles {
    reload: JoinHandle<()>,
    reopen: JoinHandle<()>,
    shutdown: JoinHandle<()>,
}

impl SignalHandles {
    pub(super) fn abort_reload(&self) {
        self.reload.abort();
    }

    pub(super) async fn wait_shutdown(self) {
        let _ = self.shutdown.await;
        self.reopen.abort();
        let _ = self.reopen.await;
    }
}

pub(super) fn spawn_main_signal_handlers(
    state: AppState,
    signals: claviron_sys::signal::ServerSignals,
) -> super::Result<SignalHandles> {
    signals.validate().map_err(super::Error::other)?;
    let mut terminate = signal(SignalKind::terminate()).map_err(|error| {
        super::Error::other(format!("cannot install SIGTERM listener: {error}"))
    })?;
    let mut hangup = signal(SignalKind::from_raw(signals.reload.raw()))
        .map_err(|error| {
            super::Error::other(format!(
                "cannot install reload listener on {}: {error}",
                signals.reload
            ))
        })?;
    let mut reopen_requests = signal(SignalKind::from_raw(
        signals.reopen_logs.raw(),
    ))
    .map_err(|error| {
        super::Error::other(format!(
            "cannot install log reopen listener on {}: {error}",
            signals.reopen_logs
        ))
    })?;
    let reopen_state = state.clone();
    let reopen = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = reopen_state.on_shutdown() => break,
                received = reopen_requests.recv() => {
                    if received.is_none() {
                        break;
                    }
                    info!("log files reopen requested (signal)");
                    reopen_state.events().emit(claviron_runtime::ReopenLogs);
                }
            }
        }
    });

    let reload_state = state.clone();
    let reload = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = reload_state.on_shutdown() => break,
                received = hangup.recv() => {
                    if received.is_none() {
                        warn!("reload signal stream closed; exiting reload listener");
                        break;
                    }
                    match reload_state.registry_ref().reload_all(&reload_state).await {
                        Ok(outcome) if outcome.is_complete() => {
                            info!(reloaded_providers = outcome.reloaded_count(), "reload completed successfully");
                        }
                        Ok(outcome) => {
                            for failure in outcome.failures() {
                                error!(component = failure.component(), error = %failure.error(), "provider reload failed");
                            }
                            warn!(
                                reloaded_providers = outcome.reloaded_count(),
                                failed_providers = outcome.failed_count(),
                                "reload completed with provider failures"
                            );
                        }
                        Err(error) => error!(%error, "reload failed")
                    }
                }
            }
        }
    });

    let shutdown = tokio::spawn(async move {
        tokio::select! {
            _ = state.on_shutdown() => {
                debug!("shutdown listener stopped (state shutdown)");
                return;
            }
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    error!(%error, "Ctrl-C listener failed");
                }
            }
            _ = terminate.recv() => {}
        }
        state.initiate_shutdown();
    });

    Ok(SignalHandles { reload, reopen, shutdown })
}
