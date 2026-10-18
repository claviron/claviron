use claviron_runtime::WorkerId;
use nix::sys::signal::Signal;
use tracing::{debug, error, warn};

use crate::control::{WorkerControl, WorkerProcess, WorkerProcessSnapshot};
use crate::lifecycle::{
    ExitOutcome, OwnedProcess, ProcessExit, WorkerIntent, WorkerPhase,
};
fn process_is_gone(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
        || error.raw_os_error() == Some(nix::libc::ESRCH)
}

/// Send `signal` to all currently alive workers.
/// Returns `(worker_id, pid)` for signaled processes.
pub(crate) async fn signal_alive_workers(
    control: &WorkerControl,
    signal: Signal,
) -> Vec<(WorkerId, u32)> {
    let workers = control.workers.read().await;
    let mut sent = Vec::new();
    for worker in workers.iter() {
        if let Some(pid) =
            crate::process::health::signal_worker_if_alive(worker, signal)
        {
            sent.push((worker.id, pid));
        }
    }
    sent
}

/// Internal signal helper: only sends signal when process is alive.
pub(crate) fn signal_worker_if_alive(
    worker: &WorkerProcess,
    signal: Signal,
) -> Option<u32> {
    let worker_id = worker.id;
    let process = worker.process.as_ref()?;
    crate::process::health::signal_if_alive(worker_id, process, signal)
}

/// Signal exactly this process, through its identity-validated handle.
/// `None` when it has already exited.
pub(crate) fn signal_if_alive(
    worker_id: WorkerId,
    process: &OwnedProcess,
    signal: Signal,
) -> Option<u32> {
    if process.has_exited() {
        return None;
    }
    let pid = process.pid();
    match process.signal(signal) {
        Ok(()) => Some(pid),
        Err(error) if process_is_gone(&error) => None,
        Err(error) => {
            warn!(
                "Failed to send {:?} to worker #{} (PID {}): {}",
                signal, worker_id, pid, error
            );
            None
        }
    }
}

/// Reap every process the kernel has reported gone and record the exit on
/// its row. The one place, besides a drain's own completion, where a
/// finished process becomes a finished slot.
pub(crate) async fn reap_exited(control: &WorkerControl) {
    let mut workers = control.workers.write().await;
    for worker in workers.iter_mut() {
        let already_recorded =
            matches!(worker.phase(), WorkerPhase::Exited { .. });
        let Some(process) = worker.process.as_mut() else {
            continue;
        };
        if !process.has_exited() || already_recorded {
            continue;
        }
        match process.try_reap() {
            Ok(Some(ProcessExit::Reaped(status))) if status.success() => {
                debug!("⚙️  Worker #{} exited successfully", worker.id);
            }
            Ok(Some(ProcessExit::Reaped(status))) => {
                warn!(
                    "⚙️  Worker #{} exited with status: {}",
                    worker.id, status
                )
            }
            Ok(Some(ProcessExit::Observed)) => {
                debug!("⚙️  Adopted worker #{} exit observed", worker.id)
            }
            Ok(None) => {
                debug!(
                    "⚙️  Worker #{} is gone but not yet reapable",
                    worker.id
                );
                continue;
            }
            Err(error) => {
                error!("failed to reap worker #{}: {}", worker.id, error)
            }
        }
        worker.record_exit();
    }
}

/// Check worker processes and classify unexpected exits.
///
/// Returns `(capacity_workers, restartable_dead_workers)`.
/// Draining workers may still have a live OS process, but they no
/// longer count as fleet capacity and are never restarted from this
/// health path.
pub(crate) async fn check_worker_health(
    control: &WorkerControl
) -> (Vec<WorkerProcessSnapshot>, Vec<WorkerId>) {
    let mut alive = Vec::new();
    let mut dead = Vec::new();
    let mut workers = control.workers.write().await;

    for worker in workers.iter_mut() {
        if let WorkerPhase::Exited { outcome } = worker.phase() {
            // A reaped process is not news. Whether it needs replacing
            // was decided when the exit was recorded, from the intent that
            // stood then; a slot nobody wants running rests.
            if outcome == ExitOutcome::Failed
                && worker.intent() == WorkerIntent::EnsureRunning
            {
                dead.push(worker.id);
            }
            continue;
        }

        // Reaping is how a finished child is proven finished; it also
        // detects zombies and gracefully exited processes, and works on
        // every platform. A slot this master was not allowed to fill holds
        // no process and is not a death: reading it as one turns an
        // unclaimed slot into a crash — which aborts boot before the
        // monitor loop that was supposed to retry it ever runs, and on the
        // monitor path spends the recovery budget an unclaimed slot is
        // documented never to touch.
        let Some(process) = worker.process.as_mut() else {
            if worker.phase().is_unowned() {
                continue;
            }
            worker.mark_failed();
            dead.push(worker.id);
            continue;
        };
        let pid = process.pid();
        match process.try_reap() {
            Ok(Some(status)) => {
                let intent = worker.intent();
                match worker.record_exit() {
                    ExitOutcome::Planned => {
                        debug!(
                            "Worker #{} (PID {}) drained ({:?}) with status: {}",
                            worker.id, pid, intent, status
                        );
                    }
                    ExitOutcome::Failed => {
                        warn!(
                            "Worker #{} (PID {}) exited unexpectedly with status: {}",
                            worker.id, pid, status
                        );
                        dead.push(worker.id);
                    }
                }
            }
            Ok(None) => {
                // Process is still running
                if worker.is_capacity() {
                    alive.push(WorkerProcessSnapshot {
                        id: worker.id,
                        pid,
                        lifecycle: worker.lifecycle(),
                    });
                }
            }
            Err(e) => {
                let intent = worker.intent();
                match worker.record_exit() {
                    ExitOutcome::Planned => {
                        debug!(
                            "Worker #{} health check error while draining ({:?}): {}; treating as exited",
                            worker.id, intent, e
                        );
                    }
                    ExitOutcome::Failed => {
                        // Error checking status - treat as dead
                        warn!(
                            "Worker #{} health check error: {}, treating as dead",
                            worker.id, e
                        );
                        dead.push(worker.id);
                    }
                }
            }
        }
    }

    (alive, dead)
}
